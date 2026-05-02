use std::{
    path::{Path, PathBuf},
    process::Stdio,
    sync::Arc,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    net::TcpListener,
    process::{Child, Command},
    sync::Mutex,
    task::JoinHandle,
    time::{sleep, Duration},
};
use tracing::info;
use uuid::Uuid;

#[cfg(unix)]
#[allow(unused_imports)] // Needed for pre_exec method
use std::os::unix::process::CommandExt;

const BIND_HOST: &str = "0.0.0.0";
const LOCAL_HEALTH_HOST: &str = "127.0.0.1";
const INDEXER_STARTUP_TIMEOUT: Duration = Duration::from_secs(60);
const INDEXER_STARTUP_POLL_INTERVAL: Duration = Duration::from_millis(250);
const PIPE_LOG_LIMIT_BYTES: usize = 32 * 1024;

pub struct TokenIndexerInstance {
    // Handle to the spawned pnpm dev process
    pub child: Child,
    pub api_server_url: String,
    stdout: Arc<Mutex<String>>,
    stderr: Arc<Mutex<String>>,
    stdout_task: Option<JoinHandle<()>>,
    stderr_task: Option<JoinHandle<()>>,
}

impl TokenIndexerInstance {
    pub async fn new(
        interactive: bool,
        rpc_url: &str,
        ws_url: &str,
        pipe_output: bool,
        chain_id: u64,
        database_url: String,
        interactive_port: Option<u16>,
    ) -> std::io::Result<Self> {
        let manifest_dir = std::env::var("CARGO_MANIFEST_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("."));
        let token_indexer_dir = resolve_token_indexer_dir(&manifest_dir);

        let ponder_port = if interactive {
            interactive_port.unwrap_or(50104_u16)
        } else {
            let listener = TcpListener::bind((LOCAL_HEALTH_HOST, 0))
                .await
                .expect("Should be able to bind to port");

            listener
                .local_addr()
                .expect("Should have a local address")
                .port()
        };

        // uuid for the schema
        let schema_uuid = Uuid::now_v7();
        let mut cmd = Command::new("pnpm");
        cmd.args([
            "dev",
            "--disable-ui",
            "--hostname",
            BIND_HOST,
            "--port",
            ponder_port.to_string().as_str(),
            "--schema",
            schema_uuid.to_string().as_str(),
        ])
        .kill_on_drop(true)
        .current_dir(token_indexer_dir)
        .env("DATABASE_URL", database_url)
        .env("PONDER_CHAIN_ID", chain_id.to_string())
        .env("PONDER_RPC_URL_HTTP", rpc_url)
        .env("PONDER_WS_URL_HTTP", ws_url)
        .env("PONDER_DISABLE_CACHE", "true")
        .env("PONDER_CONTRACT_START_BLOCK", "0")
        .env("DATABASE_SCHEMA", schema_uuid.to_string())
        .env("PONDER_SCHEMA", schema_uuid.to_string())
        .env("PONDER_LOG_LEVEL", "trace");

        // Set the child process to be the leader of its own process group
        // This prevents killing the parent test process when we clean up
        #[cfg(unix)]
        unsafe {
            cmd.pre_exec(|| {
                // setpgid(0, 0) makes this process the leader of a new process group
                libc::setpgid(0, 0);
                Ok(())
            });
        }

        if pipe_output {
            cmd.stdout(Stdio::inherit()).stderr(Stdio::inherit());
        } else {
            cmd.stdout(Stdio::piped()).stderr(Stdio::piped());
        }

        let mut child = cmd.spawn().expect("Failed to spawn token indexer process");
        let stdout = Arc::new(Mutex::new(String::new()));
        let stderr = Arc::new(Mutex::new(String::new()));
        let stdout_task = if pipe_output {
            None
        } else {
            child
                .stdout
                .take()
                .map(|pipe| spawn_pipe_drain(pipe, stdout.clone()))
        };
        let stderr_task = if pipe_output {
            None
        } else {
            child
                .stderr
                .take()
                .map(|pipe| spawn_pipe_drain(pipe, stderr.clone()))
        };

        let api_server_url = format!("http://{BIND_HOST}:{ponder_port}");
        info!("Indexer API server URL: {api_server_url}");

        let mut instance = Self {
            child,
            api_server_url,
            stdout,
            stderr,
            stdout_task,
            stderr_task,
        };
        instance.wait_until_ready().await?;

        Ok(instance)
    }

    /// Check if the process is still running
    pub fn is_running(&mut self) -> bool {
        matches!(self.child.try_wait(), Ok(None))
    }

    /// Kill the process
    pub async fn kill(&mut self) -> std::io::Result<()> {
        self.child.kill().await
    }

    /// Wait for the process to finish
    pub async fn wait(&mut self) -> std::io::Result<std::process::ExitStatus> {
        self.child.wait().await
    }

    async fn wait_until_ready(&mut self) -> std::io::Result<()> {
        let client = reqwest::Client::new();
        let health_url = self.api_server_url.replace(BIND_HOST, LOCAL_HEALTH_HOST) + "/health";
        let start = std::time::Instant::now();

        loop {
            if let Some(status) = self.child.try_wait()? {
                let (stdout, stderr) = self.output_snapshot().await;
                return Err(std::io::Error::other(format!(
                    "token indexer exited before becoming ready: {status}\nstdout:\n{stdout}\nstderr:\n{stderr}"
                )));
            }

            if client.get(&health_url).send().await.is_ok() {
                return Ok(());
            }

            if start.elapsed() >= INDEXER_STARTUP_TIMEOUT {
                let (stdout, stderr) = self.output_snapshot().await;
                return Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    format!(
                        "token indexer did not become ready within {:?} at {}\nstdout:\n{}\nstderr:\n{}",
                        INDEXER_STARTUP_TIMEOUT, health_url, stdout, stderr
                    ),
                ));
            }

            sleep(INDEXER_STARTUP_POLL_INTERVAL).await;
        }
    }

    async fn output_snapshot(&self) -> (String, String) {
        (
            self.stdout.lock().await.clone(),
            self.stderr.lock().await.clone(),
        )
    }
}

fn spawn_pipe_drain<R>(mut pipe: R, buffer: Arc<Mutex<String>>) -> JoinHandle<()>
where
    R: AsyncRead + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        let mut chunk = [0_u8; 4096];
        loop {
            let read = match pipe.read(&mut chunk).await {
                Ok(0) => return,
                Ok(read) => read,
                Err(error) => {
                    append_pipe_output(&buffer, &format!("\n<failed to read pipe: {error}>\n"))
                        .await;
                    return;
                }
            };
            let text = String::from_utf8_lossy(&chunk[..read]);
            append_pipe_output(&buffer, &text).await;
        }
    })
}

async fn append_pipe_output(buffer: &Arc<Mutex<String>>, text: &str) {
    let mut output = buffer.lock().await;
    output.push_str(text);
    if output.len() > PIPE_LOG_LIMIT_BYTES {
        let excess = output.len() - PIPE_LOG_LIMIT_BYTES;
        output.drain(..excess);
    }
}

impl Drop for TokenIndexerInstance {
    fn drop(&mut self) {
        if let Some(pid) = self.child.id() {
            self.kill_process_tree(pid);
        }
        if let Some(task) = self.stdout_task.take() {
            task.abort();
        }
        if let Some(task) = self.stderr_task.take() {
            task.abort();
        }
    }
}

impl TokenIndexerInstance {
    fn kill_process_tree(&self, pid: u32) {
        let pgid = unsafe { libc::getpgid(pid as i32) };

        if pgid >= 0 {
            // Send SIGTERM to entire process group
            // This is safe because the child process was spawned with its own process group
            // via setpgid(0, 0), so this will only kill the child and its descendants,
            // not the parent test process
            unsafe {
                libc::kill(-pgid, libc::SIGTERM);
            }
        } else {
            // Fallback: just kill the parent process if getpgid failed
            let _ = std::process::Command::new("kill")
                .arg("-TERM")
                .arg(pid.to_string())
                .output();
        }
    }
}

fn resolve_token_indexer_dir(manifest_dir: &Path) -> PathBuf {
    let mut package_dir_without_node_modules = None;

    for ancestor in manifest_dir.ancestors() {
        let candidate = ancestor.join("evm-token-indexer");
        if !candidate.join("package.json").is_file() {
            continue;
        }

        if candidate.join("node_modules").is_dir() {
            return candidate;
        }

        if package_dir_without_node_modules.is_none() {
            package_dir_without_node_modules = Some(candidate);
        }
    }

    package_dir_without_node_modules.unwrap_or_else(|| PathBuf::from("evm-token-indexer"))
}
