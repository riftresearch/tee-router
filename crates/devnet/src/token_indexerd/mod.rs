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
const INDEXER_HEALTH_REQUEST_TIMEOUT: Duration = Duration::from_secs(2);
const PIPE_LOG_LIMIT_BYTES: usize = 32 * 1024;
const TOKEN_INDEXER_API_KEY_MIN_LENGTH: usize = 32;

pub struct TokenIndexerInstance {
    // Handle to the spawned pnpm dev process
    pub child: Child,
    pub api_server_url: String,
    pub api_key: String,
    stdout: Arc<Mutex<String>>,
    stderr: Arc<Mutex<String>>,
    stdout_task: Option<JoinHandle<()>>,
    stderr_task: Option<JoinHandle<()>>,
}

pub struct TokenIndexerConfig<'a> {
    pub interactive: bool,
    pub rpc_url: &'a str,
    pub ws_url: &'a str,
    pub pipe_output: bool,
    pub chain_id: u64,
    pub database_url: String,
    pub api_key: String,
    pub interactive_port: Option<u16>,
}

impl TokenIndexerInstance {
    pub async fn new(config: TokenIndexerConfig<'_>) -> std::io::Result<Self> {
        let TokenIndexerConfig {
            interactive,
            rpc_url,
            ws_url,
            pipe_output,
            chain_id,
            database_url,
            api_key,
            interactive_port,
        } = config;
        let manifest_dir = std::env::var("CARGO_MANIFEST_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("."));
        let token_indexer_dir = resolve_token_indexer_dir(&manifest_dir);

        let ponder_port = if interactive {
            interactive_port.unwrap_or(50104_u16)
        } else {
            let listener = TcpListener::bind((LOCAL_HEALTH_HOST, 0)).await?;

            listener.local_addr()?.port()
        };
        validate_token_indexer_api_key(&api_key)?;

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
        .env("EVM_TOKEN_INDEXER_API_KEY", &api_key)
        .env("PONDER_LOG_LEVEL", "trace");

        // Set the child process to be the leader of its own process group
        // This prevents killing the parent test process when we clean up
        #[cfg(unix)]
        // SAFETY: `pre_exec` runs in the child process immediately before
        // exec. The closure only calls async-signal-safe `setpgid` and
        // constructs an OS error if that syscall fails.
        unsafe {
            cmd.pre_exec(|| {
                // setpgid(0, 0) makes this process the leader of a new process group
                if libc::setpgid(0, 0) == -1 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }

        if pipe_output {
            cmd.stdout(Stdio::inherit()).stderr(Stdio::inherit());
        } else {
            cmd.stdout(Stdio::piped()).stderr(Stdio::piped());
        }

        let mut child = cmd.spawn()?;
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
            api_key,
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
        let client = reqwest::Client::builder()
            .timeout(INDEXER_HEALTH_REQUEST_TIMEOUT)
            .build()
            .map_err(std::io::Error::other)?;
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
        let Ok(pid) = libc::pid_t::try_from(pid) else {
            return;
        };
        // SAFETY: `getpgid` reads process metadata for a pid owned by this
        // instance. It does not dereference Rust memory or alias data.
        let pgid = unsafe { libc::getpgid(pid) };

        if pgid >= 0 {
            // Send SIGTERM to entire process group
            // This is safe because the child process was spawned with its own process group
            // via setpgid(0, 0), so this will only kill the child and its descendants,
            // not the parent test process
            // SAFETY: `pgid` came from `getpgid` above. Negating it targets the
            // child process group intentionally created by `setpgid(0, 0)`.
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

fn validate_token_indexer_api_key(api_key: &str) -> std::io::Result<()> {
    if api_key.trim().len() < TOKEN_INDEXER_API_KEY_MIN_LENGTH {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!(
                "token indexer API key must be at least {TOKEN_INDEXER_API_KEY_MIN_LENGTH} characters"
            ),
        ));
    }
    Ok(())
}
