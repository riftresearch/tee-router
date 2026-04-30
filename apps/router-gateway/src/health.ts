import type { GatewayConfig, HealthTargetConfig } from './config'
import type { FetchLike } from './internal/router-client'

export type DependencyHealthStatus = 'ok' | 'down' | 'unknown'

export type DependencyHealthState = {
  name: string
  status: DependencyHealthStatus
  checkedAt?: string
  latencyMs?: number
  httpStatus?: number
  error?: string
}

export type DependencyHealthSnapshot = {
  status: 'ok' | 'degraded'
  timestamp: string
  dependencies: DependencyHealthState[]
}

type DependencyHealthMonitorOptions = {
  targets: HealthTargetConfig[]
  pollIntervalMs: number
  fetch?: FetchLike
}

export class DependencyHealthMonitor {
  private readonly targets: HealthTargetConfig[]
  private readonly pollIntervalMs: number
  private readonly fetcher: FetchLike
  private readonly states = new Map<string, DependencyHealthState>()
  private timer: ReturnType<typeof setInterval> | undefined
  private inFlight = false

  constructor(options: DependencyHealthMonitorOptions) {
    this.targets = options.targets
    this.pollIntervalMs = options.pollIntervalMs
    this.fetcher = options.fetch ?? fetch

    for (const target of this.targets) {
      this.states.set(target.name, {
        name: target.name,
        status: 'unknown'
      })
    }
  }

  start(): void {
    if (this.timer || this.targets.length === 0) return

    void this.refresh()
    this.timer = setInterval(() => {
      void this.refresh()
    }, this.pollIntervalMs)
    this.timer.unref?.()
  }

  stop(): void {
    if (!this.timer) return
    clearInterval(this.timer)
    this.timer = undefined
  }

  snapshot(now = new Date()): DependencyHealthSnapshot {
    const dependencies = this.targets.map((target) => {
      return (
        this.states.get(target.name) ?? {
          name: target.name,
          status: 'unknown' as const
        }
      )
    })

    return {
      status: dependencies.some((dependency) => dependency.status === 'down')
        ? 'degraded'
        : 'ok',
      timestamp: now.toISOString(),
      dependencies
    }
  }

  async refresh(): Promise<void> {
    if (this.inFlight) return

    this.inFlight = true
    try {
      await Promise.all(this.targets.map((target) => this.check(target)))
    } finally {
      this.inFlight = false
    }
  }

  private async check(target: HealthTargetConfig): Promise<void> {
    const controller = new AbortController()
    const timeout = setTimeout(() => controller.abort(), target.timeoutMs)
    const startedAt = Date.now()

    try {
      const response = await this.fetcher(target.url, {
        method: target.method,
        headers: {
          accept: 'application/json'
        },
        signal: controller.signal
      })
      const latencyMs = Date.now() - startedAt
      await response.arrayBuffer().catch(() => undefined)

      this.states.set(target.name, {
        name: target.name,
        status: response.ok ? 'ok' : 'down',
        checkedAt: new Date().toISOString(),
        latencyMs,
        httpStatus: response.status,
        ...(response.ok ? {} : { error: `HTTP ${response.status}` })
      })
    } catch (error) {
      this.states.set(target.name, {
        name: target.name,
        status: 'down',
        checkedAt: new Date().toISOString(),
        latencyMs: Date.now() - startedAt,
        error: error instanceof Error ? error.message : 'dependency check failed'
      })
    } finally {
      clearTimeout(timeout)
    }
  }
}

export function createDependencyHealthMonitor(
  config: GatewayConfig,
  fetcher?: FetchLike
): DependencyHealthMonitor {
  return new DependencyHealthMonitor({
    targets: config.healthTargets,
    pollIntervalMs: config.healthPollIntervalMs,
    fetch: fetcher
  })
}
