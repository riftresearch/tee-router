import type { GatewayConfig, HealthTargetConfig } from './config'
import type { FetchLike } from './internal/router-client'

export type DependencyHealthStatus = 'reachable' | 'unreachable' | 'unknown'

export type DependencyHealthState = {
  name: string
  status: DependencyHealthStatus
  checkedAt?: string
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
    const dependencies = [...this.states.values()].sort((left, right) =>
      left.name.localeCompare(right.name)
    )

    return {
      status: dependencies.some(
        (dependency) => dependency.status === 'unreachable'
      )
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

    try {
      const response = await this.fetcher(target.url, {
        method: target.method,
        headers: {
          accept: 'application/json'
        },
        signal: controller.signal
      })
      const responseBody = await response.text().catch(() => '')
      if (
        response.ok &&
        target.response === 'routerProviderHealth' &&
        this.applyRouterProviderHealth(target, responseBody)
      ) {
        return
      }

      this.states.set(target.name, {
        name: target.name,
        status: response.ok ? 'reachable' : 'unreachable',
        checkedAt: new Date().toISOString()
      })
    } catch (error) {
      void error
      this.states.set(target.name, {
        name: target.name,
        status: 'unreachable',
        checkedAt: new Date().toISOString()
      })
    } finally {
      clearTimeout(timeout)
    }
  }

  private applyRouterProviderHealth(
    target: HealthTargetConfig,
    body: string
  ): boolean {
    let parsed: unknown
    try {
      parsed = JSON.parse(body)
    } catch {
      return false
    }
    if (!isRecord(parsed) || !Array.isArray(parsed.providers)) return false

    this.states.delete(target.name)
    for (const provider of parsed.providers) {
      if (!isRecord(provider) || typeof provider.provider !== 'string') continue
      const status =
        provider.status === 'down'
          ? 'unreachable'
          : provider.status === 'unknown'
            ? 'unknown'
            : 'reachable'
      this.states.set(provider.provider, {
        name: provider.provider,
        status,
        ...(typeof provider.checked_at === 'string'
          ? { checkedAt: provider.checked_at }
          : {})
      })
    }

    if (parsed.providers.length === 0) {
      this.states.set(target.name, {
        name: target.name,
        status: 'unknown'
      })
    }
    return true
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

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null
}
