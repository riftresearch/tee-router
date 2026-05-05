import type { GatewayConfig, HealthTargetConfig } from './config'
import { readLimitedResponseText } from './internal/http-body'
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

const MAX_HEALTH_RESPONSE_BODY_BYTES = 256 * 1024

export class DependencyHealthMonitor {
  private readonly targets: HealthTargetConfig[]
  private readonly pollIntervalMs: number
  private readonly fetcher: FetchLike
  private readonly states = new Map<string, DependencyHealthState>()
  private readonly expandedProviderNamesByTarget = new Map<string, Set<string>>()
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
          accept: 'application/json',
          ...(target.headers ?? {})
        },
        signal: controller.signal
      })
      const responseBody = await readLimitedResponseText(
        response,
        MAX_HEALTH_RESPONSE_BODY_BYTES
      )
      if (
        response.ok &&
        !responseBody.truncated &&
        target.response === 'routerProviderHealth' &&
        this.applyRouterProviderHealth(target, responseBody.text)
      ) {
        return
      }

      this.setTargetState(target, {
        name: target.name,
        status: response.ok ? 'reachable' : 'unreachable',
        checkedAt: new Date().toISOString()
      })
    } catch (error) {
      void error
      this.setTargetState(target, {
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

    this.clearExpandedProviderStates(target)
    this.states.delete(target.name)
    const providerNames = new Set<string>()
    for (const provider of parsed.providers) {
      if (!isRecord(provider) || typeof provider.provider !== 'string') continue
      const status = routerProviderStatusToDependencyStatus(provider.status)
      providerNames.add(provider.provider)
      this.states.set(provider.provider, {
        name: provider.provider,
        status,
        ...(typeof provider.checked_at === 'string'
          ? { checkedAt: provider.checked_at }
          : {})
      })
    }

    if (providerNames.size > 0) {
      this.expandedProviderNamesByTarget.set(target.name, providerNames)
    } else {
      this.expandedProviderNamesByTarget.delete(target.name)
      this.states.set(target.name, {
        name: target.name,
        status: 'unknown'
      })
    }
    return true
  }

  private setTargetState(
    target: HealthTargetConfig,
    state: DependencyHealthState
  ) {
    this.clearExpandedProviderStates(target)
    this.states.set(target.name, state)
  }

  private clearExpandedProviderStates(target: HealthTargetConfig) {
    const providerNames = this.expandedProviderNamesByTarget.get(target.name)
    if (!providerNames) return
    for (const providerName of providerNames) {
      this.states.delete(providerName)
    }
    this.expandedProviderNamesByTarget.delete(target.name)
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

function routerProviderStatusToDependencyStatus(
  status: unknown
): DependencyHealthStatus {
  if (status === 'ok') return 'reachable'
  if (status === 'down') return 'unreachable'
  return 'unknown'
}
