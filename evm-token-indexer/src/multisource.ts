export type SourceEmit<E> = (event: E) => Promise<void>;

export type Source<E> = {
  name: string;
  start: (emit: SourceEmit<E>) => void | (() => void) | Promise<void | (() => void)>;
};

type MultiSourceOptions<E, K> = {
  sources: Source<E>[];
  dedupKey: (event: E) => K;
  maxSeen?: number;
  seenTtlMs?: number;
};

type EventHandler<E> = (event: E, sourceName: string) => void | Promise<void>;

export class MultiSource<E, K> {
  private readonly sources: Source<E>[];
  private readonly dedupKey: (event: E) => K;
  private readonly maxSeen: number;
  private readonly seenTtlMs: number;
  private readonly recentSeen = new Map<K, number>();
  private readonly handlers: EventHandler<E>[] = [];
  private readonly stopFns: (() => void)[] = [];
  private started = false;

  constructor(options: MultiSourceOptions<E, K>) {
    this.sources = options.sources;
    this.dedupKey = options.dedupKey;
    this.maxSeen = options.maxSeen ?? 100_000;
    this.seenTtlMs = options.seenTtlMs ?? 30 * 60 * 1000;
  }

  onEvent(handler: EventHandler<E>) {
    this.handlers.push(handler);
  }

  async start() {
    if (this.started) return;
    this.started = true;

    await Promise.all(
      this.sources.map(async (source) => {
        const stop = await source.start(async (event) => {
          await this.emitFrom(source.name, event);
        });
        if (typeof stop === "function") {
          this.stopFns.push(stop);
        }
      }),
    );
  }

  stop() {
    for (const stop of this.stopFns.splice(0)) {
      stop();
    }
    this.started = false;
  }

  async emitFrom(sourceName: string, event: E) {
    if (this.markSeen(event)) return false;
    for (const handler of this.handlers) {
      await handler(event, sourceName);
    }
    return true;
  }

  private markSeen(event: E) {
    const now = Date.now();
    this.prune(now);
    const key = this.dedupKey(event);
    if (this.recentSeen.has(key)) {
      this.recentSeen.delete(key);
      this.recentSeen.set(key, now);
      return true;
    }
    this.recentSeen.set(key, now);
    while (this.recentSeen.size > this.maxSeen) {
      const oldest = this.recentSeen.keys().next();
      if (oldest.done) break;
      this.recentSeen.delete(oldest.value);
    }
    return false;
  }

  private prune(now: number) {
    const cutoff = now - this.seenTtlMs;
    for (const [key, seenAt] of this.recentSeen) {
      if (seenAt > cutoff) break;
      this.recentSeen.delete(key);
    }
  }
}
