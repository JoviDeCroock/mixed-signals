import {type Signal, Signal as SignalCtor, signal} from '@preact/signals-core';
import {
  UNWATCH_SIGNALS_METHOD,
  WATCH_SIGNALS_METHOD,
} from '../shared/protocol.ts';
import {
  REFRESH_REFLECTED_MODEL,
  type RefreshableReflectedModel,
} from './model.ts';
import type {RPCClient} from './rpc.ts';

/** @internal */
export interface WireContext {
  rpc: RPCClient;
}

type SignalId = number | string;

function uniqueSignalIds(ids: Array<SignalId | undefined>): SignalId[] {
  const unique = new Set<SignalId>();
  for (const id of ids) {
    if (id !== undefined) unique.add(id);
  }
  return Array.from(unique);
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  if (!value || typeof value !== 'object') return false;
  const prototype = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
}

export class ClientReflection {
  private signals = new Map<SignalId, Signal<any>>();
  private signalIds = new WeakMap<Signal<any>, SignalId>();
  private activeSignals = new Set<Signal<any>>();
  private models = new Map<string, any>();
  private modelRegistry = new Map<string, any>();
  private rpc: RPCClient;
  private ctx: WireContext;
  private watchBatch = new Set<Signal<any>>();
  private unwatchBatch = new Set<Signal<any>>();
  private watchFlushTimer: ReturnType<typeof setTimeout> | null = null;
  private unwatchFlushTimer: ReturnType<typeof setTimeout> | null = null;

  constructor(rpc: RPCClient, ctx?: any) {
    this.rpc = rpc;
    this.ctx = ctx && ctx.rpc === rpc ? ctx : {rpc};
  }

  /** Clear all cached client state. Prefer prepareReconnect() for reconnects. */
  reset() {
    this.cancelBatchTimers();
    this.signals.clear();
    this.signalIds = new WeakMap();
    this.activeSignals.clear();
    this.models.clear();
    this.watchBatch.clear();
    this.unwatchBatch.clear();
  }

  /**
   * Keep cached signals/models alive, but discard pending watch traffic tied to
   * the old transport generation. The next root snapshot will refresh/rebind
   * them and replayActiveSignals() will subscribe the new connection.
   */
  prepareReconnect() {
    this.cancelBatchTimers();
    this.watchBatch.clear();
    this.unwatchBatch.clear();
  }

  /**
   * Drop raw wire-id mappings when a different server process owns the next
   * snapshot. Existing signal objects stay alive and can be rebound as the new
   * root/model snapshots identify their replacement wire ids.
   */
  prepareProcessChange() {
    this.prepareReconnect();
    this.signals.clear();
    this.signalIds = new WeakMap();
  }

  /** Replay all currently watched signal ids on a freshly connected transport. */
  replayActiveSignals(exclude?: Iterable<SignalId>): SignalId[] {
    this.prepareReconnect();
    const excluded = exclude ? new Set(exclude) : undefined;
    const ids = uniqueSignalIds(
      Array.from(this.activeSignals, (sig) => this.signalIds.get(sig)),
    ).filter((id) => !excluded?.has(id));
    if (ids.length > 0) {
      this.rpc.notify(WATCH_SIGNALS_METHOD, ids);
    }
    return ids;
  }

  registerModel(typeName: string, ctor: any) {
    this.modelRegistry.set(typeName, ctor);
  }

  getModelMarkers(): string[] {
    return Array.from(this.models.keys());
  }

  private cancelBatchTimers() {
    if (this.watchFlushTimer) {
      clearTimeout(this.watchFlushTimer);
      this.watchFlushTimer = null;
    }
    if (this.unwatchFlushTimer) {
      clearTimeout(this.unwatchFlushTimer);
      this.unwatchFlushTimer = null;
    }
  }

  private rememberSignal(id: SignalId, sig: Signal<any>) {
    const previousId = this.signalIds.get(sig);
    if (previousId !== undefined && previousId !== id) {
      if (this.signals.get(previousId) === sig) {
        this.signals.delete(previousId);
      }
    }

    this.signals.set(id, sig);
    this.signalIds.set(sig, id);
  }

  private scheduleWatch(sig: Signal<any>) {
    // Batch watch messages so a render burst becomes one frame.
    this.watchBatch.add(sig);
    if (!this.watchFlushTimer) {
      this.watchFlushTimer = setTimeout(() => {
        const signals = Array.from(this.watchBatch);
        this.watchBatch.clear();
        this.watchFlushTimer = null;
        const ids = uniqueSignalIds(
          signals
            .filter((signal) => this.activeSignals.has(signal))
            .map((signal) => this.signalIds.get(signal)),
        );
        if (ids.length > 0) {
          this.rpc.notify(WATCH_SIGNALS_METHOD, ids);
        }
      }, 1);
    }
  }

  private scheduleUnwatch(sig: Signal<any>) {
    // Unwatchs are batched separately so quick remounts can cancel them.
    this.unwatchBatch.add(sig);
    if (!this.unwatchFlushTimer) {
      this.unwatchFlushTimer = setTimeout(() => {
        const signals = Array.from(this.unwatchBatch);
        this.unwatchBatch.clear();
        this.unwatchFlushTimer = null;
        const ids = uniqueSignalIds(
          signals
            .filter((signal) => !this.activeSignals.has(signal))
            .map((signal) => this.signalIds.get(signal)),
        );
        if (ids.length > 0) {
          this.rpc.notify(UNWATCH_SIGNALS_METHOD, ids);
        }
      }, 1);
    }
  }

  getOrCreateSignal(id: SignalId, initialValue: any): Signal<any> {
    const existingSignal = this.signals.get(id);
    if (existingSignal) return existingSignal;

    let unwatchTimeout: ReturnType<typeof setTimeout> | null = null;
    let createdSignal!: Signal<any>;

    createdSignal = signal(initialValue, {
      watched: () => {
        this.activeSignals.add(createdSignal);
        if (unwatchTimeout) {
          clearTimeout(unwatchTimeout);
          unwatchTimeout = null;
        } else {
          // Only tell the server once the client actually observes this signal.
          this.scheduleWatch(createdSignal);
        }
      },
      unwatched: () => {
        // Debounce unwatch so transient unmount/remount cycles stay subscribed.
        unwatchTimeout = setTimeout(() => {
          this.activeSignals.delete(createdSignal);
          this.scheduleUnwatch(createdSignal);
          unwatchTimeout = null;
        }, 10);
      },
    });

    this.rememberSignal(id, createdSignal);
    return createdSignal;
  }

  syncSignalSnapshot(id: SignalId, value: any): Signal<any> {
    const sig = this.getOrCreateSignal(id, value);
    sig.value = value;
    return sig;
  }

  reconcileRoot(previousRoot: any, nextRoot: any): any {
    // Preserve only identities the protocol can prove. The root shell is kept
    // stable for ergonomics, but unbranded nested objects/arrays are replaced.
    if (isPlainObject(previousRoot) && isPlainObject(nextRoot)) {
      for (const key of Object.keys(previousRoot)) {
        if (!(key in nextRoot)) {
          delete previousRoot[key];
        }
      }

      for (const [key, value] of Object.entries(nextRoot)) {
        previousRoot[key] = this.reconcileIdentifiedValue(
          previousRoot[key],
          value,
        );
      }

      return previousRoot;
    }

    return this.reconcileIdentifiedValue(previousRoot, nextRoot);
  }

  /** @internal */
  syncSignalIdentity(
    previousSignal: Signal<any>,
    nextSignal: Signal<any>,
  ): Signal<any> {
    return this.rebindSignal(previousSignal, nextSignal, true);
  }

  private reconcileIdentifiedValue(previousValue: any, nextValue: any): any {
    if (previousValue === nextValue) return previousValue;

    if (
      previousValue instanceof SignalCtor &&
      nextValue instanceof SignalCtor
    ) {
      return this.rebindSignal(previousValue, nextValue);
    }

    return nextValue;
  }

  private rebindSignal(
    previousSignal: Signal<any>,
    nextSignal: Signal<any>,
    preferExisting = false,
  ): Signal<any> {
    const nextId = this.signalIds.get(nextSignal);
    if (nextId !== undefined) {
      const existingSignal = this.signals.get(nextId);
      if (
        preferExisting &&
        existingSignal &&
        existingSignal !== previousSignal
      ) {
        this.transferActiveSignal(previousSignal, existingSignal);
        this.transferActiveSignal(nextSignal, existingSignal);
        previousSignal.value = existingSignal.peek();
        return existingSignal;
      }

      this.rememberSignal(nextId, previousSignal);
    }

    this.transferActiveSignal(nextSignal, previousSignal);

    previousSignal.value = nextSignal.peek();
    return previousSignal;
  }

  private transferActiveSignal(from: Signal<any>, to: Signal<any>) {
    if (from !== to && this.activeSignals.has(from)) {
      this.activeSignals.delete(from);
      this.activeSignals.add(to);
    }
  }

  createModelFacade(serialized: any): any {
    const raw: string = serialized['@M'];
    if (!raw) {
      throw new Error('Model missing @M field');
    }

    // Models are branded as TypeName#wireId so the facade knows both pieces.
    const hashIdx = raw.lastIndexOf('#');
    const typeName = hashIdx !== -1 ? raw.slice(0, hashIdx) : raw;
    const wireId = hashIdx !== -1 ? raw.slice(hashIdx + 1) : undefined;
    const data = {...serialized, '@wireId': wireId};

    const existing = this.models.get(raw) as
      | RefreshableReflectedModel
      | undefined;
    if (existing) {
      existing[REFRESH_REFLECTED_MODEL]?.(data);
      return existing;
    }

    const ModelCtor = this.modelRegistry.get(typeName);
    if (!ModelCtor) {
      throw new Error(`Unknown model type: ${typeName}`);
    }

    const model = new ModelCtor(this.ctx, data);
    this.models.set(raw, model);
    return model;
  }

  handleUpdate(id: SignalId, value: any, mode?: string) {
    const sig = this.signals.get(id);
    if (!sig) return;

    if (!mode) {
      sig.value = value;
      return;
    }

    const current = sig.value;

    switch (mode) {
      case 'append':
        // Streaming text and immutable array pushes both land here.
        if (Array.isArray(current)) {
          sig.value = [...current, ...value];
        } else if (typeof current === 'string') {
          sig.value = current + value;
        }
        break;

      case 'merge':
        if (current && typeof current === 'object') {
          sig.value = {...current, ...value};
        }
        break;

      case 'splice':
        // Reserved for richer array diffs; keep client support even if rare today.
        if (Array.isArray(current)) {
          const {start, deleteCount, items} = value;
          const nextArray = [...current];
          nextArray.splice(start, deleteCount, ...items);
          sig.value = nextArray;
        }
        break;

      default:
        sig.value = value;
    }
  }
}
