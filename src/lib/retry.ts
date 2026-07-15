/**
 * A small, reusable retry runner for transient failures (rate limits, dropped
 * streams). Unlike a tight `for` loop, it waits between attempts with
 * exponential backoff so we give the upstream time to recover instead of
 * hammering it — which in practice just fails again.
 */

export interface RetryOptions {
  /** Maximum number of attempts, including the first. */
  maxAttempts: number;
  /** Delay before the first retry, in ms. Doubles on each subsequent retry. */
  baseDelayMs?: number;
  /** Injectable sleep — override in tests to avoid real timers. */
  sleep?: (ms: number) => Promise<void>;
  /** Called on every failed attempt (e.g. to log the failure). */
  onError?: (err: unknown, attempt: number) => void;
}

const defaultSleep = (ms: number): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, ms));

/**
 * Builds a bounded retry runner. The returned function invokes `fn` up to
 * `maxAttempts` times, backing off `baseDelayMs * 2^(n-1)` before retry `n`.
 * If every attempt throws, the last error is re-thrown.
 */
export function buildRetry({
  maxAttempts,
  baseDelayMs = 100,
  sleep = defaultSleep,
  onError,
}: RetryOptions) {
  return async function run<T>(
    fn: (attempt: number) => Promise<T>,
  ): Promise<T> {
    let lastError: unknown;
    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
      try {
        return await fn(attempt);
      } catch (err) {
        lastError = err;
        onError?.(err, attempt);

        const isLastAttempt = attempt === maxAttempts;
        if (!isLastAttempt) {
          await sleep(baseDelayMs * 2 ** (attempt - 1));
        }
      }
    }
    throw lastError;
  };
}
