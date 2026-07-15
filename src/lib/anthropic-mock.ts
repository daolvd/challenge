import full from "../fixtures/deck.full.json";
import truncated from "../fixtures/deck.truncated.json";

/**
 * A deterministic stand-in for the streaming Anthropic client used by the real
 * ClickedOn content pipeline. It never hits the network: behaviour is driven
 * entirely by `behavior` + a per-run call counter, so tests are reproducible.
 *
 * - "ok"                      -> returns the full fenced-JSON response.
 * - "truncate-once"           -> the FIRST call returns a response cut off
 *                                mid-JSON (as if the stream dropped); later calls
 *                                return full.
 * - "transient-429-twice"     -> the first TWO calls throw a 429; the third
 *                                succeeds.
 * - "truncate-on-revision"    -> the initial draft (call 1) is full, but the
 *                                FIRST revision re-generation (call 2) is cut off
 *                                mid-JSON; later calls return full.
 * - "truncate-every-revision" -> the initial draft (call 1) is full, but EVERY
 *                                revision re-generation (calls >= 2) is truncated.
 */
export type MockBehavior =
  | "ok"
  | "truncate-once"
  | "transient-429-twice"
  | "truncate-on-revision"
  | "truncate-every-revision";

export interface MockState {
  calls: number;
}

export interface TransientError extends Error {
  status?: number;
}

export async function mockStream(
  behavior: MockBehavior,
  state: MockState,
): Promise<string> {
  state.calls += 1;

  if (behavior === "transient-429-twice" && state.calls <= 2) {
    const err: TransientError = new Error("Rate limited (429)");
    err.status = 429;
    throw err;
  }

  if (behavior === "truncate-once" && state.calls === 1) {
    return (truncated as { text: string }).text;
  }

  // Call 1 is the initial draft (stage 1); calls >= 2 are revision re-generations.
  if (behavior === "truncate-on-revision" && state.calls === 2) {
    return (truncated as { text: string }).text;
  }

  if (behavior === "truncate-every-revision" && state.calls >= 2) {
    return (truncated as { text: string }).text;
  }

  return (full as { text: string }).text;
}
