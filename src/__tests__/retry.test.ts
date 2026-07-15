import { describe, it, expect, vi } from "vitest";
import { buildRetry } from "../lib/retry";

// Sleep is injected everywhere so no test touches a real timer.

describe("buildRetry — happy path", () => {
  it("returns the result on the first try, without retrying or sleeping", async () => {
    const sleep = vi.fn(async () => {});
    const onError = vi.fn<(err: unknown, attempt: number) => void>();
    const fn = vi.fn(async () => "done");

    const result = await buildRetry({ maxAttempts: 3, sleep, onError })(fn);

    expect(result).toBe("done");
    expect(fn).toHaveBeenCalledTimes(1);
    expect(onError).not.toHaveBeenCalled();
    expect(sleep).not.toHaveBeenCalled();
  });
});

describe("buildRetry — recovery", () => {
  it("retries transient failures, backs off exponentially, then succeeds", async () => {
    const sleep = vi.fn(async () => {});
    const onError = vi.fn<(err: unknown, attempt: number) => void>();

    let calls = 0;
    const fn = vi.fn(async () => {
      calls += 1;
      if (calls <= 2) throw new Error(`fail ${calls}`);
      return "recovered";
    });

    const result = await buildRetry({
      maxAttempts: 3,
      baseDelayMs: 100,
      sleep,
      onError,
    })(fn);

    expect(result).toBe("recovered");
    expect(fn).toHaveBeenCalledTimes(3);

    // onError fires on each failed attempt, with the 1-based attempt number.
    expect(onError).toHaveBeenCalledTimes(2);
    expect(onError).toHaveBeenNthCalledWith(1, expect.any(Error), 1);
    expect(onError).toHaveBeenNthCalledWith(2, expect.any(Error), 2);

    // Backoff doubles: 100ms before retry 2, 200ms before retry 3.
    expect(sleep).toHaveBeenNthCalledWith(1, 100);
    expect(sleep).toHaveBeenNthCalledWith(2, 200);
  });
});

describe("buildRetry — exhaustion", () => {
  it("re-throws the LAST error and does not sleep after the final attempt", async () => {
    const sleep = vi.fn(async () => {});
    const onError = vi.fn<(err: unknown, attempt: number) => void>();

    let calls = 0;
    const fn = vi.fn(async () => {
      calls += 1;
      throw new Error(`fail ${calls}`);
    });

    await expect(
      buildRetry({ maxAttempts: 3, baseDelayMs: 100, sleep, onError })(fn),
    ).rejects.toThrow("fail 3");

    expect(fn).toHaveBeenCalledTimes(3);
    expect(onError).toHaveBeenCalledTimes(3);
    // Sleeps happen between attempts only — not after the last one.
    expect(sleep).toHaveBeenCalledTimes(2);
  });
});
