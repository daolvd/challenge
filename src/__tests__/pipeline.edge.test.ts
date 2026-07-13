import { describe, it, expect, vi } from "vitest";
import { generate, MAX_REVISIONS } from "../lib/pipeline";

/**
 * The gate tests assert on `status` alone, which leaves two plausible "fixes"
 * passing while still being wrong in production. These cover that gap.
 */

describe("the revision loop is a real circuit breaker, not a big number", () => {
  it("reviews at most MAX_REVISIONS times, numbered from zero, and never hands off", async () => {
    const reviewed: number[] = [];
    const advanceToNextStage = vi.fn(async () => {});

    const res = await generate({
      behavior: "ok",
      advanceToNextStage,
      reviewPasses: (revision) => {
        reviewed.push(revision);
        return false;
      },
    });

    // A loop bounded by some arbitrary ceiling (50, 100, ...) still satisfies
    // `attempts <= 3` if it reports the wrong count, and an off-by-one silently
    // burns or skips a revision. Pin the exact calls instead.
    expect(reviewed).toEqual([0, 1, 2]);
    expect(res.attempts).toBe(MAX_REVISIONS);
    expect(res.status).toBe("error");

    // A draft that never passed review must not reach the next stage.
    expect(advanceToNextStage).not.toHaveBeenCalled();
  });
});

describe("the hand-off is awaited, not fire-and-forget", () => {
  it("fails the run when the hand-off rejects on a later tick", async () => {
    const res = await generate({
      behavior: "ok",
      advanceToNextStage: async () => {
        await new Promise((resolve) => setTimeout(resolve, 10));
        throw new Error("next stage unreachable");
      },
      reviewPasses: () => true,
    });

    // The original bug was `void advance().catch(() => {})`. A hand-off that
    // rejects immediately can mask that; one that rejects after an await cannot,
    // because `generate` will have long since returned "ok" if it isn't awaiting.
    expect(res.status).toBe("error");
    expect(res.reason).toBe("next stage unreachable");
  });
});
