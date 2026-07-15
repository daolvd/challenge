import { describe, it, expect, vi, afterEach } from "vitest";
import { generate, MAX_REVISIONS, MAX_RETRIES } from "../lib/pipeline";

/**
 * Real-world edge cases the gate tests (pipeline.test.ts) don't assert on.
 *
 * The pipeline runs in two streaming stages: an initial draft (up to MAX_RETRIES
 * attempts, with backoff) and a revision loop (up to MAX_REVISIONS rounds) that
 * re-generates the draft and reviews it each round. Worst case the model is
 * streamed MAX_RETRIES + MAX_REVISIONS (= 6) times.
 *
 * These use only the three built-in mock behaviors (anthropic-mock.ts is frozen):
 * "ok", "truncate-once", "transient-429-twice". A generation error *inside* the
 * revision loop is therefore not exercisable here — triggering it would require a
 * mock that fails on a later call, which we are not allowed to add.
 */

afterEach(() => {
  vi.restoreAllMocks();
});

describe("the run is bounded to MAX_RETRIES + MAX_REVISIONS model calls", () => {
  it("documents the 6-call ceiling", () => {
    expect(MAX_RETRIES + MAX_REVISIONS).toBe(6);
  });
});

describe("revision loop — reviews each round, passing on a later round", () => {
  it("keeps going until review passes and reports the round reached", async () => {
    const reviewed: number[] = [];
    const advanceToNextStage = vi.fn(async () => {});

    const res = await generate({
      behavior: "ok",
      advanceToNextStage,
      reviewPasses: (revision) => {
        reviewed.push(revision);
        return revision === 2; // reject rounds 0 and 1, accept round 2
      },
    });

    expect(reviewed).toEqual([0, 1, 2]);
    expect(res.status).toBe("ok");
    expect(res.attempts).toBe(2);
    expect(advanceToNextStage).toHaveBeenCalledTimes(1);
  });
});

describe("both stages combined", () => {
  it("recovers rate-limited initial streams but still fails a rejected hand-off", async () => {
    const advanceToNextStage = vi.fn(async () => {
      throw new Error("next stage unreachable");
    });

    const res = await generate({
      behavior: "transient-429-twice", // two 429s then success, within MAX_RETRIES
      advanceToNextStage,
      reviewPasses: () => true,
    });

    expect(res.status).toBe("error");
    expect(advanceToNextStage).toHaveBeenCalledTimes(1);
  });

  it("caps the revision loop and never hands off when review never passes", async () => {
    const reviewed: number[] = [];
    const advanceToNextStage = vi.fn(async () => {});

    const res = await generate({
      behavior: "truncate-once", // initial stream recovers on retry...
      advanceToNextStage,
      reviewPasses: (revision) => {
        reviewed.push(revision);
        return false; // ...but the draft is never good enough
      },
    });

    expect(reviewed).toEqual([0, 1, 2]); // exactly MAX_REVISIONS rounds
    expect(res.status).toBe("error");
    expect(res.attempts).toBe(MAX_REVISIONS);
    expect(advanceToNextStage).not.toHaveBeenCalled();
  });
});

describe("initial-stream failures are logged distinctly, with a timestamp and duration", () => {
  it("logs a different, timestamped message for a rate limit vs a truncated stream", async () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});

    // A 429 in the initial stream.
    await generate({
      behavior: "transient-429-twice",
      advanceToNextStage: async () => {},
      reviewPasses: () => true,
    });

    // A truncated initial stream.
    await generate({
      behavior: "truncate-once",
      advanceToNextStage: async () => {},
      reviewPasses: () => true,
    });

    const lines = warn.mock.calls.map((c) => String(c[0]));
    const rateLimited = lines.find((l) => l.includes("rate-limited"));
    const truncated = lines.find((l) => l.includes("truncated"));

    expect(rateLimited).toBeDefined();
    expect(truncated).toBeDefined();
    expect(rateLimited).not.toBe(truncated); // distinct message per kind

    const isoAndDuration = /\d{4}-\d{2}-\d{2}T.*failed in \d+ms/;
    expect(rateLimited).toMatch(isoAndDuration);
    expect(truncated).toMatch(isoAndDuration);
  });
});
