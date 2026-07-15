import { describe, it, expect, vi, afterEach } from "vitest";
import { generate, MAX_REVISIONS, MAX_RETRIES } from "../lib/pipeline";

/**
 * Real-world edge cases the gate tests (pipeline.test.ts) don't reach.
 *
 * The pipeline runs in two streaming stages: an initial draft (up to MAX_RETRIES
 * attempts, with backoff) and a revision loop (up to MAX_REVISIONS rounds). Each
 * revision round RE-GENERATES the draft and then reviews it — so a round can fail
 * either because re-generation errored (rate limit / truncation) or because the
 * reviewer rejected it. Both consume one of the MAX_REVISIONS rounds. Worst case
 * the model is streamed MAX_RETRIES + MAX_REVISIONS (= 6) times.
 */

afterEach(() => {
  vi.restoreAllMocks();
});

describe("the run is bounded to MAX_RETRIES + MAX_REVISIONS model calls", () => {
  it("documents the 6-call ceiling", () => {
    expect(MAX_RETRIES + MAX_REVISIONS).toBe(6);
  });
});

describe("revision loop — re-generates then reviews, passing on a later round", () => {
  it("keeps re-generating until review passes and reports the round reached", async () => {
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

describe("a re-generation error during revision consumes a round and recovers", () => {
  it("counts a truncated re-generation as one round, then succeeds on the next", async () => {
    const reviewed: number[] = [];

    const res = await generate({
      behavior: "truncate-on-revision", // round 0's re-generation is truncated
      advanceToNextStage: async () => {},
      reviewPasses: (revision) => {
        reviewed.push(revision);
        return true;
      },
    });

    // Round 0 failed in re-generation before review ran, so review only saw round 1.
    expect(reviewed).toEqual([1]);
    expect(res.status).toBe("ok");
    expect(res.attempts).toBe(1);
  });

  it("shares the MAX_REVISIONS budget between generation errors and rejections", async () => {
    const reviewed: number[] = [];

    const res = await generate({
      behavior: "truncate-on-revision", // round 0 re-generation truncates...
      advanceToNextStage: async () => {},
      // ...round 1 is rejected, round 2 passes — all three rounds are spent.
      reviewPasses: (revision) => {
        reviewed.push(revision);
        return revision === 2;
      },
    });

    expect(reviewed).toEqual([1, 2]); // round 0 never reached review
    expect(res.status).toBe("ok");
    expect(res.attempts).toBe(2);
  });
});

describe("the revision loop gives up when re-generation keeps failing", () => {
  it("stops after MAX_REVISIONS truncated re-generations and never hands off", async () => {
    const reviewPasses = vi.fn(() => true);
    const advanceToNextStage = vi.fn(async () => {});

    const res = await generate({
      behavior: "truncate-every-revision", // every re-generation truncates
      advanceToNextStage,
      reviewPasses,
    });

    expect(res.status).toBe("error");
    expect(res.attempts).toBe(MAX_REVISIONS);
    // Generation failed before review each round, and the run never advanced.
    expect(reviewPasses).not.toHaveBeenCalled();
    expect(advanceToNextStage).not.toHaveBeenCalled();
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

describe("failures are logged distinctly, with a timestamp and duration", () => {
  it("logs a different, timestamped message for a rate limit vs a truncated stream", async () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});

    // A 429 in the initial stream.
    await generate({
      behavior: "transient-429-twice",
      advanceToNextStage: async () => {},
      reviewPasses: () => true,
    });

    // A truncated re-generation in the revision loop.
    await generate({
      behavior: "truncate-on-revision",
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
