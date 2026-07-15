import { extractJson } from "./extract-json";
import { mockStream, type MockBehavior, type MockState } from "./anthropic-mock";
import { buildRetry } from "./retry";

export interface GenerateInput {
  /** Drives the mock streaming client (see anthropic-mock.ts). */
  behavior: MockBehavior;
  /** Hands the finished draft to the next pipeline stage. May reject. */
  advanceToNextStage: () => Promise<void>;
  /** Returns true once the given revision passes review. Scripted by callers/tests. */
  reviewPasses: (revision: number) => boolean;
}

export interface GenerateResult {
  status: "ok" | "error";
  attempts: number;
}

/** Initial-draft stream: transient errors (rate limits) + truncated streams. */
const MAX_RETRIES = 3;
/** Revision rounds: each re-generates the draft and re-reviews it. */
const MAX_REVISIONS = 3;
// Worst case the model is streamed MAX_RETRIES + MAX_REVISIONS (= 6) times per run.

/** The distinct ways a single attempt can fail, each logged differently. */
type FailureKind = "rate-limited" | "truncated" | "review-rejected";

/** A distinct, human-readable cause per failure kind. */
const FAILURE_DESCRIPTION: Record<FailureKind, string> = {
  "rate-limited": "upstream rate-limited the request (429)",
  truncated: "stream returned incomplete JSON (truncated mid-response)",
  "review-rejected": "reviewer rejected the draft",
};

/** Thrown when a re-generated draft streams fine but the reviewer rejects it. */
class ReviewRejected extends Error {
  constructor(revision: number) {
    super(`draft rejected at revision ${revision}`);
    this.name = "ReviewRejected";
  }
}

/** Normalises a thrown value into a human-readable reason string. */
function errorReason(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}

/** Buckets a thrown value into one of the known failure kinds. */
function classifyFailure(err: unknown): FailureKind {
  if (err instanceof ReviewRejected) return "review-rejected";
  if ((err as { status?: number }).status === 429) return "rate-limited";
  return "truncated";
}

/**
 * Logs a failed attempt with a message distinct to its kind, its position in the
 * attempt budget, and how long the attempt took — so rate limits, truncated
 * streams and review rejections are each recognisable at a glance in the logs.
 */
function logFailure(
  stage: "draft-stream" | "revision",
  err: unknown,
  attempt: number,
  maxAttempts: number,
  elapsedMs: number,
): void {
  const kind = classifyFailure(err);
  console.warn(
    `[pipeline] ${new Date().toISOString()} ${stage} attempt ${attempt}/${maxAttempts} ` +
      `failed in ${elapsedMs}ms — ${kind}: ${FAILURE_DESCRIPTION[kind]} (${errorReason(err)})`,
  );
}

/** Streams one draft and validates its JSON. Throws on truncation/transient errors. */
async function generateDraft(behavior: MockBehavior, state: MockState): Promise<void> {
  const text = await mockStream(behavior, state);
  extractJson(text); // throws when the stream was truncated mid-JSON
}

/**
 * Stage 1 — produce the initial draft, retrying transient failures (rate limits)
 * and truncated streams with exponential backoff instead of hammering the
 * upstream. Up to MAX_RETRIES attempts; throws if every one fails.
 */
async function streamInitialDraft(behavior: MockBehavior, state: MockState): Promise<void> {
  let startedAt = 0;
  const withRetry = buildRetry({
    maxAttempts: MAX_RETRIES,
    onError: (err, attempt) =>
      logFailure("draft-stream", err, attempt, MAX_RETRIES, Date.now() - startedAt),
  });

  await withRetry(async () => {
    startedAt = Date.now();
    await generateDraft(behavior, state);
  });
}

interface ReviewOutcome {
  passed: boolean;
  /** Revision index the loop reached (0-based). */
  attempts: number;
}

/**
 * Stage 2 — re-generate the draft and review it, up to MAX_REVISIONS rounds. A
 * round is retried whether re-generation errored (rate limit / truncation) or the
 * reviewer rejected the draft; both consume one of the MAX_REVISIONS attempts and
 * are logged with their distinct cause. No backoff: this is a fast review cycle,
 * not a flaky-network wait.
 */
async function reviseUntilApproved(
  behavior: MockBehavior,
  state: MockState,
  reviewPasses: (revision: number) => boolean,
): Promise<ReviewOutcome> {
  let attempts = 0;
  let startedAt = 0;
  const withRetry = buildRetry({
    maxAttempts: MAX_REVISIONS,
    baseDelayMs: 0,
    onError: (err, attempt) =>
      logFailure("revision", err, attempt, MAX_REVISIONS, Date.now() - startedAt),
  });

  try {
    await withRetry(async (n) => {
      const revision = n - 1;
      attempts = revision;
      startedAt = Date.now();

      await generateDraft(behavior, state); // re-generate before re-reviewing
      if (!reviewPasses(revision)) {
        throw new ReviewRejected(revision);
      }
    });
    return { passed: true, attempts };
  } catch {
    return { passed: false, attempts };
  }
}

/**
 * Runs one content-generation pass: stream the initial draft, then re-generate
 * and review it until it passes, then hand off to the next stage. Any stage
 * failing short-circuits the run into an `error` result.
 */
export async function generate(input: GenerateInput): Promise<GenerateResult> {
  const state: MockState = { calls: 0 };

  try {
    await streamInitialDraft(input.behavior, state);
  } catch (err) {
    console.error(
      `[pipeline] draft stream gave up after ${state.calls} attempts: ${errorReason(err)}`,
    );
    return { status: "error", attempts: state.calls };
  }

  const review = await reviseUntilApproved(input.behavior, state, input.reviewPasses);
  if (!review.passed) {
    return { status: "error", attempts: MAX_REVISIONS };
  }

  try {
    await input.advanceToNextStage();
  } catch (err) {
    console.error(`[pipeline] hand-off to next stage failed: ${errorReason(err)}`);
    return { status: "error", attempts: review.attempts };
  }

  return { status: "ok", attempts: review.attempts };
}

export { MAX_REVISIONS, MAX_RETRIES };
