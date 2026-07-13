import { extractJson } from "./extract-json";
import {
  mockStream,
  type MockBehavior,
  type MockState,
  type TransientError,
} from "./anthropic-mock";

export interface GenerateInput {
  /** Drives the mock streaming client (see anthropic-mock.ts). */
  behavior: MockBehavior;
  /** Hands the finished draft to the next pipeline stage. May reject. */
  advanceToNextStage: () => Promise<void>;
  /** Returns true once the draft passes review. Scripted by callers/tests. */
  reviewPasses: (revision: number) => boolean;
}

export interface GenerateResult {
  status: "ok" | "error";
  /** Review rounds spent. Zero when the run never got as far as review. */
  attempts: number;
  /** The extracted draft, present only on a successful run. */
  draft?: unknown;
  /** Why the run failed, present only on an unsuccessful run. */
  reason?: string;
}

/** How many times a draft may be reviewed before the run is declared stuck. */
export const MAX_REVISIONS = 3;

/** How many times a single draft may be streamed before the run gives up. */
export const MAX_STREAM_ATTEMPTS = 3;

function messageOf(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}

function failure(attempts: number, reason: string): GenerateResult {
  return { status: "error", attempts, reason };
}

/**
 * Whether re-running the same stream could plausibly do better.
 *
 * Two failures qualify. Transient API errors — a rate limit or a server-side
 * blip — say nothing about the request itself. And a truncated response leaves
 * the JSON block unterminated, so extraction throws; those errors carry no HTTP
 * status, which is what tells them apart from an API rejection.
 *
 * Everything else — a 4xx that is not a rate limit, say — is fatal: retrying
 * only burns the budget on a request that will keep failing the same way.
 */
function isWorthRestreaming(err: unknown): boolean {
  const status = (err as TransientError | undefined)?.status;
  if (status === undefined) return true;
  return status === 429 || (status >= 500 && status < 600);
}

/** Streams a draft and extracts its JSON, re-streaming on recoverable failures. */
async function streamDraft(
  behavior: MockBehavior,
  state: MockState,
): Promise<unknown> {
  let lastError: unknown;

  for (let attempt = 1; attempt <= MAX_STREAM_ATTEMPTS; attempt += 1) {
    try {
      return extractJson(await mockStream(behavior, state));
    } catch (err) {
      if (!isWorthRestreaming(err)) throw err;
      lastError = err;
    }
  }

  throw lastError;
}

/**
 * Reviews the draft until it is approved, up to MAX_REVISIONS rounds. A reviewer
 * that never accepts must fail the run rather than spin on it, so the caller is
 * told how many rounds were spent either way.
 *
 * `reviewPasses` numbers revisions from zero; `attempts` counts them from one.
 */
function review(reviewPasses: GenerateInput["reviewPasses"]): {
  approved: boolean;
  attempts: number;
} {
  for (let revision = 0; revision < MAX_REVISIONS; revision += 1) {
    if (reviewPasses(revision)) {
      return { approved: true, attempts: revision + 1 };
    }
  }

  return { approved: false, attempts: MAX_REVISIONS };
}

/**
 * Runs one content-generation pass: stream a draft, revise it until it passes
 * review, then hand it to the next stage.
 *
 * Every failure mode resolves to `status: "error"` — a stalled run must never
 * report itself as healthy.
 */
export async function generate(input: GenerateInput): Promise<GenerateResult> {
  const state: MockState = { calls: 0 };

  let draft: unknown;
  try {
    draft = await streamDraft(input.behavior, state);
  } catch (err) {
    return failure(0, messageOf(err));
  }

  const { approved, attempts } = review(input.reviewPasses);
  if (!approved) {
    return failure(
      attempts,
      `Draft still failing review after ${MAX_REVISIONS} attempts`,
    );
  }

  // The hand-off is part of the run: if it rejects, the run failed.
  try {
    await input.advanceToNextStage();
  } catch (err) {
    return failure(attempts, messageOf(err));
  }

  return { status: "ok", attempts, draft };
}
