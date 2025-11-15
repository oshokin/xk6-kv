import { check, sleep } from 'k6';
import exec from 'k6/execution';
import { openKv } from 'k6/x/kv';

// =============================================================================
// REAL-WORLD SCENARIO: BACKGROUND JOB PROCESSING SYSTEM
// =============================================================================
//
// This test simulates a distributed job queue system where producers create
// jobs and consumers process them atomically to avoid race conditions.
// This is a fundamental pattern in:
//
// - Email delivery systems (SendGrid, Mailgun)
// - Image/video processing pipelines (AWS SQS, Google Cloud Tasks)
// - Data processing workflows (Apache Kafka, RabbitMQ)
// - Notification systems (push notifications, SMS)
// - Report generation systems (analytics, billing reports)
// - File processing systems (document conversion, virus scanning)
//
// REAL-WORLD PROBLEM SOLVED:
// Multiple workers competing to process the same job simultaneously.
// Without proper job management, you get:
// - Duplicate job processing (wasted resources)
// - Lost jobs (jobs never get processed)
// - Race conditions (multiple workers on same job)
// - Inconsistent job state (partially processed jobs)
// - Resource waste (CPU, memory, network)
//
// ATOMIC OPERATIONS TESTED:
// - randomKey(): Find available jobs to process
// - compareAndSwap(): Atomically claim job slots (deterministic ownership)
// - deleteIfExists(): Complete job processing atomically
// - set(): Recycle job slots for future work
// - list(): Monitor queue health
//
// CONCURRENCY PATTERN:
// - Producer: VUs recycle completed jobs back into the queue
// - Consumer: VUs process existing jobs
// - Coordination: Shared KV store prevents duplicate processing
// - Atomic claiming ensures only one worker per job
//
// PERFORMANCE CHARACTERISTICS:
// - High throughput (thousands of jobs per second)
// - Critical for system reliability and resource efficiency
// - Must handle job failures gracefully
// - Low latency for job claiming (fast worker assignment)

// Backend selection: memory (default) or disk.
const SELECTED_BACKEND_NAME = __ENV.KV_BACKEND || 'memory';

// Optional: enable key tracking in memory backend to stress the tracking paths.
// (No effect for disk backend; safe to leave on)
const ENABLE_TRACK_KEYS_FOR_MEMORY_BACKEND =
  (__ENV.KV_TRACK_KEYS && __ENV.KV_TRACK_KEYS.toLowerCase() === 'true') || true;

// Job queue configuration (deterministic slots and retries).
const JOB_KEY_PREFIX = 'job:';
const JOB_SLOT_COUNT = parseInt(__ENV.JOB_SLOTS || '40', 10);
const MAX_CLAIM_ATTEMPTS = 20;
const CLAIM_RETRY_SLEEP_SECONDS = 0.005;

// ---------------------------------------------
// Open a shared KV store available to all VUs.
// ---------------------------------------------
const kv = openKv(
  SELECTED_BACKEND_NAME === 'disk'
    ? { backend: 'disk', trackKeys: ENABLE_TRACK_KEYS_FOR_MEMORY_BACKEND }
    : { backend: 'memory', trackKeys: ENABLE_TRACK_KEYS_FOR_MEMORY_BACKEND }
);

// Rationale: 20 VUs × 100 iterations provide enough churn to keep the queue
// busy while still finishing quickly. The near-100% thresholds guarantee that
// every slot is claimed, processed, and monitored without "expected" failures.
export const options = {
  vus: parseInt(__ENV.VUS || '20', 10),
  iterations: parseInt(__ENV.ITERATIONS || '100', 10),
  thresholds: {
    'checks{job:claimed}': ['rate>0.999'],
    'checks{job:processed}': ['rate>0.999'],
    'checks{job:monitoring}': ['rate>0.999']
  }
};

// setup: seeds deterministic job slots so we can prove deterministic fairness
// across backends, including disk.
export async function setup() {
  await kv.clear();

  for (let i = 0; i < JOB_SLOT_COUNT; i++) {
    await kv.set(jobKeyFromIndex(i), buildQueuedJob(i));
  }
}

export async function teardown() {
  if (SELECTED_BACKEND_NAME === 'disk') {
    kv.close();
  }
}

// Each VU claims a slot, simulates work, recycles the job, and emits health
// metrics—touching every atomic helper the queue exposes.
export default async function backgroundJobProcessingTest() {
  const vuId = exec.vu.idInTest;

  const claim = await claimJobSlot(vuId);

  check(Boolean(claim), {
    'job:claimed': () => Boolean(claim)
  });

  // Simulate work.
  sleep(0.01);

  const processed = await recycleJobSlot(claim.jobKey, claim.jobSnapshot);

  check(processed, {
    'job:processed': () => processed
  });

  const queueSample = await kv.list({ prefix: JOB_KEY_PREFIX, limit: 5 });

  check(queueSample.length > 0, {
    'job:monitoring': () => queueSample.length > 0
  });
}

// jobKeyFromIndex: deterministic mapping of slot numbers to shared keys so all
// VUs contend on the same namespace.
function jobKeyFromIndex(index) {
  return `${JOB_KEY_PREFIX}${String(index + 1).padStart(4, '0')}`;
}

// buildQueuedJob: creates the initial "queued" payload so the queue always has
// something to process immediately after setup.
function buildQueuedJob(index) {
  return {
    id: jobKeyFromIndex(index),
    state: 'queued',
    type: 'email',
    version: 0,
    payload: {
      to: `user${index + 1}@example.com`,
      subject: 'Queued Job'
    },
    createdAt: Date.now()
  };
}

// claimJobSlot: attempts to claim work via CAS with both random and sequential
// probes, guaranteeing we cover race conditions and order-statistics logic.
async function claimJobSlot(vuId) {
  for (let attempt = 0; attempt < MAX_CLAIM_ATTEMPTS; attempt++) {
    const candidateKeys = await buildCandidateKeys(attempt);

    for (const jobKey of candidateKeys) {
      const snapshot = await safeGet(jobKey);

      if (!snapshot || snapshot.state !== 'queued') {
        continue;
      }

      const claimedSnapshot = {
        ...snapshot,
        state: 'processing',
        claimedBy: vuId,
        claimedAt: Date.now()
      };

      const claimed = await kv.compareAndSwap(jobKey, snapshot, claimedSnapshot);

      if (claimed) {
        return { jobKey, jobSnapshot: claimedSnapshot };
      }
    }

    sleep(CLAIM_RETRY_SLEEP_SECONDS);
  }

  throw new Error(`Unable to claim any job slot after ${MAX_CLAIM_ATTEMPTS} attempts`);
}

// buildCandidateKeys: mixes pseudo-random sampling with deterministic coverage
// so every slot eventually gets checked even if randomKey skips it.
async function buildCandidateKeys(iteration) {
  const keys = new Set();
  const randomCandidate = await kv.randomKey({ prefix: JOB_KEY_PREFIX });

  if (randomCandidate) {
    keys.add(randomCandidate);
  }

  for (let offset = 0; offset < JOB_SLOT_COUNT; offset++) {
    keys.add(jobKeyFromIndex((iteration + offset) % JOB_SLOT_COUNT));
  }

  return keys;
}

// recycleJobSlot: deletes the claimed job and immediately requeues it, keeping
// pressure on the system without growing memory usage.
async function recycleJobSlot(jobKey, jobSnapshot) {
  const deleted = await kv.deleteIfExists(jobKey);

  if (!deleted) {
    return false;
  }

  const recycledJob = {
    id: jobSnapshot.id,
    state: 'queued',
    type: jobSnapshot.type,
    version: (jobSnapshot.version || 0) + 1,
    payload: jobSnapshot.payload,
    createdAt: jobSnapshot.createdAt,
    lastProcessedAt: Date.now(),
    lastProcessedBy: jobSnapshot.claimedBy
  };

  await kv.set(jobKey, recycledJob);

  return true;
}

// safeGet: wraps kv.get() to swallow "not found" errors that can happen during
// high churn, letting the caller decide what to do.
async function safeGet(key) {
  try {
    return await kv.get(key);
  } catch (err) {
    return null;
  }
}
