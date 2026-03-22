import { check } from 'k6';
import exec from 'k6/execution';
import { VUS, ITERATIONS, createKv, createSetup, createTeardown } from './common.js';

// =============================================================================
// REAL-WORLD SCENARIO: CLAIM BOOTSTRAP + MALFORMED PAYLOAD TRIAGE
// =============================================================================
//
// A reservation service creates claim locks with compareAndSwap(key, null, value).
// During incidents, engineers observe:
// - compareAndSwap(..., null, ...) returned false
// - exists() later returned false
// and often assume the CAS call silently failed with an internal error.
//
// REAL-WORLD PROBLEMS UNCOVERED:
// - False positives where interleaving delete operations are mistaken for CAS errors.
// - Confusion between compare mismatch (boolean false) and true Promise rejections.
// - Legacy API behavior drift after introducing richer compare APIs.
//
// ATOMIC OPERATIONS TESTED:
// - compareAndSwap(): legacy boolean CAS semantics (including null absent-sentinel).
// - compareAndDelete(): legacy boolean CAD null-value comparison semantics.
// - compareAndSwapDetailed(): mismatch introspection for incident debugging.
// - setIfAbsent(): first-writer-wins under contention.
//
// CONCURRENCY PATTERN:
// - Promise.all setIfAbsent() attempts target one bootstrap key to guarantee a
//   single winner while other phases stay deterministic per VU.
//
// PERFORMANCE CHARACTERISTICS:
// - Lightweight per-iteration diagnostics with a short parallel bootstrap burst.

// Test name used for generating test-specific database and snapshot paths.
const TEST_NAME = 'cas-null-rejection-diagnostics';

// kv is the shared store client used throughout the scenario.
const kv = createKv(TEST_NAME);

// Prefix for claim keys used in legacy vs detailed CAS/CAD contract checks.
const CLAIM_PREFIX = __ENV.CLAIM_PREFIX || 'claim:diag:';

// Number of concurrent setIfAbsent() callers competing for the same bootstrap key.
const CONCURRENT_BOOTSTRAPPERS = parseInt(__ENV.CONCURRENT_BOOTSTRAPPERS || '8', 10);

// options configures the load profile and pass/fail thresholds.
export const options = {
  vus: VUS,
  iterations: ITERATIONS,
  thresholds: {
    'checks{op:casLegacy,phase:deterministic}': ['rate==1.0'],
    'checks{op:compareDelete,contract:null-semantics}': ['rate==1.0'],
    'checks{op:rejection,phase:deterministic}': ['rate==1.0'],
    'checks{op:setIfAbsent,phase:contention}': ['rate>0.995']
  }
};

// setup clears residual claim keys before contract and rejection probes run.
export const setup = createSetup(kv);

// teardown closes disk stores so repeated runs do not collide.
export const teardown = createTeardown(kv, TEST_NAME);

// casNullRejectionDiagnostics separates boolean false (compare mismatch) from
// Promise rejections, documents null CAD semantics, and stresses setIfAbsent().
export default async function casNullRejectionDiagnostics() {
  const iter = exec.scenario.iterationInTest;
  const claimKey = `${CLAIM_PREFIX}${exec.vu.idInInstance}:${iter}`;
  const claimValue = { owner: `vu-${exec.vu.idInInstance}`, createdAt: Date.now() };

  // Legacy CAS: null expected means "only swap if absent"; key exists so both APIs refuse.
  await kv.set(claimKey, claimValue);

  const legacyCas = await kv.compareAndSwap(claimKey, null, { owner: 'other' });
  const detailedCas = await kv.compareAndSwapDetailed(
    claimKey,
    null,
    { owner: 'other' },
    { includeCurrentOnMismatch: true }
  );

  check(true, {
    'casLegacy:deterministic': () =>
      legacyCas === false &&
      detailedCas.swapped === false &&
      detailedCas.reason === 'mismatch' &&
      detailedCas.existed === true &&
      Object.prototype.hasOwnProperty.call(detailedCas, 'current')
  }, { op: 'casLegacy', phase: 'deterministic' });

  // Interleaving: another worker may delete between CAS and exists(); not a store bug.
  await kv.delete(claimKey);
  const existsAfterDelete = await kv.exists(claimKey);

  check(true, {
    'casLegacy:deterministic': () => existsAfterDelete === false
  }, { op: 'casLegacy', phase: 'deterministic' });

  // Legacy compareAndDelete: null/undefined expected values compare to stored JSON null,
  // they are not absent-key sentinels like CAS null-expected.
  const nullKey = `${claimKey}:null`;
  await kv.set(nullKey, null);
  const deleteWithNull = await kv.compareAndDelete(nullKey, null);

  const undefinedKey = `${claimKey}:undefined`;
  await kv.set(undefinedKey, null);
  const deleteWithUndefinedDetailed = await kv.compareAndDeleteDetailed(
    undefinedKey,
    undefined,
    { includeCurrentOnMismatch: true }
  );

  const nonNullKey = `${claimKey}:non-null`;
  await kv.set(nonNullKey, 'active');
  const nullMismatch = await kv.compareAndDeleteDetailed(nonNullKey, null, { includeCurrentOnMismatch: true });

  check(true, {
    'compareDelete:null-semantics': () =>
      deleteWithNull === true &&
      deleteWithUndefinedDetailed.deleted === true &&
      nullMismatch.deleted === false &&
      nullMismatch.reason === 'mismatch' &&
      nullMismatch.existed === true &&
      Object.prototype.hasOwnProperty.call(nullMismatch, 'current')
  }, { op: 'compareDelete', contract: 'null-semantics' });

  // Serializer failures must reject; boolean false is reserved for compare mismatch.
  const brokenKey = `${claimKey}:broken`;
  const cyclic = {};
  cyclic.self = cyclic;

  let rejected = false;
  try {
    await kv.compareAndSwap(brokenKey, null, cyclic);
  } catch (err) {
    rejected = String(err?.name || '').length > 0;
  }

  check(true, {
    'rejection:deterministic': () => rejected === true
  }, { op: 'rejection', phase: 'deterministic' });

  // Burst bootstrap: many workers race the same coordination key; one succeeds.
  const bootstrapKey = `${claimKey}:bootstrap`;
  const attempts = await Promise.all(
    Array.from({ length: CONCURRENT_BOOTSTRAPPERS }, (_, idx) =>
      kv.setIfAbsent(bootstrapKey, `bootstrapper-${idx}`)
    )
  );
  const winners = attempts.filter(Boolean).length;

  check(true, {
    'setIfAbsent:contention': () => winners === 1
  }, { op: 'setIfAbsent', phase: 'contention' });
}
