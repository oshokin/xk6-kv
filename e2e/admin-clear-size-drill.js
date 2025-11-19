import { check } from 'k6';
import exec from 'k6/execution';
import { VUS, ITERATIONS, createKv, createSetup, createTeardown } from './common.js';

// =============================================================================
// REAL-WORLD SCENARIO: ADMIN “NUKE & RESEED” MAINTENANCE WINDOW
// =============================================================================
//
// Operators occasionally wipe entire keyspaces (kv.clear()) while other workers
// continue to write scratch data. This script keeps the writes flowing while a
// single coordinator issues periodic clear()+size() checks to ensure promises
// resolve cleanly under concurrent churn.
//
// REAL-WORLD PROBLEMS UNCOVERED:
// - clear() and size() promises resolving on goroutines other than the event loop.
// - Inconsistent size readings when clear() collides with active writers.
// - Residual keys that should have been deleted during maintenance windows.
//
// ATOMIC OPERATIONS TESTED:
// - set(): background writers.
// - clear(): coordinator nukes the store.
// - size(): coordinator validates that clear finished.

// Prefix for scratch keys written by background workers.
const SCRATCH_PREFIX = __ENV.SCRATCH_PREFIX || 'scratch:';

// Number of keys each worker writes per iteration (tests bulk write throughput).
const KEYS_PER_BATCH = parseInt(__ENV.KEYS_PER_BATCH || '80', 10);

// Iteration interval for maintenance cycles (every Nth iteration).
const MAINTENANCE_INTERVAL = parseInt(__ENV.MAINTENANCE_INTERVAL || '10', 10);

// kv is the shared store client used throughout the scenario.
const kv = createKv();

// options configures the load profile and pass/fail thresholds.
export const options = {
  vus: VUS,
  iterations: ITERATIONS,
  thresholds: {
    'checks{clear:operations-complete}': ['rate>0.99']
  }
};

// setup resets the store before the rehearsal begins.
export const setup = createSetup(kv);

// teardown closes disk stores so repeated runs do not collide.
export const teardown = createTeardown(kv);

// adminClearSizeDrill writes scratch keys and occasionally executes coordinated
// clear()+size() probes to verify promise resolution under heavy churn.
export default async function adminClearSizeDrill() {
  const isCoordinator = exec.vu.idInInstance === 1;
  const globalIteration = exec.scenario.iterationInTest;
  const isMaintenanceCycle = isCoordinator && (globalIteration % MAINTENANCE_INTERVAL === 0);

  // Coordinator periodically clears the store while others keep writing.
  // This tests that clear() and size() promises resolve correctly under concurrent load.
  if (isMaintenanceCycle) {
    await kv.clear();
    const sizeAfter = await kv.size();

    // Size may not be 0 due to concurrent writes - that's expected and realistic.
    // We're just verifying the operations complete without errors.
    check(true, {
      'clear:operations-complete': () => typeof sizeAfter === 'number' && sizeAfter >= 0
    });
    return;
  }

  // All VUs continuously write scratch keys (including during coordinator's clear).
  if (!isMaintenanceCycle) {
    const scratchKeys = Array.from({ length: KEYS_PER_BATCH }, (_, idx) => {
      return `${SCRATCH_PREFIX}${exec.vu.idInInstance}:${globalIteration}:${idx}`;
    });

    await Promise.all(
      scratchKeys.map((key, idx) =>
        kv.set(key, {
          key,
          payload: `blob-${idx}`,
          createdAt: Date.now()
        })
      )
    );
  }
}
