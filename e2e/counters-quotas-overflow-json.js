// For detailed test implementation, see: common-counters-quotas-overflow.js

import { createOverflowScenario, overflowOptions } from './common-counters-quotas-overflow.js';

// Use standard overflow test options (single VU, single iteration).
export const options = overflowOptions;

// Run overflow protection tests with JSON serialization backend.
export default createOverflowScenario('json');
