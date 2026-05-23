// Compile-time smoke checks for public xk6-kv declarations.
// CI runs `npm run typecheck` to validate this file.
import { openKv } from 'k6/x/kv';

const kv = openKv({
  backend: 'memory',
  serialization: 'json',
  trackKeys: true
});

async function typecheckSmoke(): Promise<void> {
  await kv.setMany({
    'user:1': { name: 'Alice' },
    'user:2': { name: 'Bob' }
  });

  const sampled = await kv.randomKeys({ prefix: 'user:', count: 2, unique: true });
  for (const key of sampled) {
    const asString: string = key;
    void asString;
  }

  const keyPage = await kv.scanKeys({ prefix: 'user:', limit: 10 });
  const keys: string[] = keyPage.keys;
  const done: boolean = keyPage.done;
  const cursor: string = keyPage.cursor;
  void keys;
  void done;
  void cursor;

  const claim = await kv.claimRandom<{ name: string }>({
    prefix: 'user:',
    owner: 'typecheck-smoke',
    ttl: 1000
  });
  if (claim) {
    const claimToken: number = claim.token;
    const claimedName: string = claim.entry.value.name;
    void claimToken;
    void claimedName;
  }

  const exportSummary = await kv.exportJSONL({
    fileName: '.k6.kv.typescript-smoke.jsonl',
    prefix: 'user:',
    limit: 1
  });
  const exported: number = exportSummary.exported;
  const bytesWritten: number = exportSummary.bytesWritten;
  const fileName: string = exportSummary.fileName;
  void exported;
  void bytesWritten;
  void fileName;

  const exportCSVSummary = await kv.exportCSV({
    fileName: '.k6.kv.typescript-smoke.csv',
    prefix: 'user:',
    columns: ['name'],
    includeKey: true,
    delimiter: ',',
    limit: 1
  });
  const exportedCSV: number = exportCSVSummary.exported;
  void exportedCSV;

  const allocation = await kv.allocationStats({ prefix: 'user:' });
  const claimable: number = allocation.claimable;
  void claimable;

  const claimKeysResult = await kv.claimKeys<{ name: string }>(['user:1'], {
    ttl: 1000,
    owner: 'vu-1',
    allOrNothing: false
  });
  const claimKeysBusy: string[] = claimKeysResult.busy;
  void claimKeysBusy;
  // @ts-expect-error claimKeys() accepts explicit keys and does not support prefix filtering.
  await kv.claimKeys(['user:1'], { prefix: 'user:' });

  if (claim && claimKeysResult.claimed.length > 0) {
    const lifecycleRefs = [claim, claimKeysResult.claimed[0]];
    const releaseMany = await kv.releaseClaims(lifecycleRefs);
    const completeMany = await kv.completeClaims(lifecycleRefs, { deleteKey: false });
    const renewMany = await kv.renewClaims(lifecycleRefs, { ttl: 1000 });
    void releaseMany;
    void completeMany;
    void renewMany;
  }

  const validateCSVResult = await kv.validateCSV({
    fileName: './examples/fixtures/users.csv',
    keyColumn: 'id',
    hasHeader: true
  });
  const csvValid: boolean = validateCSVResult.valid;
  const csvCheckedAll: boolean = validateCSVResult.checkedAll;
  void csvValid;
  void csvCheckedAll;

  const validateJSONLResult = await kv.validateJSONL({
    fileName: './examples/fixtures/users.jsonl',
    limit: 1
  });
  const jsonlValid: boolean = validateJSONLResult.valid;
  const jsonlCheckedAll: boolean = validateJSONLResult.checkedAll;
  void jsonlValid;
  void jsonlCheckedAll;
}

void typecheckSmoke;
