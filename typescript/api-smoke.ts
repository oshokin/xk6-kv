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
}

void typecheckSmoke;
