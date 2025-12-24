# TypeScript Support for xk6-kv

Complete TypeScript starter kit for xk6-kv. Just copy and use!

## Quick Start

**Copy the entire `typescript/` folder to your project:**

**Linux/macOS:**

```bash
# Copy everything
cp -r typescript/ my-k6-project/
cd my-k6-project/

# Install dependencies
npm install

# Start writing tests!
# example.ts is already there as a template
```

**Windows (PowerShell):**

```powershell
# Copy everything
Copy-Item -Recurse typescript\ my-k6-project\
cd my-k6-project\

# Install dependencies
npm install

# Start writing tests!
# example.ts is already there as a template
```

## What You Get

```bash
typescript/
├── xk6-kv.d.ts       # Type definitions for xk6-kv extension
├── k6-console.d.ts   # Console types (missing from @types/k6)
├── package.json      # Declares dependencies
├── tsconfig.json     # TypeScript compiler configuration
├── example.ts        # Comprehensive usage example
└── README.md         # Setup instructions
```

**Everything is pre-configured!** Just copy the folder and run `npm install`.

## Usage

**Write your test:**

```typescript
import { openKv } from 'k6/x/kv';

export default async function () {
  const kv = openKv({ backend: 'memory' });
  await kv.set('key', 'value');
  
  const value = await kv.get('key');
  console.log(value);
  kv.close();
}
```

**Type check:**

```bash
npx tsc --noEmit
```

**Run with k6:**

```bash
k6 run example.ts
```

> ⚠️ **Snapshot defaults:** Just like the Go/JS examples, calling `await kv.backup()` without a `fileName` while you’re on `backend: 'memory'` writes into `.k6.kv`, the disk backend’s default file. That’s by design so you can `backup()` in `teardown()` and later rerun the same TypeScript script with `backend: 'disk'` to replay the captured dataset. Pass a custom `fileName` if you need a separate artifact.

## Features

✅ Full IntelliSense/autocomplete  
✅ Type checking catches errors early  
✅ Documentation hints in IDE  
