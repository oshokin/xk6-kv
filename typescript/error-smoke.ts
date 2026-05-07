// Compile-time smoke checks for xk6-kv error typing.
import { type KVError, type KVErrorName } from 'k6/x/kv';

const entryError = {
  key: 'bad',
  name: 'SerializerError',
  message: 'json: unsupported type: func()'
} satisfies NonNullable<KVError['errors']>[number];

const batchError = {
  name: 'InvalidOptionsError',
  message: 'setMany validation failed: 1 invalid entry',
  errors: [entryError]
} satisfies KVError;

function consumeKVError(err: KVError): void {
  const topLevelName: KVErrorName = err.name;
  const topLevelMessage: string = err.message;
  void topLevelName;
  void topLevelMessage;

  const details = err.errors ?? [];
  for (const detail of details) {
    const key: string | undefined = detail.key;
    const name: string = detail.name;
    const message: string = detail.message;
    void key;
    void name;
    void message;
  }
}

consumeKVError(batchError);
