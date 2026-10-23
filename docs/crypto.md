# Crypto

## Script API: `native.crypto`
Authenticated encryption without handling key material. Scripts name a key (`"invoices"`: 1-64
characters of letters, digits, `-`, `_`, `.`); the host derives it from the operator's master key
and your tenant name (HKDF-SHA256), so tenants cannot read each other's data even with the same
key name. Needs [`crypto`](#deployment-master-keys) in the deployment config.

| Function | Result |
| --- | --- |
| `encrypt(key, plaintext, aad?)` | binary string: XChaCha20-Poly1305 with a random nonce. 42 bytes larger than the input. |
| `decrypt(key, ciphertext, aad?)` | plaintext; raises if the key or `aad` differs or the data was modified. |
| `is_current(ciphertext)` | `true` if it was sealed with the operator's active master key. |
| `hmac_sha256(key, data, key_id?)` | 32-byte tag; `key_id` selects an older master key to verify old tags. |
| `public_key(key, key_id?)`, `seal(public_key, plaintext)`, `open(key, ciphertext)` | public-key encryption in the age format, see below. |
| `sha256(data)`, `random(n)` | digest; `n` random bytes (1-1024). Work without `crypto` configured. |
| `hex_encode/decode`, `base64_encode/decode` | conversions between binary strings and text. |

`aad` is optional context that must match on decrypt (for example the object key, which stops a
ciphertext being moved to another key). Inputs are capped at `max_object_mb`.

```lua
local b = native.data.Out
b.put("invoices/7.json", native.crypto.encrypt("invoices", body, "invoices/7.json"))
local plain = native.crypto.decrypt("invoices", b.get("invoices/7.json"), "invoices/7.json")
```

## Public-key variant (uploads from outside)
`public_key(key, key_id?)` returns an
[age](https://age-encryption.org) public key (`age1...`) whose private half exists only inside this
tenant's scripts, derived from the master key like the symmetric keys (but unrelated to the
symmetric key of the same name). Hand it to anyone; they encrypt with the stock tool and upload,
for instance through a [`presign_put`](data.md) URL, and a script reads the object and calls `open(key, blob)`.
`seal(public_key, plaintext)` does the same encryption from a script.

```sh
age -r age1... report.csv > report.csv.age     # no -a: armored input is not supported
```

```lua
local pk = native.crypto.public_key("inbox")                     -- publish this
local url = native.data.In.presign_put("drop/report.csv.age")    -- give this to the sender
local csv = native.crypto.open("inbox", native.data.In.get("drop/report.csv.age"))
```

`open` tries every configured master key, so uploads sealed to an older public key still open after
rotation; senders should switch to the new `public_key` (`key_id` returns an older one). The
format does not authenticate the sender: anyone with the public key can upload, so treat opened
data as untrusted input.

## Deployment: master keys {#deployment-master-keys}

`crypto.keys` maps a key number (1-255) to the **name of an environment variable** holding a
32-byte master key, as 64 hex characters or base64 (`openssl rand -hex 32`). The secret itself never
goes in the config file; inject it from Secrets Manager or similar. `crypto.active` is the number
used for new encryption. Startup fails if a listed variable is missing or malformed.

```json
{ "crypto": { "keys": { "1": "INTEGRAAL_KEY_1" }, "active": 1 } }
```

## Rotation
Ciphertext records the number of the master key that sealed it, so after the
operator adds key 2 and makes it active, old data still decrypts and new data uses key 2. To
migrate lazily, re-encrypt whatever `is_current` reports as `false`.

To rotate: generate key 2, deploy with both keys and `"active": 2`, let scripts re-encrypt data
that `native.crypto.is_current` reports as stale, then drop key 1. Data sealed with a key you remove
can no longer be decrypted, and losing every key loses the data: back them up separately from the data.

Changing the *key name* is a tenant-side rotation and needs the script to read with the old name
and write with the new one.
