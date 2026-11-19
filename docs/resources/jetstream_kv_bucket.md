# jetstream_kv_bucket Resource

The `jetstream_kv_bucket` Resource manages JetStream Based Key-Value buckets, supports editing Buckets in place.

## Example Usage

```hcl
resource "jetstream_kv_bucket" "CFG" {
  name = "CFG"
  history = 10
}
```

### Attribute Reference

* `name` - (required) The unique name of the KV bucket, must match `\A[a-zA-Z0-9_-]+\z`
* `description` - (optional) Contains additional information about this bucket
* `storage` - (optional) Storage backend to use, defaults to `file`, can be `file` or `memory`
* `history` - (optional) Number of historic values to keep
* `ttl` - (optional) How many seconds to keep values for, keeps forever when not set
* `placement_cluster` - (optional) Place the bucket in a specific cluster, influenced by placement_tags
* `placement_tags` - (optional) Place the bucket only on servers with these tags
* `max_value_size` - (optional) Maximum size of any value
* `max_bucket_size` - (optional) The maximum size of all data in the bucket
* `replicas` - (optional) How many replicas to keep on a JetStream cluster
* `limit_marker_ttl` - (optional) Enables per-key TTLs and limit markers; duration in seconds
* `allow_direct` - (optional, computed) Allow direct get on the backing stream (`$JS.API.DIRECT.GET`). When unset, Terraform preserves the live stream value (KV create defaults to `true`).
* `deny_delete` - (optional, computed) Restrict deleting messages from the backing stream via the API. When unset, Terraform preserves the live stream value (KV create defaults to `true`). Cannot be changed from `true` to `false` (the NATS server forbids it).
* `discard` - (optional, computed) Discard policy of the backing stream when limits are reached: `old` or `new`. When unset, Terraform preserves the live stream value (KV create defaults to `new`).

  Caveats:

  * Combining `discard = "old"` with `max_bucket_size` silently drops the oldest keys when the bucket is full.
  * Applications whose clients call create-bucket on startup (`nats.go` `CreateKeyValue` / `UpdateKeyValue` / `CreateOrUpdateKeyValue` hard-code `DiscardNew`) will reset the stream to `new`.
