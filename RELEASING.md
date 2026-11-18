# Releasing terraform-provider-jetstream (Covered fork)

Target version for this line: **0.4.0-covered.1** (git tag `v0.4.0-covered.1`).

## Terraform version pinning

`0.4.0-covered.1` is a valid Terraform/OpenTofu semver **prerelease**. Prereleases are only selected by an **exact** version constraint — ranges like `>= 0.4.0` or `~> 0.4.0` will **not** match.

Pin consumers with:

```hcl
terraform {
  required_providers {
    jetstream = {
      source  = "covered/jetstream" # Terralist authority/name as configured
      version = "= 0.4.0-covered.1"
    }
  }
}
```

(`version = "0.4.0-covered.1"` without `=` is also exact.)

## Tag and publish GitHub release assets

Do **not** create the tag until you intend to release. The `release` workflow runs only on `v*` tag pushes.

1. Ensure repo secret `GPG_PRIVATE_KEY` is set (ASCII-armored private key). `PASSPHRASE` is optional; leave it unset when the key has no passphrase.
2. From the release commit:

   ```bash
   git tag v0.4.0-covered.1
   git push origin v0.4.0-covered.1
   ```

3. The workflow runs GoReleaser and publishes a **non-draft** GitHub release with zip archives per os/arch, `terraform-provider-jetstream_0.4.0-covered.1_SHA256SUMS`, `…_SHA256SUMS.sig`, and `…_manifest.json`.

Stable anonymous download URLs (public repo):

```text
https://github.com/CoveredInsurance/terraform-provider-jetstream/releases/download/v0.4.0-covered.1/terraform-provider-jetstream_0.4.0-covered.1_<os>_<arch>.zip
https://github.com/CoveredInsurance/terraform-provider-jetstream/releases/download/v0.4.0-covered.1/terraform-provider-jetstream_0.4.0-covered.1_SHA256SUMS
https://github.com/CoveredInsurance/terraform-provider-jetstream/releases/download/v0.4.0-covered.1/terraform-provider-jetstream_0.4.0-covered.1_SHA256SUMS.sig
https://github.com/CoveredInsurance/terraform-provider-jetstream/releases/download/v0.4.0-covered.1/terraform-provider-jetstream_0.4.0-covered.1_manifest.json
```

This provider speaks Terraform plugin protocol **5.0** (see `terraform-registry-manifest.json`).

## Terralist upload

After the GitHub release exists, register it with Terralist (adjust host, token, and fill each platform `shasum` from `SHA256SUMS`):

```bash
curl -L -X POST \
  -H "Authorization: Bearer x-api-key:${TERRALIST_API_KEY}" \
  -H "Content-Type: application/json" \
  -d @- \
  "${TERRALIST_URL}/v1/api/providers/covered/jetstream/0.4.0-covered.1/upload" <<'EOF'
{
  "protocols": ["5.0"],
  "shasums": {
    "url": "https://github.com/CoveredInsurance/terraform-provider-jetstream/releases/download/v0.4.0-covered.1/terraform-provider-jetstream_0.4.0-covered.1_SHA256SUMS",
    "signature_url": "https://github.com/CoveredInsurance/terraform-provider-jetstream/releases/download/v0.4.0-covered.1/terraform-provider-jetstream_0.4.0-covered.1_SHA256SUMS.sig"
  },
  "platforms": [
    {
      "os": "linux",
      "arch": "amd64",
      "download_url": "https://github.com/CoveredInsurance/terraform-provider-jetstream/releases/download/v0.4.0-covered.1/terraform-provider-jetstream_0.4.0-covered.1_linux_amd64.zip",
      "shasum": "<sha256-from-SHA256SUMS>"
    },
    {
      "os": "linux",
      "arch": "arm64",
      "download_url": "https://github.com/CoveredInsurance/terraform-provider-jetstream/releases/download/v0.4.0-covered.1/terraform-provider-jetstream_0.4.0-covered.1_linux_arm64.zip",
      "shasum": "<sha256-from-SHA256SUMS>"
    },
    {
      "os": "darwin",
      "arch": "amd64",
      "download_url": "https://github.com/CoveredInsurance/terraform-provider-jetstream/releases/download/v0.4.0-covered.1/terraform-provider-jetstream_0.4.0-covered.1_darwin_amd64.zip",
      "shasum": "<sha256-from-SHA256SUMS>"
    },
    {
      "os": "darwin",
      "arch": "arm64",
      "download_url": "https://github.com/CoveredInsurance/terraform-provider-jetstream/releases/download/v0.4.0-covered.1/terraform-provider-jetstream_0.4.0-covered.1_darwin_arm64.zip",
      "shasum": "<sha256-from-SHA256SUMS>"
    }
  ]
}
EOF
```

Add other os/arch rows from the release as needed. Because the GitHub repo is public, Terralist can fetch these URLs anonymously (no `headers` required).
