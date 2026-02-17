# dsync

`dsync` is a specialized S3-compatibility and synchronization tool designed to intercept Dgraph export commands and reliably sync the resulting RDF and schema files to an S3-compatible object store.

It operates as a transparent HTTP proxy, listening for export responses from the Dgraph admin API and then handling the end-to-end process of waiting for the filesystem export to complete before initiating a secure, encrypted upload to S3.

### Technical Design Choices

The architecture of `dsync` is built with specific S3-compatibility and resilience in mind:

#### 1. Proxy-Based Interception
Instead of requiring manual triggering, `dsync` functions as a `goproxy`. It monitors the Dgraph `/admin` endpoint. When an export command is issued, it captures the `task.id` from the response. This allows it to decouple the user's export command from the long-running synchronization process.

#### 2. Deterministic Object Mapping
Dgraph exports files with complex naming conventions based on timestamps and export types. `dsync` simplifies downstream consumption by mapping these files to predictable object names:
- Files containing `rdf` → `transformer.rdf.gz`
- Files containing `schema` → `transformer.schema.gz`

#### 3. Enforced SSE-C (Server-Side Encryption with Customer-Provided Keys)
Security is a core requirement. `dsync` requires a `crypto-key` at startup. Every object uploaded to S3 is encrypted using **SSE-C**. The tool automatically handles the generation of MD5 digests for the encryption key, ensuring that data is encrypted at rest using a key that remains entirely under the customer's control.

#### 4. Resilient Multipart Upload Handling
Certain S3-compatible implementations can exhibit race conditions or transient errors during the finalization of large multipart uploads. `dsync` includes a specialized error handler for the `"This multipart completion is already in progress"` error. Instead of failing the sync, it performs an out-of-band `StatObject` call to verify if the upload was actually successful, providing higher reliability for large database exports.

#### 5. Tuned S3 Transport
Handling large database exports requires specialized network configurations. `dsync` tunes the underlying HTTP transport for the MinIO client:
- **`ResponseHeaderTimeout` (15 Minutes)**: Accommodates large file processing and high-latency S3 operations.
- **`IdleConnTimeout` (90 Seconds)**: Optimizes connection reuse across the multiple files generated during a single export task.

#### 6. Task State Awareness
`dsync` doesn't just watch the filesystem; it understands the Dgraph task lifecycle. It implements a robust "busy-wait" polling mechanism that queries the Dgraph admin API. It only begins the sync process once the task status is explicitly confirmed as `Success`, preventing the upload of partial or corrupted export files.

### Configuration

`dsync` is configured via command-line flags:

| Flag | Description |
|------|-------------|
| `--endpoint` | S3-compatible endpoint (e.g., `s3.amazonaws.com` or a MinIO instance) |
| `--access-key` | S3 Access Key |
| `--secret` | S3 Secret Key |
| `--bucket-name` | Target bucket name |
| `--crypto-key` | Data encryption key for SSE-C (must be 32 bytes) |
| `--towatch` | Local directory where Dgraph exports files (default: `/dgraph/export`) |

### How It Works

1. **Proxy**: The user points their Dgraph client to `dsync` (defaulting to port `10080`).
2. **Intercept**: `dsync` detects an export command and starts a background synchronization worker.
3. **Wait**: The worker polls Dgraph until the export task is complete.
4. **Scan**: `dsync` scans the `--towatch` directory for the newly exported files.
5. **Encrypt & Upload**: Files are streamed to S3 using the configured SSE-C key.
6. **Cleanup**: Upon successful synchronization, `dsync` removes the local exported files to save disk space.
