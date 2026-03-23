# Ralph Fix Plan — QWP WebSocket Protocol for go-questdb-client

## Phase 1: Foundation — Wire Primitives

- [x] Create `qwp_constants.go`: all QWP type codes (0x01–0x16), magic bytes ("QWP1"), flags (FLAG_DELTA_SYMBOL_DICT=0x08, etc.), status codes (0x00–0x07), limits (MAX_BATCH_SIZE=16MB, MAX_COLUMNS=2048, MAX_ROWS=1M), header size (12), schema modes (FULL=0x00, REF=0x01). Include getFixedTypeSize() and isFixedWidthType() helper functions. See spec 01-qwp-protocol.md for all values.
- [x] Create `qwp_varint.go`: unsigned LEB128 varint encode (putVarint into []byte, returns bytes written), decode (readVarint from []byte, returns value + bytes consumed), varintSize(uint64) int. Also: putString (varint length + UTF-8 bytes), readString. Write `qwp_varint_test.go` with golden byte tests for values 0, 127, 128, 300, 16383, 16384, max uint32, max int64. Test round-trip and size function.
- [x] Create `qwp_wire.go`: a `qwpWireBuffer` type wrapping `[]byte` with methods: putByte, putUint16LE, putUint32LE, putInt32LE, putUint64LE, putInt64LE, putFloat32LE, putFloat64LE, putInt64BE (for decimals), putVarint, putString, putBytes, reset (len=0, keep cap), len, bytes, ensure(n int) for pre-growth. Also patchUint32LE(offset, val) for patching payload length. Write tests in `qwp_wire_test.go` verifying byte-level output for each method.

## Phase 2: Columnar Table Buffer

- [x] Create `qwp_buffer.go` with `qwpColumnBuffer` struct: stores column name, typeCode, nullable flag, scale (for decimals), and type-specific data slices (fixedData []byte with fixedSize stride, boolData []byte for bit-packing, strOffsets []uint32 + strData []byte for strings, symbolIDs []int for symbols, nullBitmap []byte, nullCount). Implement methods: addLong(int64), addDouble(float64), addBool(bool), addString(string), addSymbolID(int), addNull(), addTimestamp(int64), addByte(int8), addShort(int16), addInt32(int32), addFloat32(float32), addChar(rune), addUuid(hi,lo uint64), addLong256(bytes), reset(). Write tests verifying backing bytes for each type.
- [x] Add `qwpTableBuffer` struct to `qwp_buffer.go`: tableName, columns []qwpColumnBuffer, columnIndex map[string]int, rowCount, schemaHash/schemaValid. Methods: getOrCreateColumn(name, typeCode, nullable) → *qwpColumnBuffer (with type conflict detection), commitRow() (gap-fills missing columns with null/sentinel), cancelRow() (rollback to checkpoint), reset(), getSchemaHash(). Write tests for multi-column tables, gap-filling, type conflicts, row cancel.
- [x] Add array column support to `qwpColumnBuffer`: arrayOffsets []uint32 + arrayData []byte (per-row: nDims + shape + flattened LE elements). Methods: addDoubleArray(nDims, shape, flatData), addLongArray(nDims, shape, flatData). Updated addNull (nDims=1, dim0=0 sentinel), reset, truncateTo. Write tests.
- [x] Add decimal column support to `qwpColumnBuffer`: addDecimal(Decimal) method with big-endian sign-extended unscaled values in fixedData. Scale tracking (first non-null value locks scale, subsequent values validated). Overflow detection for values too large for wire size. Handles null Decimal via addNull delegation. Write tests.

## Phase 3: QWP Message Encoder

- [x] Create `qwp_encoder.go` with `qwpEncoder` struct: owns a `qwpWireBuffer` scratch. Method `encodeTable(tb *qwpTableBuffer, schemaMode, schemaHash)` writes header (12B) → table name → rowCount → colCount → schema (full/reference) → column data → patches payload length. Supports all fixed-width types, decimal schema (extra scale byte), null bitmap, boolean bit-packing, string offsets+data, symbol varints, array raw bytes. 9 golden byte tests: fixed-width types, header, schema ref, nullable, multi-column, empty table, reuse, decimal schema.
- [x] Add boolean column encoding to the encoder: bit-packed output. Null bitmap encoding for nullable columns. Golden byte tests: 3 bools → 0x05, nullable bool with null → bitmap 0x02 + data 0x01.
- [x] Add string/varchar column encoding: (rowCount+1) uint32 LE offsets + string data. Symbol column encoding: varint global IDs per row. Golden byte tests for strings ("hello","world") offsets [0,5,10], symbols [0,1,42], varchar "abc".
- [x] Add array column encoding: raw arrayData (per-row nDims+shape+elements). Decimal column encoding: big-endian unscaled values with scale byte in schema. Golden byte tests for 1D double array, decimal schema+data.
- [x] Add delta symbol dictionary encoding to the encoder: encodeTableWithDeltaDict(tb, globalDict, maxSentId, batchMaxId, schemaMode, schemaHash) prepends delta dict (deltaStart+deltaCount+symbol strings) before table block, sets FLAG_DELTA_SYMBOL_DICT in header. Refactored encoder to use writeHeader/writeTableBlock/patchPayloadLength helpers. 4 tests: full golden bytes, empty delta, all-new symbols, delta+schema ref.
- [x] Add schema hash computation: XXHash64 over column names + wire type codes in qwp_hash.go. Matches Java QwpTableBuffer.getSchemaHash(). Tests: deterministic, column change, nullable affects hash, known value, XXHash64 empty input canonical vector.
- [x] Add geohash column encoding: addGeohash(value, precision) in buffer with precision tracking, truncateTo support. Encoder writes precision varint + per-row ceil(precision/8) bytes LE. 4 buffer tests + 4 encoder golden byte tests (precision 20/12/8/60, nullable, null sentinel).

## Phase 4: WebSocket Transport

- [x] Add `github.com/coder/websocket` dependency and create `qwp_transport.go` with qwpTransport struct wrapping *websocket.Conn. Methods: connect(ctx,url,opts), sendMessage(ctx,data), readAck(ctx)→(status,payload,err), close(ctx). ACK format: status(1B)+sequence(8B LE)+optional error(uint16 len+UTF-8). TLS insecure skip verify via custom http.Client. Auth header support. Gorilla encoding byte (0x00) prepended to timestamp columns. 11 tests: ACK parsing (6 cases), sequence parsing, unconnected, mock WS connect/close, mock send/receive, mock error ACK, integration test vs real QuestDB.
- [x] Add ACK response parsing and QwpError type: qwp_errors.go with QwpError(Status, Sequence, Message), wire status code constants matching actual server (0=OK, 1=PARSE_ERROR, 2=SCHEMA_ERROR, 3=WRITE_ERROR, 4=SECURITY_ERROR, 255=INTERNAL_ERROR), newQwpErrorFromAck(), IsRetriable(), IsSchemaError(). 11 tests.
- [x] TLS and auth support: already implemented in qwp_transport.go via qwpTransportOpts (tlsInsecureSkipVerify, authorization). TLS uses custom http.Client with InsecureSkipVerify. Auth header passed in WebSocket upgrade request.
- [x] Add retry logic: sendWithRetry() with exponential backoff (10ms initial, doubling up to 1s max, 0-10ms jitter), cumulative retryTimeout. Retries on INTERNAL_ERROR (255). Handles SCHEMA_ERROR via callback for re-encoding with full schema. Returns *RetryTimeoutError on timeout. Context cancellation supported. 6 tests: success, non-retriable, recovery after 2 failures, timeout, no-retry mode, schema error handling.

## Phase 5: QWP Sender — Synchronous Mode

- [x] Create `qwp_sender.go` implementing full LineSender interface. Table/Symbol/all column methods with validation and method chaining. At/AtNow commit rows with auto-flush support (row count + time interval). Flush encodes per-table with delta symbol dict and schema caching. Close auto-flushes + graceful WS close. Global symbol dictionary (map[string]int32 + []string). Schema hash caching with sentSchemaHashes set. Critical encoder fix: null bitmap flag byte (0x00/0x01+bitmap) before every column's data, no 0x80 nullable flag in schema type codes — matches actual QuestDB server wire format. 17 sender tests + integration test passing against real QuestDB.
- [x] Add auto-flush support: row count threshold (autoFlushRows) and time interval (autoFlushInterval). Triggered from At()/AtNow(). Match existing HTTP sender auto-flush behavior. Write test verifying auto-flush triggers.
- [x] Add DecimalColumn, DecimalColumnFromString, DecimalColumnShopspring methods to the sender. Reuse existing Decimal type. Write test.
- [x] Add Float64Array1DColumn, Float64Array2DColumn, Float64Array3DColumn, Float64ArrayNDColumn methods. Reuse existing NdArray type. Write test.

## Phase 6: Extended QWP Column Types

- [x] Create `QwpSender` interface in `qwp_sender.go` that embeds LineSender and adds QWP-specific methods. Make `qwpLineSender` implement QwpSender. Add ByteColumn(int8), ShortColumn(int16), Int32Column(int32), Float32Column(float32) — each writes to a fixed-width column buffer with appropriate type code and stride. Write tests.
- [x] Add CharColumn(rune) — stores as uint16 LE (UTF-16 code unit). Add DateColumn(time.Time) — stores as int64 milliseconds since epoch. Add TimestampNanosColumn(time.Time) — stores as int64 nanoseconds since epoch. Write tests including edge cases (zero time, max time, non-BMP rune for CharColumn).
- [x] Add UuidColumn(hi, lo uint64) — stores as 16 bytes (lo LE then hi LE). Add VarcharColumn(string) — same encoding as StringColumn but with TYPE_VARCHAR type code. Write tests.
- [x] Add GeohashColumn(hash uint64, precision int) — stores precision once per column, packed bits per row. Write tests.
- [x] Add Int64Array1DColumn, Int64Array2DColumn, Int64Array3DColumn for LONG_ARRAY type. Write tests.

## Phase 7: Async Mode

- [x] Create `qwp_sender_async.go` with `qwpAsyncState` struct: sendCh (buffered chan), inFlightCount/Max with mutex+cond, nextSequence/ackedSequence, ioErr, done/stopCh channels, WaitGroup. Implement acquireSlot() (blocks when window full), releaseSlot(), setError(). Write unit tests for flow control (acquireSlot blocks at max, unblocks on release).
- [x] Implement I/O goroutine `ioLoop()`: reads from sendCh, sends over WebSocket, reads ACK, processes response, releases slot. On error: sets ioErr, signals cond, returns. Write test with mock connection verifying batches are sent and ACKs processed.
- [x] Integrate async mode into qwpLineSender: if inFlightWindow > 1, create qwpAsyncState and start ioLoop goroutine on connect. Flush encodes pending rows, copies to batch, enqueues via sendCh (with acquireSlot). Flush waits for all in-flight to complete. Close: flush, close sendCh, wait for goroutine, close connection. Write test with in-flight window = 2.
- [x] Add race condition tests for async mode: run with `go test -race`. Test concurrent auto-flush triggering, flush during active row building, close during in-flight batches.

## Phase 8: API Integration & Config

- [x] Extend `conf_parse.go` to handle `ws` and `wss` schemas: map to qwpSenderType, set TLS mode for wss. Parse `in_flight_window` config key. Add validation in `sanitizeQwpConf()`. Write config string parsing tests: "ws::addr=localhost:9000;", "wss::addr=localhost:9000;tls_verify=unsafe_off;in_flight_window=4;".
- [x] Extend `sender.go`: add `qwpSenderType` constant, add `WithQwp()` and `WithInFlightWindow()` option functions, add qwp case to `newLineSender()` and `newLineSenderConfig()`. Update `validateConf()` for inFlightWindow. Write tests for option functions and factory.
- [x] Ensure existing tests still pass: run `go test ./...` and fix any breakage caused by interface changes or new sender type integration.

## Phase 9: Integration Tests

- [x] Write `qwp_integration_test.go` with basic end-to-end test: create QWP sender via config string "ws::addr=localhost:9000;", insert rows with all basic types (symbol, long, double, string, bool, timestamp), flush, query via HTTP /exec, verify data. Include helper functions: queryQuestDB(), dropTable(), skipIfNoServer().
- [x] Add integration tests for extended column types: byte, short, int32, float32, char, date, uuid, varchar, timestamp_nanos, double_array, long_array, decimal. Each test: insert → flush → query → verify.
- [x] Add integration tests for multi-table batches, large batches (10k rows), auto-flush, symbol deduplication across flushes.
- [x] Add integration test for async mode: create sender with in_flight_window=4, insert 50k rows, flush, verify all data arrived. Test with race detector.
- [x] Add integration test for config string creation: LineSenderFromConf with ws:// schema.

## Phase 10: Polish & Hardening

- [x] Review all error paths: connection errors, encoding errors, server errors, context cancellation. Ensure no goroutine leaks in async mode (add test). Ensure Close() is idempotent and safe after errors.
- [x] Add benchmarks: BenchmarkQwpEncode, BenchmarkQwpFlush, BenchmarkQwpVarint, BenchmarkQwpSymbolLookup. Verify zero allocations per row on steady state with -benchmem.
- [x] Run full test suite with race detector: `go test -race ./...`. Fix any data races.
- [x] Final review: run `go vet ./...`, check for any TODOs or incomplete implementations. Verify all specs are implemented. Ensure all public types and functions have doc comments.

## Phase 11: Architecture Review Fixes (CRITICAL)

These are bugs and design issues found during architectural review. They range from correctness bugs (will cause data corruption or protocol errors) to design issues (defeat the purpose of async mode). Fix them in priority order.

- [x] **BUG: FLAG_GORILLA set but no Gorilla encoding implemented.** In `qwp_encoder.go`, both `encodeTable()` and `encodeTableWithDeltaDict()` always set `qwpFlagGorilla` (0x04) in the header flags. But the encoder writes timestamps as raw 8-byte LE values — there is NO Gorilla delta-of-delta compression. If the server interprets this flag, it will try to decode Gorilla-compressed data and get garbage. **Fix:** Remove `qwpFlagGorilla` from both `writeHeader()` calls. Only set it when actual Gorilla encoding is implemented (which is out of scope for now). Write a test that verifies the header flags byte does NOT contain 0x04.

- [x] **BUG: Schema hash keying doesn't include table name.** In `qwp_sender.go`, `sentSchemaHashes` stores the raw `schemaHash` from `tb.getSchemaHash()`. But two different tables can have identical column schemas (same names, same types). The Java client computes `schemaKey = schemaHash ^ (int64(hashString(tableName)) << 32)` to distinguish them. Without this, if table A's schema is sent and cached, then table B with the same columns will incorrectly use schema reference mode — the server will reject it because the schema hash was registered for table A, not table B. **Fix:** Compute a combined `schemaKey` that XORs the schema hash with a hash of the table name, and use that as the key in `sentSchemaHashes`. Apply this in both `flushTable()` and `flushAsync()`. Write a test with two tables having identical columns, verifying both get full schemas on first flush.

- [x] **BUG: flushAsync optimistically marks schema/symbols as sent before ACK.** In `qwp_sender.go:735-739`, `flushAsync()` adds the schema hash to `sentSchemaHashes` and bumps `maxSentSymbolId` immediately after enqueuing the batch, BEFORE the I/O goroutine has sent it and received the ACK. If the batch fails (network error, server rejection), these are incorrectly cached: (a) subsequent flushes will use schema reference mode for a schema the server never received, causing SCHEMA_REQUIRED errors; (b) the delta symbol dictionary will skip symbols the server never received, causing data corruption. **Fix:** Move schema hash and symbol ID updates to after successful ACK. In sync mode this is already correct (updates happen after `flushTable` returns). For async mode, the batch struct sent through the channel should carry the schema hash and batchMaxSymbolId. The I/O goroutine should signal back (or the sender should update after `waitEmpty()` confirms all batches succeeded). The simplest correct approach: do NOT update `sentSchemaHashes` or `maxSentSymbolId` in `flushAsync` before `waitEmpty()`. Move the updates to after `waitEmpty()` returns nil. Since `flushAsync` already blocks until all batches are ACKed, this is safe and correct. Write a test that verifies schema/symbols are not cached if flush fails.

- [x] **DESIGN: flushAsync blocks on every flush, negating async benefit.** `flushAsync()` calls `waitEmpty()` at the end, blocking until ALL in-flight batches are ACKed. This means auto-flush (triggered from `At()`) blocks the user goroutine on every threshold crossing — no different from sync mode. The Java client only blocks when the window is full (`acquireSlot`) or on explicit `flush()`/`close()`. **Fix:** Split the flush path into two: (1) `enqueueFlush()` — encode and enqueue without waiting (used by auto-flush from `At()`), and (2) `Flush()` — encode, enqueue, AND wait for all in-flight to drain (used for explicit flush and close). The `At()` method should call `enqueueFlush()` instead of `Flush()` when async mode is active and the auto-flush threshold is hit. `Flush()` keeps its current blocking behavior. This requires careful handling of the schema/symbol updates (they should only be committed after ACK, per the previous fix). Write a test: with window=4 and autoFlushRows=10, insert 100 rows — verify that multiple batches are in-flight concurrently (not serialized).

- [x] **BUG: ioLoop uses context.Background() — no cancellation support.** In `qwp_sender_async.go:179,189`, the I/O goroutine calls `sendMessage(context.Background(), ...)` and `readAck(context.Background(), ...)`. If the server becomes unresponsive, these calls block forever — the goroutine can never be stopped, and `Close()` hangs. **Fix:** Add a cancellable context to `qwpAsyncState` (created in `newQwpAsyncState`, cancelled in `stop()`). Use this context for all WebSocket operations in `ioLoop`. When `stop()` is called, cancel the context first, then close the channel, then wait. Write a test that verifies `Close()` completes within a timeout even when the server is unresponsive (use a mock server that accepts connections but never responds).

- [x] **BUG: Auth header format is wrong for QWP.** In `sender.go:newQwpLineSenderFromConf()`, the code sets `opts.authorization = conf.tcpKey` — it sends the raw token string as the Authorization header value. But HTTP Authorization headers require a scheme prefix: `Bearer <token>` or `Basic <base64(user:pass)>`. The transport sets this as `Authorization: <raw_value>`. Compare with the HTTP sender which properly formats basic auth with base64 encoding and bearer with the "Bearer " prefix. **Fix:** Format the authorization value properly: if both tcpKeyId and tcpKey are set, decide on the auth scheme (Bearer is most likely for QWP). Or follow the Java client's pattern — read `QwpWebSocketSender.java` to see how auth headers are constructed. Also consider supporting basic auth (httpUser/httpPass) for QWP since the WebSocket upgrade is HTTP. Write a test that verifies the Authorization header format.

- [x] **DESIGN: One message per table per flush — missing multi-table batching.** `flushSync()` iterates `tableBuffers` and sends one QWP message per table, each requiring a full round-trip (send + wait for ACK). With 10 tables, that's 10 round-trips. The QWP header has a `tableCount` field specifically to support multiple tables in one message. The Java client batches all tables into a single message. **Fix:** Extend the encoder to support multi-table messages: `encodeMultiTable(tables []*qwpTableBuffer, ...)` that writes one header with `tableCount=N` followed by N table blocks. Update `flushSync` to encode all tables in a single message and send it once. Write a test with 3 tables verifying the encoded message has `tableCount=3` and all table data is present.

- [x] **MINOR: No maxBufSize enforcement on columnar buffers.** `sanitizeQwpConf()` rejects `maxBufSize != 0` as an error, but there's no size tracking at all. A user can accumulate unbounded data in columnar buffers before flushing, potentially exhausting memory. **Fix:** Track approximate buffer size in `qwpTableBuffer` (increment on each add, decrement on reset). Check against a configurable limit in `At()` and return an error if exceeded. Accept `maxBufSize` in config instead of rejecting it. Write a test that verifies the error when buffer limit is exceeded.

- [x] **MINOR: Long256 wire format — verify endianness.** The spec (01-qwp-protocol.md) says Long256 is "32 bytes, four int64s, little-endian" but the original Java `QwpConstants.java` says "32 bytes big-endian". Read the Java client's `QwpColumnWriter.java` to see how Long256 is actually encoded on the wire and compare with the Go implementation in `qwp_buffer.go:addLong256()`. If there's a mismatch, fix the Go encoding. Write a golden byte test with a known Long256 value verified against the Java output.

## Phase 12: Zero-Allocation Performance Fixes

The goal is ZERO allocations per flush at steady state in sync mode. Current benchmarks show 2 allocs/op in `BenchmarkQwpFlush`. All fixes below must be verified with `-benchmem`. After all fixes, `BenchmarkQwpFlush` and a new full-sender benchmark must report `0 allocs/op`.

- [x] **Eliminate `qwpComputeSchemaHash` allocation.** In `qwp_hash.go:56-64`, `qwpComputeSchemaHash` declares `var buf []byte` and grows it via `append` on every call — this allocates every time `getSchemaHash()` is called after a `reset()` (which invalidates the cached hash). **Fix:** Add a `hashBuf []byte` field to `qwpTableBuffer`. In `qwpComputeSchemaHash` (or a new method on `qwpTableBuffer`), reuse this buffer: `tb.hashBuf = tb.hashBuf[:0]` then append into it. The capacity survives `reset()` since `reset()` should NOT clear `hashBuf`. Verify with `go test -bench BenchmarkQwpFlush -benchmem` — this should eliminate 1 of the 2 allocs.

- [x] **Eliminate `qwpSchemaKey` string-to-[]byte allocation.** In `qwp_hash.go:48`, `qwpSchemaKey` converts `tableName` to `[]byte` via `xxhash64([]byte(tableName), 0)` — this allocates on every call. Since the table name never changes, **fix** by caching the table name hash in `qwpTableBuffer` as a field `tableNameHash uint64`, computed once in `newQwpTableBuffer`. Then `qwpSchemaKey` takes the precomputed hash instead of the string. Update all callers: `flushSync`, `flushAsync`, `enqueueFlush`. Verify the alloc is gone with `-benchmem`.

- [x] **Eliminate `buildTableEncodeInfo` slice allocation.** In `qwp_sender.go:748-767`, `buildTableEncodeInfo()` declares `var tables []qwpTableEncodeInfo` and appends to it on every flush — allocating a new slice each time. **Fix:** Add a reusable `encodeInfoBuf []qwpTableEncodeInfo` field to `qwpLineSender`. In `buildTableEncodeInfo`, do `s.encodeInfoBuf = s.encodeInfoBuf[:0]` and append into it, returning the slice. Capacity survives across flushes. Verify with `-benchmem`.

- [x] **Eliminate async batch copy with double-buffered encoders.** In `qwp_sender.go:791` and `:866`, `flushAsync` and `enqueueFlush` allocate `make([]byte, len(encoded))` per batch to copy the encoded data before enqueuing (because the single encoder reuses its buffer). The Java client solves this with double-buffering: two pre-allocated `MicrobatchBuffer`s that swap roles. **Fix:** Give `qwpLineSender` TWO `qwpEncoder` instances (`encoder0`, `encoder1`) and a `currentEncoder` index. On each flush, encode using the current encoder, send its `.bytes()` slice directly through the channel (no copy needed — the slice is valid until that encoder is reused). Swap to the other encoder for the next flush. The I/O goroutine signals when a batch is sent (ACKed), allowing the encoder to be reused. With `inFlightWindow=N`, you need N+1 encoders (or just 2 if you block when both are in use, which is fine since the window already provides backpressure via `acquireSlot`). The simplest correct approach: 2 encoders, `acquireSlot` blocks when window is full (which means the first encoder's batch has been sent), so by the time we need to reuse encoder0, its data has already been transmitted. Remove the `make([]byte, ...)` copy. Verify 0 allocs in `BenchmarkQwpSenderAsyncSteadyState`.

- [x] **Replace `approxDataSize()` iteration with running counter.** In `qwp_buffer.go:775-787`, `approxDataSize()` iterates ALL columns summing their lengths — this is O(columns) per call and is called from `At()` on every row when `maxBufSize > 0`. **Fix:** Add a `dataSize int` field to `qwpTableBuffer`. Increment it in each column's `addX` method (by the number of bytes added). Decrement (reset to 0) in `reset()`. In `truncateTo` (for cancelRow), recompute from scratch (this is the rare path). Then `approxDataSize()` just returns `tb.dataSize`. This makes the hot path O(1) instead of O(columns). Write a test that the running counter matches the iterative computation.

- [x] **Add full-sender steady-state benchmark.** The existing `BenchmarkQwpFlush` tests raw buffer+encoder operations, not the sender layer. Add `BenchmarkQwpSenderSteadyState` that: (1) creates a `qwpLineSender` with a mock transport (or nil transport + skip send), (2) warms up with 100 rows across 2 flushes, (3) benchmarks the loop: `Table("t").Symbol("sym","AAPL").Int64Column("qty",100).Float64Column("price",150.5).StringColumn("note","test").At(ctx,ts)` for 10 rows, then calls a mock flush that exercises encoding but skips network. Verify `0 allocs/op` and `0 B/op` at steady state. This is the definitive proof that the hot path is allocation-free.

- [x] **Clean up dead code: async branch in `flush0`.** In `qwp_sender.go`, `flush0` (called from `Close`) has `if s.asyncState != nil { return s.flushAsync(ctx) }` — but after the Phase 11 restructuring, the async `Close()` path uses `enqueueFlush` + `stop()` directly, never going through `flush0`. This async branch is dead code. Remove it and add a comment explaining the async close path goes through `enqueueFlush`. This is a minor cleanup, not a performance fix.

## Phase 13: Critical Wire Format Correctness Fixes

Two critical encoding bugs found by comparing the Go encoder byte-for-byte against the Java server decoder. These bugs mean **ALL data with nullable columns is silently corrupted** — the server will misparse every column after the first nullable one.

### Issue A: Null rows must be EXCLUDED from column data

The Go encoder writes data for ALL `rowCount` rows, including null rows (with sentinel values like MinInt64, NaN, empty strings). **The Java server expects data for only `valueCount = rowCount - nullCount` rows.** Null rows are identified solely by the null bitmap — no data is written for them.

This means:
- Fixed-width: server reads `valueCount * fixedSize` bytes, Go writes `rowCount * fixedSize` → extra bytes shift all subsequent columns
- String: server reads `(valueCount + 1)` offsets, Go writes `(rowCount + 1)` → misalignment
- Symbol: server reads `valueCount` varints, Go writes `rowCount` varints → misalignment
- Boolean: server reads `ceil(valueCount/8)` bytes, Go writes `ceil(rowCount/8)` → misalignment
- Array: server skips null rows, Go writes null sentinel arrays → extra bytes

### Issue B: Decimal scale byte is in the wrong section

The Go encoder writes the scale byte in the **schema** section (after type code). The Java server reads it from the **column data** section (after null bitmap, before values). This corrupts both the schema parse (shifts all subsequent column defs by 1 byte per decimal column) AND the data parse (missing scale byte before values).

### Tasks

- [x] **CRITICAL: Fix null-value packing — only emit non-null rows in column data.** This is the biggest change. The encoder must skip null rows when writing data. Read the Java server decoders to understand the exact expected format for each type:
  - `QwpFixedWidthColumnCursor.of()` in `/home/jara/devel/oss/questdb-http2/core/src/main/java/io/questdb/cutlass/qwp/protocol/QwpFixedWidthColumnCursor.java` — reads `valueCount = rowCount - nullCount` values, NOT `rowCount`
  - `QwpStringColumnCursor.of()` — reads `(valueCount + 1)` offsets, NOT `(rowCount + 1)`
  - `QwpBooleanColumnCursor.of()` — reads `ceil(valueCount/8)` bits, NOT `ceil(rowCount/8)`
  - `QwpSymbolColumnCursor.of()` — reads `valueCount` varints, NOT `rowCount`
  - `QwpArrayColumnCursor.of()` — skips null rows, only reads data for non-null rows
  **Implementation approach:** There are two strategies. Either (a) stop writing sentinel data in `addNull()` (only set bitmap bit + increment rowCount), and encode the data arrays as-is (which now contain only non-null values). Or (b) keep writing sentinels in `addNull()` but filter them out in the encoder using the null bitmap. Approach (a) is cleaner — the buffer already has `rowCount` and `nullCount`, and the data arrays would naturally contain only `valueCount = rowCount - nullCount` entries. The `addNull()` method should ONLY mark the null bitmap and increment rowCount, without appending sentinel data. The `commitRow()` gap-fill must also only call the bitmap-marking version of addNull. The encoder then writes the data arrays directly. BUT: `truncateTo()` for row cancel needs adjustment since data arrays are now shorter than rowCount. Track `valueCount` per column explicitly. Write golden byte tests that encode a column with nulls and verify the output matches what the Java server expects: null bitmap + only non-null values packed together.

- [x] **CRITICAL: Move decimal scale byte from schema to column data section.** In `qwp_encoder.go`, `encodeSchemaFull()` currently writes a scale byte after the type code for decimal columns. Remove that. Instead, in `encodeColumnData()`, for decimal types, write the scale byte AFTER the null bitmap flag/bitmap and BEFORE the packed values. Read `QwpDecimalColumnCursor.of()` in the Java server to verify the exact position. Also read `QwpSchema.parseFullSchema()` to confirm it does NOT expect a scale byte in the schema — it just reads (varint name + 1 byte type) per column. Write a golden byte test for a table with a decimal column that has some null and some non-null values.

- [x] **Update ALL golden byte tests for the new null-packing format.** After fixing the encoder, every existing test that uses nullable columns or addNull() will produce different bytes. Update the expected values. Also add NEW tests specifically for the null-packing behavior: (a) a non-nullable column (0x00 flag, all rows' data), (b) a nullable column with 0 nulls (0x00 flag, all rows' data), (c) a nullable column with some nulls (0x01 flag + bitmap + only non-null values), (d) a nullable column that is ALL nulls (0x01 flag + bitmap + 0 data bytes). Test each category for: fixed-width (LONG), STRING, BOOLEAN, SYMBOL, ARRAY.

- [x] **Verify with integration test against running QuestDB.** After fixing encoding, write an integration test that inserts rows with nullable columns (some null, some not) via QWP, queries via HTTP /exec, and verifies the data is correct. This must pass against the real server to confirm wire compatibility. Include: nullable LONG with some nulls, nullable STRING with some nulls, nullable BOOLEAN with some nulls, nullable DOUBLE with some nulls. Skip this test if server is unavailable (`skipIfNoServer`).

## Phase 14: Concurrency Polish

Minor concurrency improvements found during review. None of these are bugs — the current code is correct — but they improve consistency, intent clarity, and robustness.

- [x] **Eliminate `pendingSchemaKeys` allocation in `flushAsync`.** In `qwp_sender.go`, `flushAsync()` declares `var pendingSchemaKeys []int64` and appends to it on every explicit `Flush()` call — allocating a new slice each time. This was introduced in Phase 11 to defer schema caching until after ACK. It violates the zero-allocation goal from Phase 12 (the `BenchmarkQwpSenderSteadyState` benchmark doesn't cover the explicit Flush path with async mode). **Fix:** Add a reusable `pendingSchemaKeysBuf []int64` field to `qwpLineSender`, same pattern as `encodeInfoBuf`. Reset length but keep capacity: `s.pendingSchemaKeysBuf = s.pendingSchemaKeysBuf[:0]`. Verify with a new benchmark or extend `BenchmarkQwpSenderSteadyState` to cover async explicit-Flush and confirm `0 allocs/op`.

- [x] **Use `cond.Signal()` instead of `cond.Broadcast()` in `releaseSlot()`.** In `qwp_sender_async.go:138`, `releaseSlot()` uses `cond.Broadcast()` which wakes ALL waiters. Only one waiter can proceed (one slot opened → one `acquireSlot` caller), and `qwpLineSender` is single-user-goroutine so there's at most one waiter. `Signal()` is semantically more precise and marginally cheaper. Keep `Broadcast()` in `setError()` and `markStopped()` where multiple different waiters (`acquireSlot` + `waitEmpty`) could be blocked simultaneously.

- [x] **Make async stop grace period configurable.** In `qwp_sender_async.go:262`, `qwpAsyncStopGracePeriod` is a fixed `2 * time.Second`. If a large batch is being sent over a slow network, 2 seconds may not be enough — the context gets cancelled, in-flight data is lost, and the user gets a cancellation error. The Java client does not have this limitation (it waits for the I/O thread indefinitely). **Fix:** Add a `closeTimeout` field to `qwpLineSender` (defaulting to 5 seconds), pass it through to `stop()`. Expose via config string key `close_timeout` and option function `WithCloseTimeout(d time.Duration)`. Document that calling `Flush()` before `Close()` guarantees all data is ACKed, while `Close()` alone uses best-effort with the timeout.

## Completed
- [x] Project initialization and exploration

## Notes
- QuestDB server with QWP support is running at localhost:9000 (no auth)
- WebSocket endpoint: ws://localhost:9000/write/v4
- Reference Java client: /home/jara/devel/oss/questdb-http2/java-questdb-client/
- Reference Rust client: /home/jara/devel/oss/c-questdb-client/questdb-rs/src/ingress/buffer/qwp.rs
- When in doubt about wire format, check the Java server decoder at /home/jara/devel/oss/questdb-http2/core/src/main/java/io/questdb/cutlass/qwp/
- The encoder golden byte tests are the most important tests — they catch encoding bugs early
- For schema hash computation, read QwpTableBuffer.getSchemaHash() in the Java client
