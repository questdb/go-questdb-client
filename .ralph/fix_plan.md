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
- [ ] Add geohash column encoding to the encoder. Write golden byte test.

## Phase 4: WebSocket Transport

- [ ] Add `github.com/coder/websocket` dependency: `go get github.com/coder/websocket`. Create `qwp_transport.go` with a `qwpTransport` struct wrapping `*websocket.Conn`. Methods: connect(ctx, url, opts) error, sendMessage(ctx, data []byte) error, readAck(ctx) (statusCode byte, payload []byte, err error), close(ctx) error. Write a basic test that connects to ws://localhost:9000/write/v4 and verifies the connection succeeds (integration test, skip if server unavailable).
- [ ] Add ACK response parsing to `qwp_transport.go`: parse status codes, handle PARTIAL error payload (extract table errors). Create `qwp_errors.go` with QwpError type (statusCode, message, tableErrors). Write unit tests for parsing each status code from byte slices.
- [ ] Add TLS support: wss:// connections with configurable certificate verification. Add auth header support (Basic and Bearer). Write test for auth header construction.
- [ ] Add retry logic for STATUS_OVERLOADED (0x07): exponential backoff (10ms, 20ms, 40ms... up to 1s), cumulative timeout from retryTimeout config. Match the existing HTTP sender's retry pattern in http_sender.go.

## Phase 5: QWP Sender — Synchronous Mode

- [ ] Create `qwp_sender.go` with `qwpLineSender` struct implementing LineSender. Implement Table(name), Symbol(name,val), and the core column methods: Int64Column, Float64Column, StringColumn, BoolColumn, TimestampColumn, Long256Column. Each method: validates name, looks up/creates column in current table buffer, appends value. Implement At(ctx,ts) and AtNow(ctx): commits row, checks auto-flush. Implement Flush(ctx): encodes each table buffer, sends over WebSocket, reads ACK, resets buffers. Implement Close(ctx): flush + close connection. Write unit test using a mock WebSocket server (or test encoding only without network).
- [ ] Add global symbol dictionary to the sender: map[string]int for ID lookup, []string for reverse lookup. Symbol() assigns global IDs. Flush includes delta dictionary. Track maxSentSymbolId, update after successful ACK. Write test verifying delta dictionary contents across multiple flushes.
- [ ] Add schema caching: track sentSchemaHashes set. First flush sends full schema, second flush uses reference mode. Handle STATUS_SCHEMA_REQUIRED by clearing hash and retrying with full schema. Write test.
- [ ] Add auto-flush support: row count threshold (autoFlushRows) and time interval (autoFlushInterval). Triggered from At()/AtNow(). Match existing HTTP sender auto-flush behavior. Write test verifying auto-flush triggers.
- [ ] Add DecimalColumn, DecimalColumnFromString, DecimalColumnShopspring methods to the sender. Reuse existing Decimal type. Write test.
- [ ] Add Float64Array1DColumn, Float64Array2DColumn, Float64Array3DColumn, Float64ArrayNDColumn methods. Reuse existing NdArray type. Write test.

## Phase 6: Extended QWP Column Types

- [ ] Create `QwpSender` interface in `qwp_sender.go` that embeds LineSender and adds QWP-specific methods. Make `qwpLineSender` implement QwpSender. Add ByteColumn(int8), ShortColumn(int16), Int32Column(int32), Float32Column(float32) — each writes to a fixed-width column buffer with appropriate type code and stride. Write tests.
- [ ] Add CharColumn(rune) — stores as uint16 LE (UTF-16 code unit). Add DateColumn(time.Time) — stores as int64 milliseconds since epoch. Add TimestampNanosColumn(time.Time) — stores as int64 nanoseconds since epoch. Write tests including edge cases (zero time, max time, non-BMP rune for CharColumn).
- [ ] Add UuidColumn(hi, lo uint64) — stores as 16 bytes (lo LE then hi LE). Add VarcharColumn(string) — same encoding as StringColumn but with TYPE_VARCHAR type code. Write tests.
- [ ] Add GeohashColumn(hash uint64, precision int) — stores precision once per column, packed bits per row. Write tests.
- [ ] Add Int64Array1DColumn, Int64Array2DColumn, Int64Array3DColumn for LONG_ARRAY type. Write tests.

## Phase 7: Async Mode

- [ ] Create `qwp_sender_async.go` with `qwpAsyncState` struct: sendCh (buffered chan), inFlightCount/Max with mutex+cond, nextSequence/ackedSequence, ioErr, done/stopCh channels, WaitGroup. Implement acquireSlot() (blocks when window full), releaseSlot(), setError(). Write unit tests for flow control (acquireSlot blocks at max, unblocks on release).
- [ ] Implement I/O goroutine `ioLoop()`: reads from sendCh, sends over WebSocket, reads ACK, processes response, releases slot. On error: sets ioErr, signals cond, returns. Write test with mock connection verifying batches are sent and ACKs processed.
- [ ] Integrate async mode into qwpLineSender: if inFlightWindow > 1, create qwpAsyncState and start ioLoop goroutine on connect. Flush encodes pending rows, copies to batch, enqueues via sendCh (with acquireSlot). Flush waits for all in-flight to complete. Close: flush, close sendCh, wait for goroutine, close connection. Write test with in-flight window = 2.
- [ ] Add race condition tests for async mode: run with `go test -race`. Test concurrent auto-flush triggering, flush during active row building, close during in-flight batches.

## Phase 8: API Integration & Config

- [ ] Extend `conf_parse.go` to handle `ws` and `wss` schemas: map to qwpSenderType, set TLS mode for wss. Parse `in_flight_window` config key. Add validation in `sanitizeQwpConf()`. Write config string parsing tests: "ws::addr=localhost:9000;", "wss::addr=localhost:9000;tls_verify=unsafe_off;in_flight_window=4;".
- [ ] Extend `sender.go`: add `qwpSenderType` constant, add `WithQwp()` and `WithInFlightWindow()` option functions, add qwp case to `newLineSender()` and `newLineSenderConfig()`. Update `validateConf()` for inFlightWindow. Write tests for option functions and factory.
- [ ] Ensure existing tests still pass: run `go test ./...` and fix any breakage caused by interface changes or new sender type integration.

## Phase 9: Integration Tests

- [ ] Write `qwp_integration_test.go` with basic end-to-end test: create QWP sender via config string "ws::addr=localhost:9000;", insert rows with all basic types (symbol, long, double, string, bool, timestamp), flush, query via HTTP /exec, verify data. Include helper functions: queryQuestDB(), dropTable(), skipIfNoServer().
- [ ] Add integration tests for extended column types: byte, short, int32, float32, char, date, uuid, varchar, timestamp_nanos, double_array, long_array, decimal. Each test: insert → flush → query → verify.
- [ ] Add integration tests for multi-table batches, large batches (10k rows), auto-flush, symbol deduplication across flushes.
- [ ] Add integration test for async mode: create sender with in_flight_window=4, insert 50k rows, flush, verify all data arrived. Test with race detector.
- [ ] Add integration test for config string creation: LineSenderFromConf with ws:// schema.

## Phase 10: Polish & Hardening

- [ ] Review all error paths: connection errors, encoding errors, server errors, context cancellation. Ensure no goroutine leaks in async mode (add test). Ensure Close() is idempotent and safe after errors.
- [ ] Add benchmarks: BenchmarkQwpEncode, BenchmarkQwpFlush, BenchmarkQwpVarint, BenchmarkQwpSymbolLookup. Verify zero allocations per row on steady state with -benchmem.
- [ ] Run full test suite with race detector: `go test -race ./...`. Fix any data races.
- [ ] Final review: run `go vet ./...`, check for any TODOs or incomplete implementations. Verify all specs are implemented. Ensure all public types and functions have doc comments.

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
