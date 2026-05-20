/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

//go:build !windows

package questdb

// Go port of QuestDB's QwpSenderFuzzTest (e2e package), slice S1: the
// shared runner plus the simplest entry point (testLoad — default
// fuzz, symbols on, no reorder/skip/dup/new-col/non-ASCII/diff-case
// fuzz tweaks). The class has 27 @Test methods overall; the bulk of
// the work is the runner this file ships, and each remaining variant
// becomes a small entry-point that calls into it with different
// senderFuzzFuzz parameters.
//
// Faithful-port re-architecture (cf. the four ingress-oracle slices):
//
//   - The Java oracle (TableData / LineData) compares cursor-printer
//     text. The Go port stores typed values per cell and verifies
//     via the QWP query client cell-by-cell (same approach as the
//     ingress-oracle tests). The "what is the property under test"
//     stays the same; the assertion mechanism is Go-idiomatic and
//     avoids coupling to the server's CursorPrinter text format.
//   - Server tables are NOT pre-created. The producers' first writes
//     auto-create each table + its column set on the QuestDB side
//     (the test's whole premise). dropAllTables before / after via
//     t.Cleanup makes the test fixture-state-independent.
//   - Shared atomic timestamp counter (Java AtomicLong) →
//     sync/atomic.Int64 — guarantees globally-unique (table, ts) pairs
//     across all producer goroutines so there are no ts ties.
//   - Per-row "postfix" for STRING/SYMBOL values uses printable ASCII
//     A–Z for S1. Java emits a random char from the full BMP; that
//     fragility (unpaired surrogates etc.) is replaced with deterministic
//     ASCII here. Non-ASCII postfixes are the explicit job of the
//     senderFuzzFuzz.nonAsciiValueFactor variant (future slice S2).
//   - Row counts are CI-bounded compared to Java; the property under
//     test (multi-table multi-thread concurrent ingest, per-type
//     round-trip across the wire, no row loss) is unchanged.
//   - The Go QwpSender enforces "no same column twice in one row"
//     at the client (Java sends both and lets the server apply LWW);
//     senderFuzzAddColumnValue early-returns on a duplicate key. The
//     duplicatesFactor mechanic therefore reduces to a no-op at the
//     wire level in Go; we still record the first value in the
//     oracle, so the read-back matches what actually landed.
//   - Reproducible via QWP_FUZZ_SEED (shared newFuzzRand).
//
// Backlog (out of scope for S1):
//   - Fuzz variants: skip / reorder / dup / new-column / non-ASCII /
//     diff-case / sendSymbolsWithSpace (one entry-point each).
//   - Concurrent ALTER COLUMN TYPE thread + cross-type-cast oracle.
//   - Server-buffer tuning tests (testLoadSmallBuffer,
//     forceRecvFragmentationChunkSize) — server-side knob, not
//     reachable from a network client without a per-test fixture
//     boot.

import (
	"context"
	"fmt"
	"math/big"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// --- column / symbol catalog (mirrors Java QwpSenderFuzzTest fields)

// senderFuzzColType identifies a column's logical type for the
// per-type wire emission. Symbols are emitted via Symbol() rather
// than a typed Column(), but share the same value-derivation path so
// they live in the same enum.
type senderFuzzColType int

const (
	sftString senderFuzzColType = iota
	sftDouble
	sftByte
	sftShort
	sftInt
	sftFloat
	sftChar
	sftUUID
	sftLong256
	sftTsNano
	sftSymbol
)

// senderFuzzLegacyColumnCount: the first 6 entries in the column
// catalog are STRING/DOUBLE (the legacy ILP types the original Java
// test grew out of). The 8 typed columns that follow are always set
// on every row — skipColumns / addNewColumn restrict their eligible
// pool to legacy indexes so an unset typed cell never appears in
// the oracle (cf. file header note on type-default rendering).
const senderFuzzLegacyColumnCount = 6

// senderFuzzNewColumnRandomizeFactor is the postfix range for
// auto-injected "new column" names (e.g. "temperature0" vs
// "temperature1"). Mirrors Java NEW_COLUMN_RANDOMIZE_FACTOR. Unused
// in S1 (newColumnFactor=-1 = off); defined for future slices.
const senderFuzzNewColumnRandomizeFactor = 2

// senderFuzzColNameBases catalogs the case variants per column slot.
// Index 0 is the canonical (lowercase) form; indices 1+ are
// case-vary variants for diffCasesInColNames fuzz. QuestDB treats
// column names case-insensitively, so the oracle keys by
// strings.ToLower(name).
var senderFuzzColNameBases = [][]string{
	{"terület", "TERÜLet", "tERülET", "TERÜLET"},
	{"temperature", "TEMPERATURE", "Temperature", "TempeRaTuRe"},
	{"humidity", "HUMIdity", "HumiditY", "HUmiDIty", "HUMIDITY", "Humidity"},
	{"hőmérséklet", "HŐMÉRSÉKLET", "HŐmérséKLEt", "hőMÉRséKlET"},
	{"notes", "NOTES", "NotEs", "noTeS"},
	{"ветер", "Ветер", "ВЕТЕР", "вЕТЕр", "ВетЕР"},
	{"pressure_b", "PRESSURE_B", "Pressure_B"},
	{"pressure_s", "PRESSURE_S", "Pressure_S"},
	{"pressure_i", "PRESSURE_I", "Pressure_I"},
	{"pressure_f", "PRESSURE_F", "Pressure_F"},
	{"flag_c", "FLAG_C", "Flag_C"},
	{"sensor_id_u", "SENSOR_ID_U", "Sensor_Id_U"},
	{"token_l256", "TOKEN_L256", "Token_L256"},
	{"event_at_ns", "EVENT_AT_NS", "Event_At_Ns"},
}

var senderFuzzColTypes = []senderFuzzColType{
	sftString, sftDouble, sftDouble, sftDouble, sftString, sftDouble, // legacy 6
	sftByte, sftShort, sftInt, sftFloat, sftChar, sftUUID, sftLong256, sftTsNano,
}

// senderFuzzColValueBases drives per-row value derivation. The
// integer-family bases (BYTE/SHORT/INT/FLOAT, indices 6..9) are
// chosen so that base*10+digit always fits in the smallest target
// type (BYTE) — once the future ALTER COLUMN TYPE slice narrows a
// column across the integer family, every previously-written value
// still casts losslessly.
var senderFuzzColValueBases = []string{
	"europe", "8", "2", "1", "note", "6",
	"5", "9", "11", "7", "M", "u", "l", "1700000000000000000",
}

var senderFuzzSymbolNameBases = [][]string{
	{"location", "Location", "LOCATION", "loCATion", "LocATioN"},
	{"city", "ciTY", "CITY"},
}

var senderFuzzSymbolValueBases = []string{"us-midwest", "London"}

// senderFuzzNonAsciiChars spans the BMP byte-length spectrum (2/3
// byte UTF-8) so the wire path exercises multi-byte encoding without
// touching the surrogate pair edge cases — mirrors Java's
// nonAsciiChars (no astral plane chars; all single Go runes).
var senderFuzzNonAsciiChars = []rune{
	'ó', 'í', 'Á', 'ч', 'Ъ', 'Ж', 'ю', 0x3000, 0x3080, 0x3a55,
}

const senderFuzzBatchSize = 10

// senderFuzzTableNameRandomizeFactor controls the random table-name
// casing on each per-row pick (`WEATHERn` vs `weathern`). QuestDB
// resolves table names case-insensitively, so both forms target the
// same table.
const senderFuzzTableNameRandomizeFactor = 2

// senderFuzzMaxNumOfSkippedCols caps how many legacy STRING/DOUBLE
// columns the skipColumns fuzz may remove from one row (Java
// MAX_NUM_OF_SKIPPED_COLS). Typed columns are never eligible.
const senderFuzzMaxNumOfSkippedCols = 2

// senderFuzzSymbolsWithSpaceRandomizeFactor: when sendSymbolsWithSpace
// is on, ~50% of symbol value emissions get double-spaces injected at
// a random position. Mirrors Java SEND_SYMBOLS_WITH_SPACE_RANDOMIZE_FACTOR.
const senderFuzzSymbolsWithSpaceRandomizeFactor = 2

// --- per-row data + per-table oracle ------------------------------

// senderFuzzCell stores the typed value emitted by the sender for a
// single (row, column). On verification we read the typed value back
// through QwpColumnBatch and compare in the same type.
type senderFuzzCell struct {
	typ senderFuzzColType
	s   string
	f64 float64
	i64 int64
	ch  rune
	// uuid limbs
	uhi, ulo uint64
	// long256 limbs (l256[0] = LSB)
	l256 [4]int64
}

// senderFuzzRow groups one batch of cells per timestamp. Because the
// shared atomic timestamp counter guarantees globally-unique ts
// across producers, each row owns its ts unambiguously.
type senderFuzzRow struct {
	ts    int64 // microseconds
	cells map[string]senderFuzzCell
}

func newSenderFuzzRow(ts int64) *senderFuzzRow {
	return &senderFuzzRow{ts: ts, cells: make(map[string]senderFuzzCell, 16)}
}

// senderFuzzTable is the per-table oracle: rows appended in producer
// order under a lock (concurrent producers can hit the same table),
// then sorted by ts at assertion time to match `ORDER BY ts`. The
// colNames set is the union of every column ever written across the
// table's rows — the assertion uses it to verify that columns the
// schema has but a particular row didn't write are NULL on read-back
// (matches Java's TableData.generateRows NULL-fill behaviour).
type senderFuzzTable struct {
	mu       sync.Mutex
	name     string // canonical lowercase
	rows     []*senderFuzzRow
	colNames map[string]struct{}
}

func newSenderFuzzTable(name string) *senderFuzzTable {
	return &senderFuzzTable{name: name, colNames: make(map[string]struct{}, 32)}
}

func (t *senderFuzzTable) addRow(r *senderFuzzRow) {
	t.mu.Lock()
	t.rows = append(t.rows, r)
	for k := range r.cells {
		t.colNames[k] = struct{}{}
	}
	t.mu.Unlock()
}

func (t *senderFuzzTable) size() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.rows)
}

// snapshotRowsSorted returns a ts-sorted copy of the table's rows.
func (t *senderFuzzTable) snapshotRowsSorted() []*senderFuzzRow {
	t.mu.Lock()
	out := make([]*senderFuzzRow, len(t.rows))
	copy(out, t.rows)
	t.mu.Unlock()
	sort.Slice(out, func(i, j int) bool { return out[i].ts < out[j].ts })
	return out
}

// --- parameter structs --------------------------------------------

// senderFuzzLoad mirrors Java initLoadParameters. Each producer
// runs numIterations × numLines rows distributed across numTables
// tables, with an optional sleep between iterations.
//
// clientAutoFlushRows, when > 0, adds auto_flush_rows=N to the QWP
// connect string so the sender flushes every N rows. Used by tests
// whose fuzz config inflates per-batch frame size past the default
// server recv buffer (mirrors Java's clientAutoFlushRows).
type senderFuzzLoad struct {
	numLines           int
	numIterations      int
	numThreads         int
	numTables          int
	waitMs             int
	clientAutoFlushRows int
}

// senderFuzzFuzz mirrors Java initFuzzParameters. -1 means "off"
// for every factor; exerciseSymbols defaults to true (the testLoad
// path).
type senderFuzzFuzz struct {
	duplicatesFactor       int
	columnReorderingFactor int
	columnSkipFactor       int
	newColumnFactor        int
	nonAsciiValueFactor    int
	diffCasesInColNames    bool
	exerciseSymbols        bool
	sendSymbolsWithSpace   bool
	columnConvertProb      float64
}

func defaultSenderFuzzFuzz() senderFuzzFuzz {
	return senderFuzzFuzz{
		duplicatesFactor:       -1,
		columnReorderingFactor: -1,
		columnSkipFactor:       -1,
		newColumnFactor:        -1,
		nonAsciiValueFactor:    -1,
		diffCasesInColNames:    false,
		exerciseSymbols:        true,
		sendSymbolsWithSpace:   false,
		columnConvertProb:      0,
	}
}

// --- generation helpers -------------------------------------------

// senderFuzzShouldFuzz: a fuzz factor of -1 (or 0) means "off"; any
// positive N fires the fuzz on ~1/N of calls. Mirrors Java
// shouldFuzz.
func senderFuzzShouldFuzz(rnd *rand.Rand, factor int) bool {
	return factor > 0 && rnd.Intn(factor) == 0
}

// senderFuzzGenerateName picks one case variant for a column /
// symbol name. Used both for catalogued names and for the
// auto-injected new-column names; postfix is non-empty when called
// from the new-column path so the generated identifier doesn't
// collide with a catalogued one.
func senderFuzzGenerateName(bases []string, diffCases, randomize bool, rnd *rand.Rand) string {
	caseIdx := 0
	if diffCases {
		caseIdx = rnd.Intn(len(bases))
	}
	postfix := ""
	if randomize {
		postfix = strconv.Itoa(rnd.Intn(senderFuzzNewColumnRandomizeFactor))
	}
	return bases[caseIdx] + postfix
}

func senderFuzzGenerateColumnName(idx int, randomize bool, fuzz senderFuzzFuzz, rnd *rand.Rand) string {
	return senderFuzzGenerateName(senderFuzzColNameBases[idx], fuzz.diffCasesInColNames, randomize, rnd)
}

func senderFuzzGenerateSymbolName(idx int, randomize bool, fuzz senderFuzzFuzz, rnd *rand.Rand) string {
	return senderFuzzGenerateName(senderFuzzSymbolNameBases[idx], fuzz.diffCasesInColNames, randomize, rnd)
}

// senderFuzzPickTableName randomly selects one of numTables, with a
// random uppercase/lowercase prefix on each call (QuestDB resolves
// table names case-insensitively).
func senderFuzzPickTableName(numTables int, rnd *rand.Rand) string {
	prefix := "weather"
	if rnd.Intn(senderFuzzTableNameRandomizeFactor) == 0 {
		prefix = "WEATHER"
	}
	return prefix + strconv.Itoa(rnd.Intn(numTables))
}

// senderFuzzPostfixChar returns the single-character suffix appended
// to STRING/SYMBOL value bases. With nonAsciiValueFactor > 0, a
// matching ratio of calls returns a BMP non-ASCII char from the
// catalog (2/3-byte UTF-8) — exercises multi-byte encoding on the
// wire. Otherwise a printable-ASCII letter (Java emits a random
// BMP char; the surrogate edge cases that fragility implies aren't
// the property under test).
func senderFuzzPostfixChar(fuzz senderFuzzFuzz, rnd *rand.Rand) string {
	if senderFuzzShouldFuzz(rnd, fuzz.nonAsciiValueFactor) {
		return string(senderFuzzNonAsciiChars[rnd.Intn(len(senderFuzzNonAsciiChars))])
	}
	return string(rune('A' + rnd.Intn(26)))
}

// senderFuzzAddColumnValue emits one (typed) column over the QWP
// sender AND records the typed value in the oracle row.
// Faithful to Java QwpSenderFuzzTest.addColumnValue with the
// CursorPrinter "yield text" removed (we compare typed cells, not
// rendered strings).
func senderFuzzAddColumnValue(
	typ senderFuzzColType,
	valueBase string,
	colName string,
	qs QwpSender,
	row *senderFuzzRow,
	fuzz senderFuzzFuzz,
	rnd *rand.Rand,
) {
	key := strings.ToLower(colName)
	// Go-divergence vs Java: the Go QwpSender enforces "no same column
	// twice in one row" at the client side (Java sends both writes
	// and lets the server apply LWW). Skip the second emission and
	// keep the first value in the oracle to match the wire reality.
	// Affects: the duplicatesFactor mechanic becomes a client-side
	// no-op; addNewColumn / addNewSymbol attempts that collide on the
	// generated random postfix likewise skip. Documented in the file
	// header.
	if _, exists := row.cells[key]; exists {
		return
	}
	switch typ {
	case sftDouble:
		base, _ := strconv.Atoi(valueBase)
		v := float64(base*10 + rnd.Intn(9))
		qs.Float64Column(colName, v)
		row.cells[key] = senderFuzzCell{typ: typ, f64: v}
	case sftString:
		s := valueBase + senderFuzzPostfixChar(fuzz, rnd)
		qs.StringColumn(colName, s)
		row.cells[key] = senderFuzzCell{typ: typ, s: s}
	case sftSymbol:
		base := valueBase
		if fuzz.sendSymbolsWithSpace && rnd.Intn(senderFuzzSymbolsWithSpaceRandomizeFactor) == 0 && len(base) > 1 {
			// Inject double-space at a random interior position
			// (mirrors Java sendSymbolsWithSpace branch).
			spaceIdx := rnd.Intn(len(base) - 1)
			base = base[:spaceIdx] + "  " + base[spaceIdx:]
		}
		s := base + senderFuzzPostfixChar(fuzz, rnd)
		qs.Symbol(colName, s)
		row.cells[key] = senderFuzzCell{typ: typ, s: s}
	case sftByte:
		base, _ := strconv.Atoi(valueBase)
		v := int8(base*10 + rnd.Intn(9))
		qs.ByteColumn(colName, v)
		row.cells[key] = senderFuzzCell{typ: typ, i64: int64(v)}
	case sftShort:
		base, _ := strconv.Atoi(valueBase)
		v := int16(base*10 + rnd.Intn(9))
		qs.ShortColumn(colName, v)
		row.cells[key] = senderFuzzCell{typ: typ, i64: int64(v)}
	case sftInt:
		base, _ := strconv.Atoi(valueBase)
		v := int32(base*10 + rnd.Intn(9))
		qs.Int32Column(colName, v)
		row.cells[key] = senderFuzzCell{typ: typ, i64: int64(v)}
	case sftFloat:
		base, _ := strconv.Atoi(valueBase)
		v := float32(base*10 + rnd.Intn(9))
		qs.Float32Column(colName, v)
		row.cells[key] = senderFuzzCell{typ: typ, f64: float64(v)}
	case sftChar:
		c := rune(valueBase[0]) + rune(rnd.Intn(10))
		qs.CharColumn(colName, c)
		row.cells[key] = senderFuzzCell{typ: typ, ch: c}
	case sftUUID:
		// Force the top 32 bits of each limb non-zero so neither half
		// renders as the LONG_NULL sentinel — the same guard Java
		// applies in addColumnValue.
		hi := uint64(rnd.Int31()+1)<<32 | uint64(rnd.Uint32())
		lo := uint64(rnd.Int31()+1)<<32 | uint64(rnd.Uint32())
		qs.UuidColumn(colName, hi, lo)
		row.cells[key] = senderFuzzCell{typ: typ, uhi: hi, ulo: lo}
	case sftLong256:
		// Java sends 4 limbs LSB-first via long256Column(name, l0..l3).
		// Go's Long256Column takes a big.Int composed MSB-first. We
		// store the limbs LSB-first in the cell (l256[0] = l0 = LSB)
		// so the readback Long256Word(ci, br, w) maps directly to
		// l256[w].
		l0 := (rnd.Int63() & 0x7FFFFFFFFFFFFFFF) | 1
		l1 := (rnd.Int63() & 0x7FFFFFFFFFFFFFFF) | 1
		l2 := (rnd.Int63() & 0x7FFFFFFFFFFFFFFF) | 1
		l3 := (rnd.Int63() & 0x7FFFFFFFFFFFFFFF) | 1
		v := new(big.Int).SetUint64(uint64(l3))
		for _, limb := range []int64{l2, l1, l0} {
			v.Lsh(v, 64)
			v.Or(v, new(big.Int).SetUint64(uint64(limb)))
		}
		qs.Long256Column(colName, v)
		row.cells[key] = senderFuzzCell{typ: typ, l256: [4]int64{l0, l1, l2, l3}}
	case sftTsNano:
		// Step in microseconds off the base so the low 3 nanos are
		// always zero — matches Java's nanos = base + rnd*1000.
		base, _ := strconv.ParseInt(valueBase, 10, 64)
		nanos := base + int64(rnd.Intn(1_000_000))*1_000
		qs.TimestampNanosColumn(colName, time.Unix(0, nanos).UTC())
		row.cells[key] = senderFuzzCell{typ: typ, i64: nanos}
	}
}

// senderFuzzGenerateOrdering returns either the identity ordering or
// a shuffled permutation of [0..n), depending on columnReorderingFactor.
// Mirrors Java generateOrdering.
func senderFuzzGenerateOrdering(n, factor int, rnd *rand.Rand) []int {
	out := make([]int, n)
	for i := 0; i < n; i++ {
		out[i] = i
	}
	if senderFuzzShouldFuzz(rnd, factor) {
		rnd.Shuffle(n, func(i, j int) { out[i], out[j] = out[j], out[i] })
	}
	return out
}

// senderFuzzSkipColumns optionally removes 1..senderFuzzMaxNumOfSkippedCols
// legacy STRING/DOUBLE indexes (those < senderFuzzLegacyColumnCount)
// from the ordering. Typed columns are never eligible: an unset
// typed cell renders differently from its type-default sentinel, so
// skipping one would clash with the oracle's "absent → NULL"
// assertion once the future ALTER slice converts types across the
// integer family. Mirrors Java skipColumns.
func senderFuzzSkipColumns(orig []int, factor int, rnd *rand.Rand) []int {
	if !senderFuzzShouldFuzz(rnd, factor) {
		return orig
	}
	out := append([]int(nil), orig...)
	numToSkip := rnd.Intn(senderFuzzMaxNumOfSkippedCols) + 1
	for i := 0; i < numToSkip; i++ {
		// Count legacy-eligible entries still in the slice.
		eligible := 0
		for _, idx := range out {
			if idx < senderFuzzLegacyColumnCount {
				eligible++
			}
		}
		if eligible == 0 {
			break
		}
		target := rnd.Intn(eligible)
		for j := 0; j < len(out); j++ {
			if out[j] < senderFuzzLegacyColumnCount {
				if target == 0 {
					out = append(out[:j], out[j+1:]...)
					break
				}
				target--
			}
		}
	}
	return out
}

// senderFuzzAddDuplicateColumn re-emits the same column (same name)
// with a freshly random value when duplicatesFactor fires. Server
// resolves duplicates per row as last-write-wins; the oracle's
// row.cells map naturally overwrites the prior cell.
func senderFuzzAddDuplicateColumn(colIdx int, colName string, qs QwpSender, row *senderFuzzRow, fuzz senderFuzzFuzz, rnd *rand.Rand) {
	if !senderFuzzShouldFuzz(rnd, fuzz.duplicatesFactor) {
		return
	}
	senderFuzzAddColumnValue(senderFuzzColTypes[colIdx], senderFuzzColValueBases[colIdx],
		colName, qs, row, fuzz, rnd)
}

func senderFuzzAddDuplicateSymbol(symIdx int, symName string, qs QwpSender, row *senderFuzzRow, fuzz senderFuzzFuzz, rnd *rand.Rand) {
	if !senderFuzzShouldFuzz(rnd, fuzz.duplicatesFactor) {
		return
	}
	senderFuzzAddColumnValue(sftSymbol, senderFuzzSymbolValueBases[symIdx],
		symName, qs, row, fuzz, rnd)
}

// senderFuzzAddNewColumn picks a random legacy column slot, generates
// a name with a numeric postfix (so it doesn't collide with the
// catalogued name), emits its value, and records it. The server
// auto-adds the column to the table on first write; rows that
// didn't emit it appear as NULL on read.
func senderFuzzAddNewColumn(qs QwpSender, row *senderFuzzRow, fuzz senderFuzzFuzz, rnd *rand.Rand) {
	if !senderFuzzShouldFuzz(rnd, fuzz.newColumnFactor) {
		return
	}
	extraColIdx := rnd.Intn(senderFuzzLegacyColumnCount)
	colName := senderFuzzGenerateColumnName(extraColIdx, true, fuzz, rnd)
	senderFuzzAddColumnValue(senderFuzzColTypes[extraColIdx], senderFuzzColValueBases[extraColIdx],
		colName, qs, row, fuzz, rnd)
}

func senderFuzzAddNewSymbol(qs QwpSender, row *senderFuzzRow, fuzz senderFuzzFuzz, rnd *rand.Rand) {
	if !senderFuzzShouldFuzz(rnd, fuzz.newColumnFactor) {
		return
	}
	extraSymIdx := rnd.Intn(len(senderFuzzSymbolNameBases))
	symName := senderFuzzGenerateSymbolName(extraSymIdx, true, fuzz, rnd)
	senderFuzzAddColumnValue(sftSymbol, senderFuzzSymbolValueBases[extraSymIdx],
		symName, qs, row, fuzz, rnd)
}

// senderFuzzEmitRow emits one row through the QWP sender + records
// it in the oracle. Symbols first (the QWP ordering invariant the
// ingress-oracle ports already document), then columns. Each symbol
// /column emission may be followed by a same-cell duplicate and a
// brand-new injected column, depending on the duplicatesFactor /
// newColumnFactor settings — faithful to Java generateLine.
func senderFuzzEmitRow(
	tableName string,
	qs QwpSender,
	row *senderFuzzRow,
	fuzz senderFuzzFuzz,
	rnd *rand.Rand,
) {
	qs.Table(tableName)
	if fuzz.exerciseSymbols {
		symIndexes := senderFuzzSkipColumns(
			senderFuzzGenerateOrdering(len(senderFuzzSymbolNameBases), fuzz.columnReorderingFactor, rnd),
			fuzz.columnSkipFactor, rnd)
		// Note: skipColumns only removes *legacy* indexes; symbol
		// indexes are 0/1 (always < legacy threshold) so they ARE
		// eligible for skip in Java — preserve that here.
		for _, symIdx := range symIndexes {
			symName := senderFuzzGenerateSymbolName(symIdx, false, fuzz, rnd)
			senderFuzzAddColumnValue(sftSymbol, senderFuzzSymbolValueBases[symIdx],
				symName, qs, row, fuzz, rnd)
			senderFuzzAddDuplicateSymbol(symIdx, symName, qs, row, fuzz, rnd)
			senderFuzzAddNewSymbol(qs, row, fuzz, rnd)
		}
	}
	colIndexes := senderFuzzSkipColumns(
		senderFuzzGenerateOrdering(len(senderFuzzColNameBases), fuzz.columnReorderingFactor, rnd),
		fuzz.columnSkipFactor, rnd)
	for _, colIdx := range colIndexes {
		colName := senderFuzzGenerateColumnName(colIdx, false, fuzz, rnd)
		senderFuzzAddColumnValue(senderFuzzColTypes[colIdx], senderFuzzColValueBases[colIdx],
			colName, qs, row, fuzz, rnd)
		senderFuzzAddDuplicateColumn(colIdx, colName, qs, row, fuzz, rnd)
		senderFuzzAddNewColumn(qs, row, fuzz, rnd)
	}
}

// --- runner -------------------------------------------------------

// senderFuzzRunTest spawns load.numThreads producer goroutines, each
// running load.numIterations × load.numLines rows distributed across
// load.numTables tables. After every producer finishes, drains WAL
// for every table that received rows and asserts the table contents
// cell-by-cell against the oracle.
//
// The runner is the foundational piece every QwpSenderFuzzTest
// scenario consumes; each future entry point is just a small
// configuration of (senderFuzzLoad, senderFuzzFuzz) calling here.
func senderFuzzRunTest(t *testing.T, srv *qwpFuzzServer, load senderFuzzLoad, fuzz senderFuzzFuzz, rnd *rand.Rand) {
	t.Helper()

	// One oracle per logical table (canonical lowercase name).
	oracles := make(map[string]*senderFuzzTable, load.numTables)
	for i := 0; i < load.numTables; i++ {
		name := "weather" + strconv.Itoa(i)
		oracles[name] = newSenderFuzzTable(name)
	}

	// Shared atomic ts counter (Java AtomicLong timestampMicros) —
	// every row gets a globally-unique microsecond timestamp, so
	// no two rows ever collide on ts.
	var tsCounter atomic.Int64
	tsCounter.Store(1_465_839_830_102_300)

	// Wipe any leftover tables from a previous test run, and ensure
	// the same on exit. dropAllTables is the fixture's "clean slate"
	// primitive — this slice is its first consumer.
	srv.dropAllTables(t)
	t.Cleanup(func() { srv.dropAllTables(t) })

	var wg sync.WaitGroup
	errs := make([]error, load.numThreads)
	for tid := 0; tid < load.numThreads; tid++ {
		threadSeed := rnd.Int63()
		wg.Add(1)
		go func(tid int, seed int64) {
			defer wg.Done()
			defer func() {
				if rec := recover(); rec != nil {
					errs[tid] = fmt.Errorf("thread %d panicked: %v", tid, rec)
				}
			}()
			tRnd := rand.New(rand.NewSource(seed))
			ctx := context.Background()
			conf := fmt.Sprintf("ws::addr=%s;", srv.wsAddr())
			if load.clientAutoFlushRows > 0 {
				conf += fmt.Sprintf("auto_flush_rows=%d;", load.clientAutoFlushRows)
			}
			sctx, scancel := context.WithTimeout(context.Background(), 15*time.Second)
			ls, err := LineSenderFromConf(sctx, conf)
			scancel()
			if err != nil {
				errs[tid] = fmt.Errorf("thread %d open: %w", tid, err)
				return
			}
			qs, ok := ls.(QwpSender)
			if !ok {
				errs[tid] = fmt.Errorf("thread %d: not a QwpSender (%T)", tid, ls)
				_ = ls.Close(ctx)
				return
			}
			defer func() {
				cctx, ccancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer ccancel()
				_ = qs.Close(cctx)
			}()
			published := 0
			for n := 0; n < load.numIterations; n++ {
				for j := 0; j < load.numLines; j++ {
					ts := tsCounter.Add(1)
					tableName := senderFuzzPickTableName(load.numTables, tRnd)
					row := newSenderFuzzRow(ts)
					senderFuzzEmitRow(tableName, qs, row, fuzz, tRnd)
					if err := qs.At(ctx, time.UnixMicro(ts).UTC()); err != nil {
						errs[tid] = fmt.Errorf("thread %d at@row %d: %w", tid, published, err)
						return
					}
					base := strings.ToLower(tableName)
					if tbl, ok := oracles[base]; ok {
						tbl.addRow(row)
					}
					published++
					if published%senderFuzzBatchSize == 0 {
						if err := qs.Flush(ctx); err != nil {
							errs[tid] = fmt.Errorf("thread %d flush@%d: %w", tid, published, err)
							return
						}
					}
				}
				if err := qs.Flush(ctx); err != nil {
					errs[tid] = fmt.Errorf("thread %d end-of-iter flush: %w", tid, err)
					return
				}
				if load.waitMs > 0 {
					time.Sleep(time.Duration(load.waitMs) * time.Millisecond)
				}
			}
		}(tid, threadSeed)
	}
	wg.Wait()
	for tid, e := range errs {
		if e != nil {
			t.Fatalf("thread %d: %v", tid, e)
		}
	}

	// Wait for WAL apply per table that has rows, then assert.
	for _, tbl := range oracles {
		if tbl.size() == 0 {
			continue
		}
		if !senderFuzzPollRows(t, srv, tbl.name, tbl.size(), 60*time.Second) {
			t.Logf("server log tail (8K):\n%s", srv.tailLog(8000))
			t.Fatalf("table %q did not reach %d rows", tbl.name, tbl.size())
		}
	}

	qc := newBindFuzzClient(t, srv)
	for _, tbl := range oracles {
		if tbl.size() > 0 {
			senderFuzzAssertTable(t, qc, tbl)
		}
	}
}

// senderFuzzAssertTable reads tbl via QWP `SELECT * ORDER BY ts` and
// matches each row's typed cells against the oracle. Columns the
// oracle never wrote (none in S1 — every row writes every column)
// are not checked; columns the oracle wrote MUST be present and
// equal in the schema.
func senderFuzzAssertTable(t *testing.T, qc *QwpQueryClient, tbl *senderFuzzTable) {
	t.Helper()
	want := tbl.snapshotRowsSorted()

	// QuestDB auto-creates the designated timestamp column with the
	// default ILP/QWP name "timestamp" when the table is created via
	// the first sender.At(...) call (no pre-created DDL here). The
	// oracle uses microsecond ts; QWP exposes it as the int64 of that
	// column. Java reaches it via metadata.getTimestampIndex(); we
	// look it up by name.
	const tsColName = "timestamp"

	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	q := qc.Query(ctx, "SELECT * FROM '"+tbl.name+"' ORDER BY "+tsColName)
	defer q.Close()

	rowIdx := 0
	for batch, err := range q.Batches() {
		if err != nil {
			t.Fatalf("table %q query: %v", tbl.name, err)
		}
		colIdx := make(map[string]int, batch.ColumnCount())
		for i := 0; i < batch.ColumnCount(); i++ {
			colIdx[strings.ToLower(batch.ColumnName(i))] = i
		}
		for br := 0; br < batch.RowCount(); br++ {
			if rowIdx >= len(want) {
				t.Fatalf("table %q: more rows returned (%d+) than oracle holds (%d)",
					tbl.name, rowIdx+1, len(want))
			}
			row := want[rowIdx]
			rowIdx++
			if ci, ok := colIdx[tsColName]; ok {
				if got := batch.Int64(ci, br); got != row.ts {
					t.Fatalf("table %q row %d ts: want %d got %d",
						tbl.name, rowIdx-1, row.ts, got)
				}
			}
			for name, cell := range row.cells {
				ci, present := colIdx[name]
				if !present {
					t.Fatalf("table %q row ts=%d: column %q set in oracle but absent from schema",
						tbl.name, row.ts, name)
				}
				if batch.IsNull(ci, br) {
					t.Fatalf("table %q row ts=%d col %q: expected non-null", tbl.name, row.ts, name)
				}
				senderFuzzAssertCell(t, batch, ci, br, tbl.name, row.ts, name, cell)
			}
			// Columns the table schema has (because some OTHER row
			// wrote them) but THIS row didn't write must be NULL on
			// read-back — mirrors Java TableData.generateRows's
			// NULL-fill behaviour.
			tbl.mu.Lock()
			absent := make([]string, 0, 4)
			for name := range tbl.colNames {
				if _, set := row.cells[name]; !set {
					absent = append(absent, name)
				}
			}
			tbl.mu.Unlock()
			for _, name := range absent {
				ci, present := colIdx[name]
				if !present {
					continue
				}
				if !batch.IsNull(ci, br) {
					t.Fatalf("table %q row ts=%d col %q: expected NULL (unset by this row), got non-null",
						tbl.name, row.ts, name)
				}
			}
		}
	}
	if rowIdx != len(want) {
		t.Fatalf("table %q: oracle holds %d rows, query returned %d",
			tbl.name, len(want), rowIdx)
	}
}

// senderFuzzPollRows is awaitRows with diagnostic-friendly return
// semantics (bool, doesn't t.Fatalf) so the caller can dump the
// server log on timeout.
func senderFuzzPollRows(t *testing.T, srv *qwpFuzzServer, table string, want int, timeout time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	q := fmt.Sprintf("SELECT count() FROM '%s'", table)
	var lastN int64
	for {
		res, err := srv.execSQL(q)
		if err == nil && len(res.Dataset) == 1 && len(res.Dataset[0]) == 1 {
			if n, ok := toInt64(res.Dataset[0][0]); ok {
				lastN = n
				if n >= int64(want) {
					return true
				}
			}
		}
		if time.Now().After(deadline) {
			t.Logf("table %q: %d / %d rows after %s", table, lastN, want, timeout)
			return false
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func senderFuzzAssertCell(t *testing.T, b *QwpColumnBatch, ci, br int,
	tableName string, ts int64, colName string, c senderFuzzCell) {
	t.Helper()
	switch c.typ {
	case sftString, sftSymbol:
		if got := b.String(ci, br); got != c.s {
			t.Fatalf("table %q row ts=%d col %q (str): want %q got %q",
				tableName, ts, colName, c.s, got)
		}
	case sftDouble:
		if got := b.Float64(ci, br); got != c.f64 {
			t.Fatalf("table %q row ts=%d col %q (double): want %v got %v",
				tableName, ts, colName, c.f64, got)
		}
	case sftByte:
		if got := int64(b.Int8(ci, br)); got != c.i64 {
			t.Fatalf("table %q row ts=%d col %q (byte): want %d got %d",
				tableName, ts, colName, c.i64, got)
		}
	case sftShort:
		if got := int64(b.Int16(ci, br)); got != c.i64 {
			t.Fatalf("table %q row ts=%d col %q (short): want %d got %d",
				tableName, ts, colName, c.i64, got)
		}
	case sftInt:
		if got := int64(b.Int32(ci, br)); got != c.i64 {
			t.Fatalf("table %q row ts=%d col %q (int): want %d got %d",
				tableName, ts, colName, c.i64, got)
		}
	case sftFloat:
		if got := float64(b.Float32(ci, br)); got != c.f64 {
			t.Fatalf("table %q row ts=%d col %q (float): want %v got %v",
				tableName, ts, colName, c.f64, got)
		}
	case sftChar:
		if got := b.Char(ci, br); got != c.ch {
			t.Fatalf("table %q row ts=%d col %q (char): want %q got %q",
				tableName, ts, colName, c.ch, got)
		}
	case sftUUID:
		gh := uint64(b.UuidHi(ci, br))
		gl := uint64(b.UuidLo(ci, br))
		if gh != c.uhi || gl != c.ulo {
			t.Fatalf("table %q row ts=%d col %q (uuid): want hi=%d lo=%d got hi=%d lo=%d",
				tableName, ts, colName, c.uhi, c.ulo, gh, gl)
		}
	case sftLong256:
		for w := 0; w < 4; w++ {
			if got := b.Long256Word(ci, br, w); got != c.l256[w] {
				t.Fatalf("table %q row ts=%d col %q (long256) w%d: want %d got %d",
					tableName, ts, colName, w, c.l256[w], got)
			}
		}
	case sftTsNano:
		if got := b.Int64(ci, br); got != c.i64 {
			t.Fatalf("table %q row ts=%d col %q (tsnano): want %d got %d",
				tableName, ts, colName, c.i64, got)
		}
	}
}

// --- entry points -------------------------------------------------

// TestQwpFuzzSenderLoad is the Go port of
// QwpSenderFuzzTest.testLoad (the simplest entry point — default
// fuzz, symbols on, no reorder/skip/dup/new-col/non-ASCII). Counts
// are CI-bounded compared to Java's (100, 5, 7, 12, 20).
func TestQwpFuzzSenderLoad(t *testing.T) {
	srv := fuzzServer(t)
	r := newFuzzRand(t)
	senderFuzzRunTest(t, srv, senderFuzzLoad{
		numLines: 50, numIterations: 2, numThreads: 3, numTables: 4, waitMs: 20,
	}, defaultSenderFuzzFuzz(), r)
}

// --- S2 fuzz variants ---------------------------------------------
//
// Each test calls senderFuzzRunTest with a different (load, fuzz)
// configuration; the runner itself is unchanged. Counts are
// CI-bounded vs Java. Java enables convertProb=0.05 (ALTER COLUMN
// TYPE) on most of these via the 7-arg initFuzzParameters overload;
// the Go ports set convertProb=0 — the ALTER concurrent thread
// lands as a dedicated S3 slice (see file header). The fuzz
// mechanics under test here (reorder / skip / dup / new-col /
// non-ASCII / diff-case / sendSymbolsWithSpace) are exercised in
// isolation, on a stable schema.

func TestQwpFuzzSenderLoadLargePayload(t *testing.T) {
	srv := fuzzServer(t)
	r := newFuzzRand(t)
	senderFuzzRunTest(t, srv, senderFuzzLoad{
		numLines: 200, numIterations: 2, numThreads: 3, numTables: 4, waitMs: 10,
	}, defaultSenderFuzzFuzz(), r)
}

func TestQwpFuzzSenderLoadNoSymbols(t *testing.T) {
	srv := fuzzServer(t)
	r := newFuzzRand(t)
	fuzz := defaultSenderFuzzFuzz()
	fuzz.nonAsciiValueFactor = 5
	fuzz.exerciseSymbols = false
	senderFuzzRunTest(t, srv, senderFuzzLoad{
		numLines: 50, numIterations: 2, numThreads: 3, numTables: 4, waitMs: 20,
	}, fuzz, r)
}

func TestQwpFuzzSenderLoadSendSymbolsWithSpace(t *testing.T) {
	srv := fuzzServer(t)
	r := newFuzzRand(t)
	fuzz := defaultSenderFuzzFuzz()
	fuzz.newColumnFactor = 2
	fuzz.sendSymbolsWithSpace = true
	senderFuzzRunTest(t, srv, senderFuzzLoad{
		numLines: 50, numIterations: 2, numThreads: 3, numTables: 4, waitMs: 20,
		clientAutoFlushRows: 5,
	}, fuzz, r)
}

func TestQwpFuzzSenderCaseVariationReorderingColumns(t *testing.T) {
	srv := fuzzServer(t)
	r := newFuzzRand(t)
	fuzz := defaultSenderFuzzFuzz()
	fuzz.columnReorderingFactor = 4
	fuzz.newColumnFactor = 2
	fuzz.diffCasesInColNames = true
	senderFuzzRunTest(t, srv, senderFuzzLoad{
		numLines: 50, numIterations: 2, numThreads: 3, numTables: 4, waitMs: 50,
	}, fuzz, r)
}

func TestQwpFuzzSenderCaseVariationReorderingColumnsNoSymbols(t *testing.T) {
	srv := fuzzServer(t)
	r := newFuzzRand(t)
	fuzz := defaultSenderFuzzFuzz()
	fuzz.columnReorderingFactor = 4
	fuzz.diffCasesInColNames = true
	fuzz.exerciseSymbols = false
	senderFuzzRunTest(t, srv, senderFuzzLoad{
		numLines: 50, numIterations: 2, numThreads: 3, numTables: 4, waitMs: 50,
	}, fuzz, r)
}

func TestQwpFuzzSenderCaseVariationReorderingColumnsSendSymbolsWithSpace(t *testing.T) {
	srv := fuzzServer(t)
	r := newFuzzRand(t)
	fuzz := defaultSenderFuzzFuzz()
	fuzz.columnReorderingFactor = 4
	fuzz.newColumnFactor = 3
	fuzz.diffCasesInColNames = true
	fuzz.sendSymbolsWithSpace = true
	senderFuzzRunTest(t, srv, senderFuzzLoad{
		numLines: 50, numIterations: 2, numThreads: 3, numTables: 4, waitMs: 50,
		clientAutoFlushRows: 5,
	}, fuzz, r)
}

func TestQwpFuzzSenderNonAsciiValues(t *testing.T) {
	srv := fuzzServer(t)
	r := newFuzzRand(t)
	fuzz := defaultSenderFuzzFuzz()
	fuzz.newColumnFactor = 3
	fuzz.nonAsciiValueFactor = 4
	senderFuzzRunTest(t, srv, senderFuzzLoad{
		numLines: 50, numIterations: 2, numThreads: 3, numTables: 4, waitMs: 50,
	}, fuzz, r)
}

func TestQwpFuzzSenderNonAsciiValuesNoSymbols(t *testing.T) {
	srv := fuzzServer(t)
	r := newFuzzRand(t)
	fuzz := defaultSenderFuzzFuzz()
	fuzz.nonAsciiValueFactor = 4
	fuzz.exerciseSymbols = false
	senderFuzzRunTest(t, srv, senderFuzzLoad{
		numLines: 50, numIterations: 2, numThreads: 3, numTables: 4, waitMs: 50,
	}, fuzz, r)
}

func TestQwpFuzzSenderReorderingColumns(t *testing.T) {
	srv := fuzzServer(t)
	r := newFuzzRand(t)
	fuzz := defaultSenderFuzzFuzz()
	fuzz.columnReorderingFactor = 4
	fuzz.nonAsciiValueFactor = 8
	fuzz.sendSymbolsWithSpace = true
	senderFuzzRunTest(t, srv, senderFuzzLoad{
		numLines: 50, numIterations: 2, numThreads: 3, numTables: 4, waitMs: 50,
	}, fuzz, r)
}

func TestQwpFuzzSenderReorderingColumnsNoSymbols(t *testing.T) {
	srv := fuzzServer(t)
	r := newFuzzRand(t)
	fuzz := defaultSenderFuzzFuzz()
	fuzz.columnReorderingFactor = 4
	fuzz.diffCasesInColNames = true
	fuzz.exerciseSymbols = false
	senderFuzzRunTest(t, srv, senderFuzzLoad{
		numLines: 50, numIterations: 2, numThreads: 3, numTables: 4, waitMs: 50,
	}, fuzz, r)
}

func TestQwpFuzzSenderReorderingManyThreads(t *testing.T) {
	srv := fuzzServer(t)
	r := newFuzzRand(t)
	fuzz := defaultSenderFuzzFuzz()
	fuzz.columnReorderingFactor = 3
	fuzz.newColumnFactor = 2
	senderFuzzRunTest(t, srv, senderFuzzLoad{
		numLines: 40, numIterations: 2, numThreads: 5, numTables: 3, waitMs: 30,
	}, fuzz, r)
}

func TestQwpFuzzSenderReorderingNonAscii(t *testing.T) {
	srv := fuzzServer(t)
	r := newFuzzRand(t)
	fuzz := defaultSenderFuzzFuzz()
	fuzz.columnReorderingFactor = 4
	fuzz.newColumnFactor = 2
	fuzz.nonAsciiValueFactor = 4
	senderFuzzRunTest(t, srv, senderFuzzLoad{
		numLines: 50, numIterations: 2, numThreads: 3, numTables: 4, waitMs: 50,
	}, fuzz, r)
}

func TestQwpFuzzSenderReorderingSkipColumnsWithNonAscii(t *testing.T) {
	srv := fuzzServer(t)
	r := newFuzzRand(t)
	fuzz := defaultSenderFuzzFuzz()
	fuzz.columnReorderingFactor = 4
	fuzz.columnSkipFactor = 4
	fuzz.newColumnFactor = 2
	fuzz.nonAsciiValueFactor = 4
	fuzz.diffCasesInColNames = true
	senderFuzzRunTest(t, srv, senderFuzzLoad{
		numLines: 50, numIterations: 2, numThreads: 3, numTables: 4, waitMs: 50,
	}, fuzz, r)
}

func TestQwpFuzzSenderReorderingSkipColumnsWithNonAsciiNoSymbols(t *testing.T) {
	srv := fuzzServer(t)
	r := newFuzzRand(t)
	fuzz := defaultSenderFuzzFuzz()
	fuzz.columnReorderingFactor = 4
	fuzz.columnSkipFactor = 4
	fuzz.nonAsciiValueFactor = 4
	fuzz.diffCasesInColNames = true
	fuzz.exerciseSymbols = false
	senderFuzzRunTest(t, srv, senderFuzzLoad{
		numLines: 50, numIterations: 2, numThreads: 3, numTables: 4, waitMs: 50,
	}, fuzz, r)
}
