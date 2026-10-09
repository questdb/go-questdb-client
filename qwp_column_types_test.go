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

package questdb

import (
	"bytes"
	"context"
	"net"
	"testing"
)

// This interface keeps the regression runnable against the pre-fix API.
type qwpIssue68Sender interface {
	QwpSender
	Ipv4Column(string, net.IP) QwpSender
	BinaryColumn(string, []byte) QwpSender
}

func issue68API(t *testing.T, s LineSender) qwpIssue68Sender {
	t.Helper()
	q, ok := s.(qwpIssue68Sender)
	if !ok {
		t.Fatal("QWP sender is missing IPv4/BINARY setters (issue #68)")
	}
	return q
}

func issue68Wire(t *testing.T, s *qwpLineSender, name string, typ qwpTypeCode, want []byte) {
	t.Helper()
	tb := s.tableBuffers["types"]
	idx, ok := tb.columnIndex[name]
	if !ok {
		t.Fatalf("column %s missing", name)
	}
	col := tb.columns[idx]
	if col.typeCode != typ {
		t.Fatalf("%s type = %x, want %x", name, col.typeCode, typ)
	}
	var enc qwpEncoder
	enc.encodeColumnData(col)
	if got := enc.wb.bytes(); !bytes.Equal(got, want) {
		t.Fatalf("%s wire = %x, want %x", name, got, want)
	}
	before := tb.dataSize
	tb.recomputeDataSize()
	if tb.dataSize != before {
		t.Fatalf("incremental size = %d, recomputed = %d", before, tb.dataSize)
	}
}

func TestQwpIssue68WireAndReuse(t *testing.T) {
	ctx := context.Background()
	srv := newQwpTestServer(t)
	defer srv.Close()
	s := newQwpSenderForTest(t, srv.URL)
	defer s.Close(ctx)
	q := issue68API(t, s)
	payload := []byte{0, 255, 128}
	s.Table("types")
	if q.Ipv4Column("ip", net.IP{192, 0, 2, 1}) != s || q.BinaryColumn("bin", payload) != s {
		t.Fatal("fluent identity changed")
	}
	payload[0] = 42 // Input must already have been copied.
	if err := s.AtNow(ctx); err != nil {
		t.Fatal(err)
	}
	s.Table("types")
	q.Ipv4Column("ip", nil)
	q.BinaryColumn("bin", []byte{})
	if err := s.AtNow(ctx); err != nil {
		t.Fatal(err)
	}
	s.Table("types")
	q.Ipv4Column("ip", net.ParseIP("198.51.100.2"))
	q.BinaryColumn("bin", nil)
	if err := s.AtNow(ctx); err != nil {
		t.Fatal(err)
	}
	// An omitted column must get a bitmap null too.
	if err := s.Table("types").AtNow(ctx); err != nil {
		t.Fatal(err)
	}
	issue68Wire(t, s, "ip", qwpTypeIPv4, []byte{1, 10, 1, 2, 0, 192, 2, 100, 51, 198})
	issue68Wire(t, s, "bin", qwpTypeBinary, []byte{1, 12, 0, 0, 0, 0, 3, 0, 0, 0, 3, 0, 0, 0, 0, 255, 128})
	flushAndAwaitAck(t, s)
	s.Table("types")
	q.Ipv4Column("ip", nil)
	q.BinaryColumn("bin", nil)
	if err := s.AtNow(ctx); err != nil {
		t.Fatal(err)
	}
	issue68Wire(t, s, "ip", qwpTypeIPv4, []byte{1, 1})
	issue68Wire(t, s, "bin", qwpTypeBinary, []byte{1, 1, 0, 0, 0, 0})
	s.Table("types")
	q.BinaryColumn("bin", []byte{7})
	if err := s.AtNow(ctx); err != nil {
		t.Fatal(err)
	}
	issue68Wire(t, s, "bin", qwpTypeBinary, []byte{1, 1, 0, 0, 0, 0, 1, 0, 0, 0, 7})
}

func TestQwpIssue68CancelAndValidation(t *testing.T) {
	ctx := context.Background()
	srv := newQwpTestServer(t)
	defer srv.Close()
	s := newQwpSenderForTest(t, srv.URL)
	defer s.Close(ctx)
	q := issue68API(t, s)
	for _, setter := range []func(){func() { q.Ipv4Column("ip", net.IPv4(1, 2, 3, 4)) }, func() { q.BinaryColumn("bin", []byte{1}) }} {
		setter()
		if err := s.AtNow(ctx); err == nil {
			t.Fatal("setter without Table accepted")
		}
	}
	s.Table("types")
	q.BinaryColumn("bin", []byte{1})
	if err := s.AtNow(ctx); err != nil {
		t.Fatal(err)
	}
	for _, ip := range []net.IP{net.ParseIP("2001:db8::1"), {}, {1, 2, 3}} {
		s.Table("types")
		q.BinaryColumn("bin", []byte{99, 100})
		q.Ipv4Column("ip", ip)
		if err := s.AtNow(ctx); err == nil {
			t.Fatalf("invalid IPv4 %v accepted", ip)
		}
	}
	s.Table("types")
	q.BinaryColumn("bin", []byte{2})
	if err := s.AtNow(ctx); err != nil {
		t.Fatal(err)
	}
	issue68Wire(t, s, "bin", qwpTypeBinary, []byte{0, 0, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 1, 2})
	for _, setter := range []func(){
		func() { q.BinaryColumn("bad.name", nil) },
		func() { q.Ipv4Column("bad.name", nil) },
		func() { q.Ipv4Column("bin", net.IPv4(1, 2, 3, 4)) },
		func() { q.BinaryColumn("bin", nil); q.BinaryColumn("bin", nil) },
		func() { q.Ipv4Column("ip", nil); q.Ipv4Column("ip", nil) },
	} {
		s.Table("types")
		setter()
		if err := s.AtNow(ctx); err == nil {
			t.Fatal("invalid column operation accepted")
		}
	}
	issue68Wire(t, s, "bin", qwpTypeBinary, []byte{0, 0, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 1, 2})
}

func TestQwpIssue68PooledLease(t *testing.T) {
	ctx := context.Background()
	pool := newQwpSenderPoolForTest(t, "", 1, 1)
	lease, err := pool.borrow(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Close(ctx)
	q := issue68API(t, lease)
	lease.Table("types")
	if q.Ipv4Column("ip", net.IPv4(192, 0, 2, 1)) != lease || q.BinaryColumn("bin", []byte{255}) != lease {
		t.Fatal("pool fluent identity changed")
	}
	if err := lease.AtNow(ctx); err != nil {
		t.Fatal(err)
	}
	delegate := lease.(*qwpPooledSender).slot.delegate.(*qwpLineSender)
	issue68Wire(t, delegate, "ip", qwpTypeIPv4, []byte{0, 1, 2, 0, 192})
	issue68Wire(t, delegate, "bin", qwpTypeBinary, []byte{0, 0, 0, 0, 0, 1, 0, 0, 0, 255})
	if err := lease.Close(ctx); err != nil {
		t.Fatal(err)
	}
	next, err := pool.borrow(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer next.Close(ctx)
	next.Table("types")
	q.BinaryColumn("stale_bin", []byte{1})
	q.Ipv4Column("stale_ip", net.IPv4(1, 2, 3, 4))
	if err := lease.AtNow(ctx); err == nil {
		t.Fatal("stale lease accepted")
	}
	if err := next.AtNow(ctx); err != nil {
		t.Fatal(err)
	}
	tb := next.(*qwpPooledSender).slot.delegate.(*qwpLineSender).tableBuffers["types"]
	for _, name := range []string{"stale_bin", "stale_ip"} {
		if _, ok := tb.columnIndex[name]; ok {
			t.Fatalf("stale lease wrote %s", name)
		}
	}
}
