/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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
	"context"
	"math/big"
	"time"
)

// LineSender allows you to insert rows into QuestDB by sending ILP
// messages.
//
// Each sender corresponds to a single client-server connection.
// A sender should not be called concurrently by multiple goroutines.
type LineSender interface {
	// Table sets the table name (metric) for a new ILP message. Should be
	// called before any Symbol or Column method.
	//
	// Table name cannot contain any of the following characters:
	// '\n', '\r', '?', ',', ”', '"', '\', '/', ':', ')', '(', '+', '*',
	// '%', '~', starting '.', trailing '.', or a non-printable char.
	Table(name string) LineSender

	// Symbol adds a symbol column value to the ILP message. Should be called
	// before any Column method.
	//
	// Symbol name cannot contain any of the following characters:
	// '\n', '\r', '?', '.', ',', ”', '"', '\\', '/', ':', ')', '(', '+',
	// '-', '*' '%%', '~', or a non-printable char.
	Symbol(name, val string) LineSender

	// Int64Column adds a 64-bit integer (long) column value to the ILP
	// message.
	//
	// Column name cannot contain any of the following characters:
	// '\n', '\r', '?', '.', ',', ”', '"', '\\', '/', ':', ')', '(', '+',
	// '-', '*' '%%', '~', or a non-printable char.
	Int64Column(name string, val int64) LineSender

	// Long256Column adds a 256-bit unsigned integer (long256) column
	// value to the ILP message.
	//
	// Only non-negative numbers that fit into 256-bit unsigned integer are
	// supported and any other input value would lead to an error.
	//
	// Column name cannot contain any of the following characters:
	// '\n', '\r', '?', '.', ',', ”', '"', '\\', '/', ':', ')', '(', '+',
	// '-', '*' '%%', '~', or a non-printable char.
	Long256Column(name string, val *big.Int) LineSender

	// TimestampColumn adds a timestamp column value to the ILP
	// message.
	//
	// Column name cannot contain any of the following characters:
	// '\n', '\r', '?', '.', ',', ”', '"', '\\', '/', ':', ')', '(', '+',
	// '-', '*' '%%', '~', or a non-printable char.
	TimestampColumn(name string, ts time.Time) LineSender

	// Float64Column adds a 64-bit float (double) column value to the ILP
	// message.
	//
	// Column name cannot contain any of the following characters:
	// '\n', '\r', '?', '.', ',', ”', '"', '\', '/', ':', ')', '(', '+',
	// '-', '*' '%%', '~', or a non-printable char.
	Float64Column(name string, val float64) LineSender

	// StringColumn adds a string column value to the ILP message.
	//
	// Column name cannot contain any of the following characters:
	// '\n', '\r', '?', '.', ',', ”', '"', '\', '/', ':', ')', '(', '+',
	// '-', '*' '%%', '~', or a non-printable char.
	StringColumn(name, val string) LineSender

	// BoolColumn adds a boolean column value to the ILP message.
	//
	// Column name cannot contain any of the following characters:
	// '\n', '\r', '?', '.', ',', ”', '"', '\', '/', ':', ')', '(', '+',
	// '-', '*' '%%', '~', or a non-printable char.
	BoolColumn(name string, val bool) LineSender

	// At sets the timestamp in Epoch nanoseconds and finalizes
	// the ILP message.
	//
	// If the underlying buffer reaches configured capacity or the
	// number of buffered messages exceeds the auto-flush trigger, this
	// method also sends the accumulated messages.
	//
	// If ts.IsZero(), no timestamp is sent to the server.
	At(ctx context.Context, ts time.Time) error

	// AtNow omits the timestamp and finalizes the ILP message.
	// The server will insert each message using the system clock
	// as the row timestamp.
	//
	// If the underlying buffer reaches configured capacity or the
	// number of buffered messages exceeds the auto-flush trigger, this
	// method also sends the accumulated messages.
	AtNow(ctx context.Context) error

	// Flush sends the accumulated messages via the underlying
	// connection. Should be called periodically to make sure that
	// all messages are sent to the server.
	//
	// For optimal performance, this method should not be called after
	// each ILP message. Instead, the messages should be written in
	// batches followed by a Flush call. The optimal batch size may
	// vary from one thousand to few thousand messages depending on
	// the message size.
	Flush(ctx context.Context) error

	// Close closes the underlying HTTP client.
	//
	// If auto-flush is enabled, the client will flush any remaining buffered
	// messages before closing itself.
	Close(ctx context.Context) error
}
