module github.com/questdb/go-questdb-client/v4/bench/qwp-egress-read-wide

go 1.23

toolchain go1.24.4

require (
	github.com/jackc/pgx/v5 v5.7.1
	github.com/questdb/go-questdb-client/v4 v4.0.0
)

require (
	github.com/coder/websocket v1.8.14 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/klauspost/compress v1.17.0 // indirect
	golang.org/x/crypto v0.27.0 // indirect
	golang.org/x/text v0.18.0 // indirect
)

replace github.com/questdb/go-questdb-client/v4 => ../..
