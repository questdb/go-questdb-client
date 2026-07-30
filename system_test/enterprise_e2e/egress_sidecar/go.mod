module github.com/questdb/go-questdb-client/v4/system_test/enterprise_e2e/egress_sidecar

go 1.23

require github.com/questdb/go-questdb-client/v4 v4.0.0

require (
	github.com/coder/websocket v1.8.14 // indirect
	github.com/klauspost/compress v1.18.4 // indirect
	golang.org/x/sys v0.16.0 // indirect
)

replace github.com/questdb/go-questdb-client/v4 => ../../..
