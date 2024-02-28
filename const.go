package questdb

type tlsMode int64

const (
	tlsDisabled           tlsMode = 0
	tlsEnabled            tlsMode = 1
	tlsInsecureSkipVerify tlsMode = 2
)
