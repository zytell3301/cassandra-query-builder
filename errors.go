package cassandraQB

import "errors"

var (
	ErrorEmptyValidData    = errors.New("no valid data supplied")
	ErrorPrimaryKeyMissing = errors.New("primary key field missing")
)
