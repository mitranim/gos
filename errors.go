package gos

import (
	"database/sql"
	"errors"
	"fmt"
)

/*
Error codes. You probably shouldn't use this directly; instead, use the `Err`
variables with `errors.Is`.
*/
type ErrCode string

const (
	ErrCodeUnknown      ErrCode = ""
	ErrCodeNoRows       ErrCode = "ErrNoRows"
	ErrCodeMultipleRows ErrCode = "ErrMultipleRows"
	ErrCodeInvalidDest  ErrCode = "ErrInvalidDest"
	ErrCodeInvalidInput ErrCode = "ErrInvalidInput"
	ErrCodeNoColDest    ErrCode = "ErrNoColDest"
	ErrCodeRedundantCol ErrCode = "ErrRedundantCol"
	ErrCodeNull         ErrCode = "ErrNull"
	ErrCodeScan         ErrCode = "ErrScan"
)

/*
Use blank error variables to detect error types:

	if errors.Is(err, gos.ErrNoRows) {
		// Handle specific error.
	}

Note that errors returned by Gos can't be compared via `==` because they may
include additional details about the circumstances. When compared by
`errors.Is`, they compare `.Cause` and fall back on `.Code`.
*/
var (
	ErrNoRows       Err = Err{Code: ErrCodeNoRows, Cause: sql.ErrNoRows}
	ErrMultipleRows Err = Err{Code: ErrCodeMultipleRows, Cause: errors.New(`expected one row, got multiple`)}
	ErrInvalidDest  Err = Err{Code: ErrCodeInvalidDest, Cause: errors.New(`invalid destination`)}
	ErrInvalidInput Err = Err{Code: ErrCodeInvalidInput, Cause: errors.New(`invalid input`)}
	ErrNoColDest    Err = Err{Code: ErrCodeNoColDest, Cause: errors.New(`column has no matching destination`)}
	ErrRedundantCol Err = Err{Code: ErrCodeRedundantCol, Cause: errors.New(`redundant column occurrence`)}
	ErrNull         Err = Err{Code: ErrCodeNull, Cause: errors.New(`null column for non-nilable field`)}
	ErrScan         Err = Err{Code: ErrCodeScan, Cause: errors.New(`error while scanning row`)}
)

// Describes a Gos error.
type Err struct {
	Code  ErrCode
	While string
	Cause error
}

// Implement `error`.
func (self Err) Error() string {
	if self == (Err{}) {
		return ""
	}
	msg := `SQL error`
	if self.Code != ErrCodeUnknown {
		msg += fmt.Sprintf(` %s`, self.Code)
	}
	if self.While != "" {
		msg += fmt.Sprintf(` while %v`, self.While)
	}
	if self.Cause != nil {
		msg += `: ` + self.Cause.Error()
	}
	return msg
}

// Implement a hidden interface in "errors".
func (self Err) Is(other error) bool {
	if self.Cause != nil && errors.Is(self.Cause, other) {
		return true
	}
	err, ok := other.(Err)
	return ok && err.Code == self.Code
}

// Implement a hidden interface in "errors".
func (self Err) Unwrap() error {
	return self.Cause
}

func (self Err) while(while string) Err {
	self.While = while
	return self
}

func (self Err) because(cause error) Err {
	self.Cause = cause
	return self
}
