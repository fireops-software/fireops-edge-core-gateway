package error

import "fmt"

type ErrInternal string

// Error implements error.
func (e ErrInternal) Error() string {
	return fmt.Sprintf("ErrInternal: %s", string(e))
}

func NewErrInternal(format string, args ...any) error {
	return ErrInternal(fmt.Sprintf(format, args...))
}
