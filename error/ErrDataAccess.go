package error

import "fmt"

type ErrDataAccess string

// Error implements [error].
func (e ErrDataAccess) Error() string {
	return fmt.Sprintf("ErrDataAccess: %s", string(e))
}

func NewErrDataAccess(format string, args ...any) error {
	return ErrDataAccess(fmt.Sprintf(format, args...))
}
