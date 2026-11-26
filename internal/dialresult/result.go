// Package dialresult validates results from caller-provided dialers.
package dialresult

import (
	"errors"
	"net"
)

// ErrNoConn means a dialer returned neither a connection nor an error.
var ErrNoConn = errors.New("dial returned no connection")

// Check closes an unusable connection and enforces the dialer contract.
func Check(c net.Conn, err error) (net.Conn, error) {
	if err != nil {
		if c != nil {
			_ = c.Close()
		}
		return nil, err
	}
	if c == nil {
		return nil, ErrNoConn
	}
	return c, nil
}
