package upstream

import (
	"errors"
	"reflect"
)

// ErrNoAddress means a backend value has no network address: it is
// neither a string nor an Addresser, and no function was given to derive
// one. Packages health, upnet and uphttp return it for such pools.
var ErrNoAddress = errors.New("upstream: backend value has no address")

// An Addresser is a backend value that knows its network address, usually
// a host and port. Pools whose values carry more than an address, such as
// a struct with a zone or labels, implement it so that health checks,
// dialers and HTTP transports find the address without a function of
// their own.
type Addresser interface {
	// Address returns the backend's address, as for net.Dial.
	Address() string
}

// AddressOf returns the address of a backend value: the value itself if
// its type is a string type, or the result of its Address method if it is
// an Addresser. ok is false for other values and for a nil Addresser.
func AddressOf[T any](v T) (addr string, ok bool) {
	switch x := any(v).(type) {
	case string:
		return x, true
	case Addresser:
		rv := reflect.ValueOf(x)
		switch rv.Kind() {
		case reflect.Pointer, reflect.Map, reflect.Slice, reflect.Func, reflect.Chan:
			if rv.IsNil() {
				return "", false
			}
		default:
		}
		return x.Address(), true
	}
	if rv := reflect.ValueOf(v); rv.Kind() == reflect.String {
		return rv.String(), true
	}
	return "", false
}

// Addressable reports whether T is a string type or implements Addresser.
// Packages that derive addresses check it up front and fail with
// ErrNoAddress. AddressOf still checks the individual value for nil.
func Addressable[T any]() bool {
	t := reflect.TypeFor[T]()
	return t.Kind() == reflect.String || t.Implements(reflect.TypeFor[Addresser]())
}
