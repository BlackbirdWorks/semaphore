// Package configsync lets the OFREP bulk evaluation answer a client that was notified of a newer configuration than
// the one the relay has loaded: the relay checks its control plane first, within a bound, and answers with what it
// then has. It never fails a request and never waits without a bound.
package configsync
