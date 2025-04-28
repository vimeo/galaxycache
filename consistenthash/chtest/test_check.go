//go:build go1.23 && !testing_binary_chtest_can_panic_at_any_time

package chtest

import "testing"

func init() {
	if !testing.Testing() {
		panic("attempt to use galaxycache's consistenthash/chtest package outside a test")
	}
}
