// SPDX-License-Identifier: MIT
// Copyright (c) Simple Container

package channel

import (
	"errors"
	"fmt"
)

// PermanentError marks a send failure that retrying cannot fix — a bad
// chat ID, a revoked token, a malformed URL.
//
// Without this distinction the delivery queue spends all five attempts
// and ~7 seconds of Lambda time on a 400 that was never going to
// succeed, and the operator sees "attempts: 5" for what is actually a
// configuration error. With it, the queue stamps giveUpAt immediately
// and the SPA can say which it was.
type PermanentError struct{ Err error }

func (e *PermanentError) Error() string { return "permanent: " + e.Err.Error() }
func (e *PermanentError) Unwrap() error { return e.Err }

// Permanent wraps err as non-retryable.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &PermanentError{Err: err}
}

// Permanentf wraps a formatted message as non-retryable.
func Permanentf(format string, args ...any) error {
	return &PermanentError{Err: fmt.Errorf(format, args...)}
}

// IsPermanent reports whether err (or anything it wraps) is permanent.
func IsPermanent(err error) bool {
	var p *PermanentError
	return errors.As(err, &p)
}

// RetryableFromStatus classifies an HTTP status the way the whole fleet
// already does: 4xx is permanent EXCEPT 429, which is the server asking
// us to come back later.
//
// Shared by both v1 channels so the Telegram and webhook paths cannot
// drift on the one classification that decides whether an operator's
// typo burns five attempts.
func RetryableFromStatus(status int) bool {
	if status == 429 {
		return true
	}
	return status < 400 || status >= 500
}
