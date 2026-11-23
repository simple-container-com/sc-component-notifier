// SPDX-License-Identifier: MIT
// Copyright (c) Simple Container

package channel

import (
	"net/http"
	"time"
)

// UserAgent is sent on EVERY outbound request, and it impersonates a
// browser on purpose.
//
// Cloudflare answers the default Go / Python-urllib user agent with a
// 403 and error code 1010 ("the owner of this website has banned your
// access based on your browser's signature"). forge-notifier lost real
// webhook deliveries to this, and the failure is maximally confusing: a
// well-formed request to a correct URL, rejected by an intermediary the
// operator does not know is in the path, with a body that mentions
// browsers rather than bots.
//
// Keep a real browser token in here. A string like "sc-notifier/1.0" is
// exactly what gets blocked.
const UserAgent = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 " +
	"(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 sc-component-notifier"

// SendTimeout bounds one delivery attempt.
//
// 15 seconds, not 3, and the difference is a scar. forge-notifier used a
// 3-second timeout on a Lambda-to-Lambda POST; a cold start on the
// receiving end takes longer than that, so real deliveries were lost to
// a timeout that looked like the destination being down. Anything in
// this fleet calling another Lambda must budget for a cold start.
const SendTimeout = 15 * time.Second

// NewHTTPClient returns the client every channel implementation should
// use for outbound delivery.
func NewHTTPClient() *http.Client {
	return &http.Client{Timeout: SendTimeout}
}
