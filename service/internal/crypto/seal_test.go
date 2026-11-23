// SPDX-License-Identifier: MIT
// Copyright (c) Simple Container

package crypto

import (
	"bytes"
	"strings"
	"testing"
)

const testKEK = "a-fake-signing-key-that-is-at-least-32-bytes-long"

func newTestSealer(t *testing.T) *Sealer {
	t.Helper()
	s, err := NewSealer([]byte(testKEK))
	if err != nil {
		t.Fatalf("NewSealer: %v", err)
	}
	return s
}

func TestSealOpenRoundTrip(t *testing.T) {
	s := newTestSealer(t)

	sealed, err := s.Seal("channels", "123456:ABC-DEF-bot-token")
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	if bytes.Contains(sealed, []byte("ABC-DEF")) {
		t.Fatal("the plaintext token appears verbatim in the sealed bytes")
	}

	got, err := s.Open("channels", sealed)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if got != "123456:ABC-DEF-bot-token" {
		t.Fatalf("round trip got %q", got)
	}
}

// A short KEK must REFUSE rather than degrade. The alternative — a
// sealer that falls back to storing plaintext — would write unsealed bot
// tokens to S3 while /healthz reported fine.
func TestShortKEKIsRefused(t *testing.T) {
	for _, kek := range []string{"", "short", strings.Repeat("x", MinKEKBytes-1)} {
		if _, err := NewSealer([]byte(kek)); err == nil {
			t.Fatalf("NewSealer accepted a %d-byte key", len(kek))
		}
	}
	if _, err := NewSealer([]byte(strings.Repeat("x", MinKEKBytes))); err != nil {
		t.Fatalf("NewSealer rejected an exactly-%d-byte key: %v", MinKEKBytes, err)
	}
}

// The scope must be bound into the ciphertext, not just used to pick a
// key. Both the derived DEK and the GCM additional-data cover it; this
// asserts the net effect, which is that a ciphertext cannot be moved
// between scopes.
func TestScopeIsBoundToTheCiphertext(t *testing.T) {
	s := newTestSealer(t)

	sealed, err := s.Seal("channels", "secret-value")
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	if _, err := s.Open("other-scope", sealed); err == nil {
		t.Fatal("a ciphertext sealed for \"channels\" opened under \"other-scope\"")
	}
}

// The scope binding has TWO independent layers — the derived key and the
// GCM additional data — and the round-trip test above cannot tell them
// apart: breaking either one alone still fails to open. The next three
// tests pin each layer separately, so a mutation that removes one is
// caught rather than masked by the other.
func TestDEKDependsOnTheScope(t *testing.T) {
	s := newTestSealer(t)

	if bytes.Equal(s.dek("channels"), s.dek("other-scope")) {
		t.Fatal("two scopes derived the same key — the scope is not in the derivation")
	}
}

// Open must pass the scope as additional data, not merely derive the key
// from it. Sealed here under the RIGHT key but with no additional data,
// so only Open's own AAD argument can reject it.
func TestOpenPassesTheScopeAsAdditionalData(t *testing.T) {
	s := newTestSealer(t)

	gcm, err := s.gcm("channels")
	if err != nil {
		t.Fatalf("gcm: %v", err)
	}
	nonce := make([]byte, gcm.NonceSize())
	sealed := gcm.Seal(nonce, nonce, []byte("secret-value"), nil)

	if _, err := s.Open("channels", sealed); err == nil {
		t.Fatal("Open accepted a ciphertext that was not authenticated under the scope")
	}
}

// And the mirror: Seal must put the scope in, so a reader that does not
// supply it cannot open the result.
func TestSealPassesTheScopeAsAdditionalData(t *testing.T) {
	s := newTestSealer(t)

	sealed, err := s.Seal("channels", "secret-value")
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	gcm, err := s.gcm("channels")
	if err != nil {
		t.Fatalf("gcm: %v", err)
	}
	nonce, ct := sealed[:gcm.NonceSize()], sealed[gcm.NonceSize():]

	if _, err := gcm.Open(nil, nonce, ct, nil); err == nil {
		t.Fatal("a sealed credential opened without the scope as additional data")
	}
}

func TestWrongKEKCannotOpen(t *testing.T) {
	a := newTestSealer(t)
	b, err := NewSealer([]byte("a-DIFFERENT-key-that-is-at-least-32-bytes-long"))
	if err != nil {
		t.Fatalf("NewSealer: %v", err)
	}

	sealed, err := a.Seal("channels", "secret-value")
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	if _, err := b.Open("channels", sealed); err == nil {
		t.Fatal("a ciphertext opened under a different KEK")
	}
}

// GCM is authenticated; a flipped bit must fail rather than decrypt to
// garbage that then gets sent to Telegram as a bot token.
func TestTamperedCiphertextFails(t *testing.T) {
	s := newTestSealer(t)

	sealed, err := s.Seal("channels", "secret-value")
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	tampered := bytes.Clone(sealed)
	tampered[len(tampered)-1] ^= 0x01

	if _, err := s.Open("channels", tampered); err == nil {
		t.Fatal("a tampered ciphertext opened cleanly")
	}
}

func TestTruncatedCiphertextFails(t *testing.T) {
	s := newTestSealer(t)
	if _, err := s.Open("channels", []byte{0x01, 0x02}); err == nil {
		t.Fatal("a 2-byte \"ciphertext\" opened cleanly")
	}
}

// Sealing the same value twice must produce different bytes. A reused or
// absent nonce in GCM is catastrophic, and the symptom — two identical
// objects in the bucket — is otherwise invisible.
func TestNonceIsFresh(t *testing.T) {
	s := newTestSealer(t)

	first, err := s.Seal("channels", "same-value")
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	second, err := s.Seal("channels", "same-value")
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	if bytes.Equal(first, second) {
		t.Fatal("two seals of the same plaintext produced identical bytes — the nonce is not random")
	}
}

// An empty credential seals to nil, not to a 28-byte envelope of
// nothing. ChannelRecord.HasCredential() is len(Credential) > 0, so a
// non-nil seal of "" would make a channel with no token report that it
// has one — and the SPA would show a configured channel that cannot
// send.
func TestEmptyPlaintextSealsToNil(t *testing.T) {
	s := newTestSealer(t)

	sealed, err := s.Seal("channels", "")
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	if len(sealed) != 0 {
		t.Fatalf("empty plaintext sealed to %d bytes, want 0", len(sealed))
	}

	got, err := s.Open("channels", nil)
	if err != nil {
		t.Fatalf("Open(nil): %v", err)
	}
	if got != "" {
		t.Fatalf("Open(nil) returned %q", got)
	}
}

// The error must not distinguish "wrong key" from "tampered". That
// distinction is a side channel for an attacker and tells the operator
// nothing they can act on differently.
func TestOpenErrorDoesNotLeakTheReason(t *testing.T) {
	s := newTestSealer(t)
	sealed, err := s.Seal("channels", "secret-value")
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	tampered := bytes.Clone(sealed)
	tampered[len(tampered)-1] ^= 0x01

	_, err = s.Open("channels", tampered)
	if err == nil {
		t.Fatal("expected an error")
	}
	// Must not echo the plaintext or the key either.
	for _, leak := range []string{"secret-value", testKEK} {
		if strings.Contains(err.Error(), leak) {
			t.Fatalf("error leaks %q: %v", leak, err)
		}
	}
}
