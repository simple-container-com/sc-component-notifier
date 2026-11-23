// SPDX-License-Identifier: MIT
// Copyright (c) Simple Container

// Package crypto seals channel credentials at rest.
//
// Scope: credentials an operator types into the SPA (a second Telegram
// chat, a webhook secret). Those must survive a redeploy, so they are
// persisted — and a bot token sitting in plaintext in an S3 object is a
// credential leak waiting for a bucket misconfiguration.
//
// DECLARATIVE credentials never come here. They arrive as
// `${secret:...}` references resolved at boot from the SC registry /
// SecretsManager and are held in memory only; persisting them would
// create a second copy of a secret that already has an owner, and the
// one in storage would silently go stale on rotation.
//
// The scheme is forge-conductor's and forge-notifier's, reimplemented
// from the scheme rather than copied: forge-notifier has no LICENSE file
// and is closed-source, and this repo is public MIT.
package crypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
)

// MinKEKBytes is the floor for the key-encryption key.
//
// The same 32-byte floor the JWT validator enforces, and for the same
// reason — it is the same operator-supplied `signingKey`. One secret,
// two uses: HS256 verification and the KEK below. That reuse is
// deliberate for a self-hostable component (one knob, not three), and
// it is safe because the KEK is never the signing key itself but an
// HMAC of it under a distinct label.
const MinKEKBytes = 32

// Sealer seals and opens credentials under a per-scope derived key.
type Sealer struct {
	kek []byte
}

// NewSealer builds a Sealer, or fails.
//
// It FAILS CLOSED on a short or empty KEK rather than falling back to
// storing plaintext. A notifier that boots with encryption silently
// disabled would write unsealed bot tokens to S3 and report healthy,
// and nobody would find out until the bucket was read by someone else.
// Refusing to boot is the louder and cheaper failure.
func NewSealer(kek []byte) (*Sealer, error) {
	if len(kek) < MinKEKBytes {
		return nil, fmt.Errorf("credential encryption key must be at least %d bytes, got %d", MinKEKBytes, len(kek))
	}
	return &Sealer{kek: kek}, nil
}

// dek derives the data-encryption key for one scope.
//
// The "secrets:" label namespaces this derivation away from any other
// use of the same KEK, so a future second derived key (signing cookies,
// say) cannot collide with it. The scope is typically a tenant or
// "channels"; distinct scopes get distinct keys, so a ciphertext cannot
// be moved between them.
func (s *Sealer) dek(scope string) []byte {
	mac := hmac.New(sha256.New, s.kek)
	mac.Write([]byte("secrets:" + scope))
	return mac.Sum(nil)
}

// Seal encrypts plaintext for scope. Output layout: nonce || ciphertext.
func (s *Sealer) Seal(scope, plaintext string) ([]byte, error) {
	if plaintext == "" {
		return nil, nil
	}
	gcm, err := s.gcm(scope)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("generate nonce: %w", err)
	}
	// The scope goes in as ADDITIONAL DATA, not just into the key. Both
	// would do on their own; together it means a ciphertext relabelled
	// to another scope fails to open with an authentication error rather
	// than decrypting to garbage.
	return gcm.Seal(nonce, nonce, []byte(plaintext), []byte(scope)), nil
}

// Open decrypts a sealed credential.
func (s *Sealer) Open(scope string, sealed []byte) (string, error) {
	if len(sealed) == 0 {
		return "", nil
	}
	gcm, err := s.gcm(scope)
	if err != nil {
		return "", err
	}
	if len(sealed) < gcm.NonceSize() {
		return "", errors.New("sealed credential is truncated")
	}
	nonce, ct := sealed[:gcm.NonceSize()], sealed[gcm.NonceSize():]
	out, err := gcm.Open(nil, nonce, ct, []byte(scope))
	if err != nil {
		// Deliberately vague to the caller's caller: a padding-oracle
		// style distinction between "wrong key" and "tampered" buys an
		// attacker something and buys the operator nothing. The SPA
		// renders this as "credential unreadable — re-enter it".
		return "", fmt.Errorf("credential could not be decrypted (wrong key or tampered)")
	}
	return string(out), nil
}

func (s *Sealer) gcm(scope string) (cipher.AEAD, error) {
	block, err := aes.NewCipher(s.dek(scope))
	if err != nil {
		return nil, fmt.Errorf("aes cipher: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("aes-gcm: %w", err)
	}
	return gcm, nil
}
