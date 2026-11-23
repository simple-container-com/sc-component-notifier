// SPDX-License-Identifier: MIT
// Copyright (c) Simple Container

package notifier

import (
	"testing"

	. "github.com/onsi/gomega"

	"github.com/simple-container-com/api/pkg/api"
)

// init() must register every type token it owns both as a config reader
// (so SC core can parse the YAML) AND as a PluginRequirement OwnedType
// (so fool-protection auto-inject reaches it). Mismatches are the one
// silent gap this test exists to catch.
func TestInitRegistersEveryOwnedType(t *testing.T) {
	RegisterTestingT(t)

	req, ok := api.LookupPluginRequirement(ProviderType)
	Expect(ok).To(BeTrue(), "ProviderType %q must appear in RegisterPluginRequirement's OwnedTypes", ProviderType)
	Expect(req.PluginName).To(Equal("notifier"))
	Expect(req.DefaultSource.Type).To(Equal("github-release"))
	Expect(req.DefaultSource.Repository).To(Equal("simple-container-com/sc-component-notifier"))

	// Every token the template exports must be lookup-able.
	for _, token := range []string{ProviderType, ResourceType} {
		_, ok := api.LookupPluginRequirement(token)
		Expect(ok).To(BeTrue(), "token %q is not in OwnedTypes — fool-protection will silently skip server.yamls that only use this type", token)
	}
}

// Config validation surfaces a clear, named error for each required field.
func TestNotifierConfigValidate(t *testing.T) {
	RegisterTestingT(t)

	err := (&NotifierConfig{}).Validate()
	Expect(err).To(HaveOccurred())
	Expect(err.Error()).To(ContainSubstring("`deployTo` is required"))

	err = (&NotifierConfig{DeployTo: "lambda"}).Validate()
	Expect(err).To(HaveOccurred())
	Expect(err.Error()).To(ContainSubstring("`image` is required"))

	err = (&NotifierConfig{DeployTo: "lambda", Image: "simplecontainer/sc-component-notifier:latest"}).Validate()
	Expect(err).To(HaveOccurred())
	Expect(err.Error()).To(ContainSubstring("`signingKey` is required"))

	ok := &NotifierConfig{
		DeployTo:   "lambda",
		Image:      "simplecontainer/sc-component-notifier:latest",
		SigningKey: "${secret:notifier-service-jwt-key}",
	}
	Expect(ok.Validate()).To(Succeed())
}
