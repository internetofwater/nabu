// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package projectpath

import (
	"os"
	"path/filepath"
	"testing"
)

func TestProjectPath(t *testing.T) {
	licensePath := filepath.Join(Root, "LICENSE")
	if _, err := os.Stat(licensePath); os.IsNotExist(err) {
		t.Fatalf("LICENSE does not exist at %s; the project path does not seem to point to the root of the repo", licensePath)
	}
}
