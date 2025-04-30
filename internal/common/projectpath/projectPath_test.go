// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package projectpath

import (
	"os"
	"path/filepath"
	"testing"
)

func TestProjectPath(t *testing.T) {
	mainGo := filepath.Join(Root, "main.go")
	if _, err := os.Stat(mainGo); os.IsNotExist(err) {
		t.Fatalf("main.go does not exist at %s; the project path does not seem to point to the root of the repo", mainGo)
	}
}
