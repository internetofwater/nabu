// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package projectpath

import (
	"path/filepath"
	"runtime"
)

// The nabu config uses relative paths but we need to make sure that path is relative to the root
// at runtime so we can run tests with a relative path across the entire project
// Root allows us to get this info
var (
	_, b, _, _ = runtime.Caller(0)

	// Root folder of this project
	Root = filepath.Join(filepath.Dir(b), "../../..")
)
