// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package source

type SourceIndex interface {
	GetSources() []Source
}

type Source interface {
}
