// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package synchronizer

import (
	"fmt"
	"path"
	"slices"
	"strings"
)

// returns the elements in `a` that aren't in `b`.
func findMissing(a, b []string) []string {
	mb := make(map[string]struct{}, len(b))
	for _, x := range b {
		mb[x] = struct{}{}
	}
	var diff []string
	for _, x := range a {
		if _, found := mb[x]; !found {
			diff = append(diff, x)
		}
	}
	return diff
}

func getTextBeforeDot(s string) string {
	n := strings.LastIndexByte(s, '.')
	if n == -1 {
		return s
	}
	return s[:n]
}

// given a prefix, return the name of the release graph
// that represents it
func makeReleaseNqName(prefix string) (string, error) {
	prefix_parts := strings.Split(prefix, "/")
	if len(prefix_parts) <= 1 {
		return "", fmt.Errorf("prefix %s did not contain a slash and thus is ambiguous", prefix)
	}
	// i.e. summoned/counties0 would become counties0
	prefix_path_as_filename := getTextBeforeDot(path.Base(strings.Join(prefix_parts[1:], "_")))

	var release_nq_name string
	if slices.Contains(prefix_parts, "summoned") && prefix_path_as_filename != "" {
		release_nq_name = fmt.Sprintf("%s_release.nq", prefix_path_as_filename) // ex: counties0_release.nq
	} else if slices.Contains(prefix_parts, "prov") && prefix_path_as_filename != "" {
		release_nq_name = fmt.Sprintf("%s_prov.nq", prefix_path_as_filename) // ex: counties0_prov.nq
	} else if slices.Contains(prefix_parts, "orgs") {
		if prefix_path_as_filename == "" {
			release_nq_name = "organizations.nq"
		} else {
			release_nq_name = fmt.Sprintf("%s_organizations.nq", prefix_path_as_filename)
		}
	} else {
		return "", fmt.Errorf("unable to form a release graph name from prefix %s", prefix)
	}
	return release_nq_name, nil
}
