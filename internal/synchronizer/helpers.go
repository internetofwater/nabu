package synchronizer

// returns the elements in `a` that aren't in `b`.
func difference(a, b []string) []string {
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

// findMissing returns a slice of strings representing the elements in a that are
// not present in b.
func findMissing(a, b []string) []string {
	// Create a map to store the elements of ga.
	gaMap := make(map[string]bool)
	for _, s := range b {
		gaMap[s] = true
	}

	// Iterate through a and add any elements that are not in b to the result slice.
	var result []string
	for _, s := range a {
		if !gaMap[s] {
			result = append(result, s)
		}
	}

	return result
}
