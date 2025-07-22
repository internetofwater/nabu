// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package common

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"fmt"
	"sort"
	"strings"
)

/*
This file represents all operations for defining a URN

A URN can be used to identify either a graph or a
prefix path in S3. Thus you can convert between
the two to perform synch operations
*/

const baseURN = "urn:iow"

type URN = string

// Map a s3 prefix to a URN
// This is essentially just a serialized path that can be used for identifying a graph
// We use a simple
func MakeURN(s3Prefix string) (URN, error) {
	if s3Prefix == "" || s3Prefix == "." {
		return "", fmt.Errorf("prefix cannot be empty")
	} else if !strings.Contains(s3Prefix, "/") {
		return "", fmt.Errorf("prefix must contain at least one '/'")
	} else if strings.Contains(s3Prefix, "//") {
		return "", fmt.Errorf("prefix cannot contain double slashes")
	}

	resultURN := baseURN
	splitOnSlash := strings.Split(s3Prefix, "/")
	for _, part := range splitOnSlash {
		if part == "" {
			break
		}
		resultURN += ":" + part
	}
	return resultURN, nil
}

// Skolemization replaces blank nodes with URIs  The mapping approach is needed since this
// function can be used on a whole data graph, not just a single triple
// reference: https://www.w3.org/TR/rdf11-concepts/#dfn-skolem-iri
func Skolemization(nq string) (string, error) {
	scanner := bufio.NewScanner(strings.NewReader(nq))

	// Maps blank nodes to all lines they appear in
	blankNodeToTriples := make(map[string][]string)

	for scanner.Scan() {
		line := scanner.Text()

		split := strings.Split(line, " ")
		const minTripleParts = 3
		if len(split) < minTripleParts {
			return "", fmt.Errorf("triple must have at least 3 parts, unexpectedly got: '%s'", line)
		}

		subject := split[0]
		predicate := split[1]
		object := split[2]

		// add only the parts of the triple that don't contain the blank node
		// this is since the blank node is non deterministic and could be named
		// something like _:b0 or _:b1 or _:b2 etc depending on the jsonld processor
		if strings.HasPrefix(subject, "_:") {
			blankNodeToTriples[subject] = append(blankNodeToTriples[subject], predicate+object)
		}
		if strings.HasPrefix(object, "_:") {
			blankNodeToTriples[object] = append(blankNodeToTriples[object], subject+predicate)
		}
	}

	if err := scanner.Err(); err != nil {
		return "", err
	}

	// Build deterministic hash replacements
	// This will give us the same IRI for each blank node
	// regardless of where it was in order in the file
	blankNodeToIRI := make(map[string]string)

	for blankNode, associatedTriples := range blankNodeToTriples {
		sort.Strings(associatedTriples) // ensure deterministic order
		joined := strings.Join(associatedTriples, "\n")

		hash := sha256.New()
		hash.Write([]byte(joined))
		hashOfAllTriplesWithTheSameBlankNode := fmt.Sprintf("%x", hash.Sum(nil))
		iri := fmt.Sprintf("<https://iow.io/nqhash/%s>", hashOfAllTriplesWithTheSameBlankNode)

		blankNodeToIRI[blankNode] = iri
	}

	// Replace all blank nodes in the file with their deterministic IRI
	fileBytes := []byte(nq)
	for blank, iri := range blankNodeToIRI {
		// Replace blank node followed by space
		fileBytes = bytes.ReplaceAll(fileBytes, []byte(blank+" "), []byte(iri+" "))
		// Replace blank node followed by period
		fileBytes = bytes.ReplaceAll(fileBytes, []byte(blank+" ."), []byte(iri+" ."))
	}

	return string(fileBytes), nil
}
