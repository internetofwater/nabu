// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package common

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"fmt"
	"io"
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

	// we have to use a reader and not a scanner since our triples
	// could be extremely large and we don't want to allocate a buffer
	reader := bufio.NewReader(strings.NewReader(nq))

	// Maps blank nodes to all lines they appear in
	blankNodeToTriples := make(map[string][]string)

	for {
		line, err := reader.ReadString('\n')
		if err != nil && err != io.EOF {
			return "", err
		}

		line = strings.TrimSpace(line)
		if line == "" {
			if err == io.EOF {
				break
			}
			continue
		}

		split := strings.Split(line, " ")
		const minTripleParts = 3
		if len(split) < minTripleParts {
			return "", fmt.Errorf("triple must have at least 3 parts, unexpectedly got: '%s'", line)
		}

		subject := split[0]
		predicate := split[1]
		object := split[2]

		if strings.HasPrefix(subject, "_:") {
			blankNodeToTriples[subject] = append(blankNodeToTriples[subject], predicate+object)
		}
		if strings.HasPrefix(object, "_:") {
			blankNodeToTriples[object] = append(blankNodeToTriples[object], subject+predicate)
		}

		if err == io.EOF {
			break
		}
	}

	// Build deterministic hash replacements
	blankNodeToIRI := make(map[string]string)

	for blankNode, associatedTriples := range blankNodeToTriples {
		sort.Strings(associatedTriples)
		joined := strings.Join(associatedTriples, "\n")

		hash := sha256.New()
		hash.Write([]byte(joined))
		// we want to use a deterministic hash for
		// skolemization so that the same triples have the
		// same skolemized iri even if they are present in
		// the file in a different order; this allows for
		// a more consistent hash checks on the entire file
		// i.e. when we take the bytesum hash of the entire
		// file, consistent iris will allow for a consistent
		// bytesum hash of the entire file
		hashOfAllTriplesWithTheSameBlankNode := fmt.Sprintf("%x", hash.Sum(nil))
		iri := fmt.Sprintf("<https://docs.geoconnex.us/nqhash/%s>", hashOfAllTriplesWithTheSameBlankNode)

		blankNodeToIRI[blankNode] = iri
	}

	// Replace all blank nodes in the file with their deterministic IRI
	fileBytes := []byte(nq)
	for blank, iri := range blankNodeToIRI {
		fileBytes = bytes.ReplaceAll(fileBytes, []byte(blank+" "), []byte(iri+" "))
		fileBytes = bytes.ReplaceAll(fileBytes, []byte(blank+" ."), []byte(iri+" ."))
	}

	return string(fileBytes), nil
}
