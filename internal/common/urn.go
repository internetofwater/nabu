// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package common

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"fmt"
	"strings"

	log "github.com/sirupsen/logrus"
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

	blankNodesToReplacement := make(map[string]string) // make a map here to hold our updated strings

	for scanner.Scan() {
		split := strings.Split(scanner.Text(), " ")
		const subjectPredicateObject = 3
		if len(split) < subjectPredicateObject {
			return "", fmt.Errorf("unexpected nq triple: %s", scanner.Text())
		}

		// use the hash of each subject, predicate, and object as
		// the iri unique identifier
		hash := sha256.New()
		// hash.Write([]byte(text))
		// hashString := fmt.Sprintf("%x", hash.Sum(nil))

		subject := split[0]
		predicate := split[1]
		object := split[2]

		blankSubjectNode := strings.HasPrefix(subject, "_:")

		if blankSubjectNode {
			if _, ok := blankNodesToReplacement[subject]; ok {
			} else {
				hash.Write([]byte(predicate))
				hash.Write([]byte(object))
				hashString := fmt.Sprintf("%x", hash.Sum(nil))
				newSubject := fmt.Sprintf("<https://iow.io/nqhash/%s>", hashString)
				blankNodesToReplacement[subject] = newSubject
			}
		}

		blankObjectNode := strings.HasPrefix(object, "_:")
		if blankObjectNode {
			if _, ok := blankNodesToReplacement[object]; ok {
			} else {
				hash.Write([]byte(subject))
				hash.Write([]byte(predicate))
				hashString := fmt.Sprintf("%x", hash.Sum(nil))
				newObject := fmt.Sprintf("<https://iow.io/nqhash/%s>", hashString)
				blankNodesToReplacement[object] = newObject
			}
		}
	}

	err := scanner.Err()
	if err != nil {
		log.Error(err)
		return "", err
	}

	filebytes := []byte(nq)

	for k, v := range blankNodesToReplacement {
		// have to add padding so we don't replace parts of other nodes
		oldNodeWithPadding := k + " "
		newNodeWithPadding := v + " "
		filebytes = bytes.ReplaceAll(filebytes, []byte(oldNodeWithPadding), []byte(newNodeWithPadding))
	}

	return string(filebytes), err
}
