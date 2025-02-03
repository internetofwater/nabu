package common

import (
	"fmt"
	"path/filepath"
	"strings"

	log "github.com/sirupsen/logrus"
)

// MakeURN formats a URN following urn:{program}:{organization}:{provider}:{sha}
func MakeURN(s string) (string, error) {
	check := prefixTransform(s) // change "summoned" to "data" if summoned is in the object prefix
	if strings.Contains(check, "orgs/") {
		return fmt.Sprintf("urn:gleaner.io:%s:%s", "iow", check), nil
	} else {
		s3c, err := getLastThree(check)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("urn:gleaner.io:%s:%s", "iow", s3c), nil
	}
}

// MakeURNFromS3Prefix formats a URN following the ADR 0001-URN-decision.md  which at the
// time of this coding resulted in   urn:{engine}:{implnet}:{source}:{type}:{sha}
// the "prefix" version only returns the prefix part of the urn, for use in the prune
// command
func MakeURNFromS3Prefix(prefix string) (string, error) {

	if prefix == "orgs" {
		return fmt.Sprintf("urn:gleaner.io:%s:orgs", "iow"), nil
	} else {
		check := prefixTransform(prefix)
		ps := strings.Split(check, "/")
		if len(ps) < 2 {
			return "", fmt.Errorf("error in input prefix. You must have at least two / in the prefix")
		}
		return fmt.Sprintf("urn:gleaner.io:%s:%s:%s", "iow", ps[len(ps)-1], ps[len(ps)-2]), nil

	}
}

// prefixTransform  In this code, the prefix will be coming in with something like
// summoned or prov.  In our 0001-URN-decision.md document, we want the urn to be like
// urn:gleaner.io:oih:edmo:prov:0255293683036aac2a95a2479cc841189c0ac3f8
// or
// urn:gleaner.io:iow:counties0:data:00010f9f071c39fcc0ca73eccad7470b675cd8a3
// this means that the string "summoned" needs to be mapped to "data".  However,
// we use prov for both the path in the S3 and the URN structure.  So in this
// location we need to convert summoned to prov

// NOTE from Colton: this method seems unnecessary
func prefixTransform(str string) string {
	if !strings.Contains(str, "summoned/") {
		return str
	}

	return strings.Replace(str, "summoned/", "data/", -1)
}

// getLastThree
// split the string and take last two segments, but flip to match URN for ADR 0001-URN-decision.md
func getLastThree(s string) (string, error) {
	extension := filepath.Ext(s) // remove the extension regardless of what it is
	trimmedString := strings.TrimSuffix(s, extension)

	sr := strings.Replace(trimmedString, "/", ":", -1) // replace / with :
	parts := strings.Split(sr, ":")                    // Split the string on the ":" character.

	if len(parts) < 3 {
		return "", fmt.Errorf("error in urn formation trying to split on object prefix. Not enough slashes delimeters in %s", s)
	}

	lastThree := parts[len(parts)-3:] // Get the last three elements.

	//flip the last two elements
	index1 := 0
	index2 := 1

	// Ensure indices are within the array bounds
	if index1 >= 0 && index1 < len(lastThree) && index2 >= 0 && index2 < len(lastThree) {
		// Swap the elements
		lastThree[index1], lastThree[index2] = lastThree[index2], lastThree[index1]
	} else {
		log.Println("error in urn formation trying to flip indices on object prefix")
	}

	s2c := strings.Join(lastThree, ":")

	return s2c, nil
}
