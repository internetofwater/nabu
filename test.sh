#!/bin/bash

# URL and headers
URL="https://geoconnex.us/ref/hu04/0316"
HEADERS=(
  -H "Want-Content-Digest: sha256"
  -H "Accept: application/json+ld"
)

# Fetch full content and headers
RESPONSE_FILE=$(mktemp)
HEADER_FILE=$(mktemp)

curl -sSL -D "$HEADER_FILE" "${HEADERS[@]}" "$URL" -o "$RESPONSE_FILE"

cat $RESPONSE_FILE | head

# Extract the sha256 hash from headers
EXPECTED_HASH=$(grep -i 'content-digest:' "$HEADER_FILE" | sed -n 's/.*sha256=\(.*\)/\1/p')

# Compute actual sha256 hash of the content
ACTUAL_HASH=$(sha256sum "$RESPONSE_FILE" | awk '{print $1}')

EXPECTED_HASH="${EXPECTED_HASH#"${EXPECTED_HASH%%[![:space:]]*}"}"  # trim leading space
EXPECTED_HASH="${EXPECTED_HASH%"${EXPECTED_HASH##*[![:space:]]}"}"  # trim trailing space


# Clean up temp files
rm -f "$HEADER_FILE" "$RESPONSE_FILE"

# Print results
echo "Expected: $EXPECTED_HASH"
echo "Actual  : $ACTUAL_HASH"

# Compare hashes
if [[ "$EXPECTED_HASH" == "$ACTUAL_HASH" ]]; then
  echo "✅ Hashes match."
else
  echo "❌ Hashes do not match!"
fi
