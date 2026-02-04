# Copyright 2026 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

FROM busybox

# This is a very simple docker container that does nothing but output jsonld to stdout.
# it is used to mock the output of a bulk sitemap container

CMD ["echo", "{ \"@context\":[{ \"schema\":\"https://schema.org/\",\"gsp\":\"http://www.opengis.net/ont/geosparql#\",\"type\":\"@type\"}],\"type\":\"schema:Place\",\"@id\":\"https://api.wwdh.internetofwater.app/collections/noaa-rfc/items/AFPU1\",\"schema:name\":\"American Fork - American Fork  Nr  Up Pwrplnt  Abv\",\"gsp:hasGeometry\":{\"@type\":\"http://www.opengis.net/ont/sf#Point\",\"gsp:asWKT\":{\"@type\":\"http://www.opengis.net/ont/geosparql#wktLiteral\",\"@value\":\"POINT (-111.681389 40.447778)\"}},\"schema:geo\":{\"@type\":\"schema:GeoCoordinates\",\"schema:longitude\":-111.681389,\"schema:latitude\":40.447778},\"schema:subjectOf\":[{\"@type\":\"schema:Dataset\",\"schema:name\":\"American Fork - American Fork  Nr  Up Pwrplnt  Abv\",\"schema:description\":\"A ensemble streamflow forecast prediction for Amer\"}] }"]
