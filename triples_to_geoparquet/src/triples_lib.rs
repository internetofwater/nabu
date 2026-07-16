// Copyright 2026 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

use std::{collections::HashMap, io::BufRead};

use log::{debug, warn};
use oxttl::NQuadsParser;

use geo_types::Geometry;
use wkt::TryFromWkt;

fn parse_wkt_from_triple_string(triple_node: &str) -> Result<Geometry, Box<dyn std::error::Error>> {
    let part = triple_node
        .splitn(2, "^^")
        .next()
        .ok_or(format!("Invalid WKT string: {}", triple_node))?
        .strip_prefix('"')
        .ok_or(format!("Invalid WKT string: {}", triple_node))?
        .strip_suffix('"')
        .ok_or(format!("Invalid WKT string: {}", triple_node))?;

    debug!("Parsed WKT: {}", part.to_string());

    Ok(Geometry::try_from_wkt_str(&part.to_string())?)
}

pub fn read_triples_into_maps<R: BufRead>(
    triples_reader: R,
) -> Result<HashMaps, Box<dyn std::error::Error>> {
    // there are two ways to encode geometries in nquads: either as WKT or as a schema.org latitude/longitude pair
    let mut pid_to_geosparql_skolemization_id: HashMap<String, String> = HashMap::new();
    let mut geosparql_skolemization_id_to_geometry: HashMap<String, Geometry> = HashMap::new();

    let mut pid_to_schema_name: HashMap<String, String> = HashMap::new();
    let mut pid_to_schema_description: HashMap<String, String> = HashMap::new();
    let mut pid_to_referenced_position_ids: HashMap<String, Vec<String>> = HashMap::new();
    let mut referenced_position_id_to_indirect_position_id: HashMap<String, String> =
        HashMap::new();
    let mut indirect_position_id_to_linear_element_uri: HashMap<String, String> = HashMap::new();

    let parser = NQuadsParser::new().with_max_buffer_size(usize::MAX);
    let parsed_quads = parser.for_reader(triples_reader);

    let mut total_parse_errors = 0;
    const MAX_PARSE_ERRORS: u32 = 50;
    for quad in parsed_quads {
        if total_parse_errors > MAX_PARSE_ERRORS {
            warn!("Too many errors when parsing quads, returning early");
            return Err("Too many errors when parsing quads".into());
        }
        let quad = match quad {
            Ok(quad) => quad,
            Err(err) => {
                warn!("Error parsing triple: {}", err);
                total_parse_errors += 1;
                continue;
            }
        };
        let subject = quad.subject;
        let predicate = quad.predicate;
        let object = quad.object;

        let predicate_str = predicate.to_string();
        match predicate_str.clone().as_str() {
            "<http://www.opengis.net/ont/geosparql#hasGeometry>" => {
                debug!("Found gsp geometry: {}", object.to_owned().to_string());
                pid_to_geosparql_skolemization_id.insert(
                    subject.to_owned().to_string(),
                    object.to_owned().to_string(),
                );
            }
            "<http://www.opengis.net/ont/geosparql#asWKT>" => {
                debug!("Found WKT: {}", object.to_owned().to_string());
                let object_string = object.to_owned().to_string();

                let geometry = parse_wkt_from_triple_string(&object_string)?;
                geosparql_skolemization_id_to_geometry
                    .insert(subject.to_owned().to_string(), geometry);
            }

            "<https://schema.org/description>" => {
                pid_to_schema_description.insert(
                    subject.to_owned().to_string(),
                    object.to_owned().to_string(),
                );
            }
            "<https://schema.org/name>" => {
                pid_to_schema_name.insert(
                    subject.to_owned().to_string(),
                    object.to_owned().to_string(),
                );
            }
            "<https://www.opengis.net/def/schema/hy_features/hyf/referencedPosition>" => {
                pid_to_referenced_position_ids
                    .entry(subject.to_owned().to_string())
                    .or_default()
                    .push(object.to_owned().to_string());
            }
            "<https://www.opengis.net/def/schema/hy_features/hyf/HY_IndirectPosition>" => {
                referenced_position_id_to_indirect_position_id.insert(
                    subject.to_owned().to_string(),
                    object.to_owned().to_string(),
                );
            }
            "<https://www.opengis.net/def/schema/hy_features/hyf/linearElement>" => {
                indirect_position_id_to_linear_element_uri.insert(
                    subject.to_owned().to_string(),
                    object.to_owned().to_string(),
                );
            }
            &_ => {}
        }
    }

    let mut pid_to_mainstem_uri: HashMap<String, String> = HashMap::new();
    for (pid, referenced_position_ids) in &pid_to_referenced_position_ids {
        for referenced_position_id in referenced_position_ids {
            let Some(indirect_position_id) =
                referenced_position_id_to_indirect_position_id.get(referenced_position_id)
            else {
                continue;
            };
            let Some(linear_element_uri) =
                indirect_position_id_to_linear_element_uri.get(indirect_position_id)
            else {
                continue;
            };
            if linear_element_uri.contains("geoconnex.us/ref/mainstems/") {
                pid_to_mainstem_uri.insert(pid.to_owned(), linear_element_uri.to_owned());
                break;
            }
        }
    }

    Ok(HashMaps {
        pid_to_geosparql_skolemization_id,
        geosparql_skolemization_id_to_geometry,
        pid_to_schema_description,
        pid_to_schema_name,
        pid_to_mainstem_uri,
    })
}

// A container struct for all hash maps; used for merging together
// triple ids with their associated geometries
pub struct HashMaps {
    pub pid_to_geosparql_skolemization_id: HashMap<String, String>,
    pub geosparql_skolemization_id_to_geometry: HashMap<String, Geometry>,
    pub pid_to_schema_description: HashMap<String, String>,
    pub pid_to_schema_name: HashMap<String, String>,
    pub pid_to_mainstem_uri: HashMap<String, String>,
}

/// Given info for both the geosparql and schema geo representations of a geometry,
/// combine them into a single canonical representation for each pid and return
/// the associated hashmap
pub fn combine_geometry_representations(
    maps: &HashMaps,
) -> Result<HashMap<String, Geometry>, Box<dyn std::error::Error>> {
    let mut pid_to_canonical_geometry: HashMap<String, Geometry> = HashMap::new();

    // first we go through and get all the geosparql geometry;
    // this is the ideal canonical representation since wkt is more flexible
    // than just a point
    for (pid, geosparql_skolemization_id) in &maps.pid_to_geosparql_skolemization_id {
        match maps
            .geosparql_skolemization_id_to_geometry
            .get(geosparql_skolemization_id)
        {
            Some(geometry) => {
                pid_to_canonical_geometry.insert(pid.to_owned(), geometry.clone());
            }
            None => {
                return Err(format!(
                    "Could not find geosparql geometry for pid {} with id {}",
                    pid, geosparql_skolemization_id
                )
                .into());
            }
        }
    }

    Ok(pid_to_canonical_geometry)
}

#[cfg(test)]
mod tests {
    use geo_types::{Geometry, Point};

    use std::collections::HashMap;

    use crate::triples_lib::{combine_geometry_representations, HashMaps};

    #[test]
    fn test_combine_geometry_representations() {
        let mut pids_to_gsp_skolemization: HashMap<String, String> = HashMap::new();
        pids_to_gsp_skolemization.insert("1".to_string(), "2".to_string());
        let mut gsp_skolemization_to_geometry: HashMap<String, Geometry> = HashMap::new();
        gsp_skolemization_to_geometry
            .insert("2".to_string(), Geometry::Point(Point::new(1.0, 2.0)));

        let mut pids_to_schema_geo_skolemization: HashMap<String, String> = HashMap::new();
        pids_to_schema_geo_skolemization.insert("1".to_string(), "2".to_string());

        let mut schema_geo_skolemization_to_geometry: HashMap<String, Point> = HashMap::new();
        schema_geo_skolemization_to_geometry.insert("2".to_string(), Point::new(1.0, 2.0));

        let empty_names: HashMap<String, String> = HashMap::new();
        let empty_descriptions: HashMap<String, String> = HashMap::new();

        let maps = HashMaps {
            pid_to_geosparql_skolemization_id: pids_to_gsp_skolemization,
            geosparql_skolemization_id_to_geometry: gsp_skolemization_to_geometry,
            pid_to_schema_name: empty_names,
            pid_to_schema_description: empty_descriptions,
            pid_to_mainstem_uri: HashMap::new(),
        };

        let result = combine_geometry_representations(&maps);
        assert!(result.is_ok());
    }

    #[test]
    fn test_read_triples_into_maps_extracts_mainstem_uri() {
        let nquads = r#"<http://example.org/feature/1> <https://www.opengis.net/def/schema/hy_features/hyf/referencedPosition> _:rp1 .
        <http://example.org/feature/1> <https://www.opengis.net/def/schema/hy_features/hyf/referencedPosition> _:rp2 .
        _:rp1 <https://www.opengis.net/def/schema/hy_features/hyf/HY_IndirectPosition> _:ip1 .
        _:ip1 <https://www.opengis.net/def/schema/hy_features/hyf/linearElement> <https://geoconnex.us/nhdplusv2/reachcode/05130108000006> .
        _:rp2 <https://www.opengis.net/def/schema/hy_features/hyf/HY_IndirectPosition> _:ip2 .
        _:ip2 <https://www.opengis.net/def/schema/hy_features/hyf/linearElement> <https://geoconnex.us/ref/mainstems/489048> ."#;

        let maps = super::read_triples_into_maps(std::io::Cursor::new(nquads))
            .expect("Expected triples to be parsed successfully");

        assert_eq!(
            maps.pid_to_mainstem_uri
                .get("<http://example.org/feature/1>")
                .unwrap(),
            "<https://geoconnex.us/ref/mainstems/489048>"
        );
    }
}
