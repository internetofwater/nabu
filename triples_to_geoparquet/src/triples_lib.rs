// Copyright 2026 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

use std::{collections::HashMap, io::BufRead};

use log::{debug, error};
use oxttl::NQuadsParser;

use geo_types::{Geometry, Point};
use wkt::{ToWkt, TryFromWkt};

use crate::{f64_from_triple_term, generally_equal};

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

    let geometry = Geometry::try_from_wkt_str(&part.to_string())?;

    Ok(geometry)
}

pub fn read_triples_into_maps<R: BufRead>(
    triples_reader: R,
) -> Result<HashMaps, Box<dyn std::error::Error>> {
    // there are two ways to encode geometries in nquads: either as WKT or as a schema.org latitude/longitude pair
    let mut pid_to_geosparql_skolemization_id: HashMap<String, String> = HashMap::new();
    let mut geosparql_skolemization_id_to_geometry: HashMap<String, Geometry> = HashMap::new();

    let mut pid_to_schema_geo_skolemization_id: HashMap<String, String> = HashMap::new();
    let mut schema_geo_skolemization_id_to_geometry: HashMap<String, Point> = HashMap::new();

    let parser = NQuadsParser::new();
    let parsed_quads = parser.for_reader(triples_reader);

    for quad in parsed_quads {
        let quad = quad?;
        let subject = quad.subject;
        let predicate = quad.predicate;
        let object = quad.object;

        let predicate_str = predicate.to_string();
        match predicate_str.clone().as_str() {
            "<http://www.opengis.net/ont/geosparql#hasGeometry>" => {
                debug!("Found geometry: {}", object.to_owned().to_string());
                pid_to_geosparql_skolemization_id.insert(
                    subject.to_owned().to_string(),
                    object.to_owned().to_string(),
                );
            }
            "<https://schema.org/geo>" => {
                debug!("Found geometry: {}", object.to_owned().to_string());
                pid_to_schema_geo_skolemization_id.insert(
                    subject.to_owned().to_string(),
                    object.to_owned().to_string(),
                );
            }

            point_coord_type @ ("<https://schema.org/longitude>"
            | "<https://schema.org/latitude>") => {
                match schema_geo_skolemization_id_to_geometry.get(&subject.to_owned().to_string()) {
                    Some(existing_val) => match (existing_val.x(), existing_val.y()) {
                        (UKNOWN_POINT_COORD, UKNOWN_POINT_COORD) => {
                            return Err("Found a point with unknown coords for both x/y; this is a sign that something went wrong on our end during construction".into());
                        }
                        (UKNOWN_POINT_COORD, y) => {
                            schema_geo_skolemization_id_to_geometry.insert(
                                subject.to_string(),
                                Point::new(f64_from_triple_term(&object.to_string())?, y),
                            );
                        }
                        (x, UKNOWN_POINT_COORD) => {
                            schema_geo_skolemization_id_to_geometry.insert(
                                subject.to_string(),
                                Point::new(x, f64_from_triple_term(&object.to_string())?),
                            );
                        }
                        (_, _) => {
                            return Err("Found a point with known coords for both x/y; this is a sign that something is defined multiple times in the triples or went wrong on our end during construction".into());
                        }
                    },
                    None => {
                        match point_coord_type {
                            "<https://schema.org/latitude>" => {
                                schema_geo_skolemization_id_to_geometry.insert(
                                    subject.to_string(),
                                    Point::new(
                                        UKNOWN_POINT_COORD,
                                        f64_from_triple_term(&object.to_string())?,
                                    ),
                                );
                            }
                            "<https://schema.org/longitude>" => {
                                schema_geo_skolemization_id_to_geometry.insert(
                                    subject.to_owned().to_string(),
                                    Point::new(
                                        f64_from_triple_term(&object.to_string())?,
                                        UKNOWN_POINT_COORD,
                                    ),
                                );
                            }
                            // skip other predicates unrelated to schema geo
                            _ => {}
                        }
                    }
                }
            }

            "<http://www.opengis.net/ont/geosparql#asWKT>" => {
                debug!("Found WKT: {}", object.to_owned().to_string());
                let object_string = object.to_owned().to_string();

                let geometry = parse_wkt_from_triple_string(&object_string)?;
                geosparql_skolemization_id_to_geometry
                    .insert(subject.to_owned().to_string(), geometry);
            }
            &_ => {}
        }
    }
    Ok(HashMaps {
        pid_to_geosparql_skolemization_id,
        geosparql_skolemization_id_to_geometry,
        pid_to_schema_geo_skolemization_id,
        schema_geo_skolemization_id_to_geometry,
    })
}

// A container struct for all hash maps; used for merging together
// triple ids with their associated geometries
pub struct HashMaps {
    pub pid_to_geosparql_skolemization_id: HashMap<String, String>,
    pub geosparql_skolemization_id_to_geometry: HashMap<String, Geometry>,
    pub pid_to_schema_geo_skolemization_id: HashMap<String, String>,
    pub schema_geo_skolemization_id_to_geometry: HashMap<String, Point>,
}

const UKNOWN_POINT_COORD: f64 = -1.0;

/// Given info for both the geosparql and schema geo representations of a geometry,
/// combine them into a single canonical representation for each pid and return
/// the associated hashmap
pub fn combine_geometry_representations(
    maps: HashMaps,
) -> Result<HashMap<String, Geometry>, Box<dyn std::error::Error>> {
    let mut pid_to_canonical_geometry: HashMap<String, Geometry> = HashMap::new();

    // first we go through and get all the geosparql geometry;
    // this is the ideal canonical representation since wkt is more flexible
    // than just a point
    for (pid, geosparql_skolemization_id) in maps.pid_to_geosparql_skolemization_id {
        match maps
            .geosparql_skolemization_id_to_geometry
            .get(&geosparql_skolemization_id)
        {
            Some(geometry) => {
                pid_to_canonical_geometry.insert(pid, geometry.clone());
            }
            None => {
                return Err(format!(
                    "Could not find geometry for geosparql skolemization id {} for pid {}",
                    geosparql_skolemization_id, pid
                )
                .into());
            }
        }
    }

    // next we go through and get all the schema geo geometries
    for (pid, schema_geo_skolemization_id) in maps.pid_to_schema_geo_skolemization_id {
        match maps
            .schema_geo_skolemization_id_to_geometry
            .get(&schema_geo_skolemization_id)
        {
            Some(point_geometry) => {
                if let Some(gsp_geometry) = pid_to_canonical_geometry.get(&pid) {
                    debug!(
                        "Found gsp geometry for pid {}: {}",
                        pid,
                        gsp_geometry.to_wkt()
                    );
                    if !generally_equal(gsp_geometry, point_geometry) {
                        return Err(format!(
                                "pid {} with geosparql geometry '{}' does not match schema geo skolemization id {} with schema geo point geometry '{}'",
                                pid, gsp_geometry.to_wkt(), schema_geo_skolemization_id, point_geometry.to_wkt()
                            ).into());
                    }
                }
                pid_to_canonical_geometry.insert(pid, Geometry::Point(point_geometry.clone()));
            }
            None => {
                error!(
                    "No schema:geo geometry for pid {} with skolemization id {}",
                    pid, schema_geo_skolemization_id
                )
            }
        }
    }

    Ok(pid_to_canonical_geometry)
}

#[cfg(test)]
mod tests {
    use geo_types::{Geometry, Point};

    use std::{collections::HashMap};

    use crate::triples_lib::{HashMaps, combine_geometry_representations};

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

        let maps = HashMaps {
            pid_to_geosparql_skolemization_id: pids_to_gsp_skolemization,
            geosparql_skolemization_id_to_geometry: gsp_skolemization_to_geometry,
            pid_to_schema_geo_skolemization_id: pids_to_schema_geo_skolemization,
            schema_geo_skolemization_id_to_geometry: schema_geo_skolemization_to_geometry,
        };

        let result = combine_geometry_representations(maps);
        assert!(result.is_ok());
    }
}
