// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

#[cfg(test)]
mod tests {
    use crate::{read_triples_into_arrays};
    use std::io::Cursor;

    #[test]
    fn test_read_triples_into_arrays() {
        // Minimal valid N-Quads covering the logic paths:
        // PID --hasGeometry--> skolem node
        // skolem node --asWKT--> WKT literal
        let nquads = r#"<http://example.org/feature/1> <http://www.opengis.net/ont/geosparql#hasGeometry> _:geom1 .
        _:geom1 <http://www.opengis.net/ont/geosparql#asWKT> "POINT (1 2)"^^<http://www.opengis.net/ont/geosparql#wktLiteral> ."#;

        let reader = Cursor::new(nquads);

        let arrays = read_triples_into_arrays(reader)
            .expect("Expected triples to be parsed successfully");

        assert_eq!(arrays.len(), 2, "Expected two columns, geometry and id");

        let geometry_array = &arrays[0];
        let id_array = &arrays[1];

        assert_eq!(geometry_array.len(), 1);
        assert_eq!(id_array.len(), 1);

        let id_array = id_array
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .expect("ID column should be a StringArray");

        assert_eq!(
            id_array.value(0),
            "<http://example.org/feature/1>"
        );
    }
}
