// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

// This is a build script which runs when `cargo build` is run.
// and compiles the protobuf files into rust code that can be used
// for the grpc server
fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::compile_protos("proto/shacl_validator.proto")?;
    Ok(())
}