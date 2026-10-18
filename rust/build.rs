use std::path::PathBuf;

// The proto is a copy of api/proto/mountos_fs.proto inside the crate, so a
// packaged crate builds without the repository around it. make api-gen refreshes
// it and make proto-coherence fails when it differs.
fn main() {
  let manifest = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR"));
  let proto = manifest.join("proto/mountos_fs.proto");
  let dir = proto.parent().expect("proto dir").to_path_buf();
  println!("cargo:rerun-if-changed={}", proto.display());
  println!("cargo:rerun-if-env-changed=PROTOC");
  let mut cfg = prost_build::Config::new();
  // Every wire message and enum is non_exhaustive, so a new field or variant never breaks a caller.
  cfg.type_attribute(".mountos", "#[non_exhaustive]");
  cfg
    .compile_protos(&[proto], &[dir])
    .expect("prost-build failed (protoc must be on PATH or set via PROTOC)");
}
