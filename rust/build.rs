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
  prost_build::compile_protos(&[proto], &[dir]).expect("prost-build failed (protoc must be on PATH or set via PROTOC)");
}
