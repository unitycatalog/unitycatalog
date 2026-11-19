use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{bail, Context, Result};
use buffa_codegen::generated::compiler::CodeGeneratorRequest;
use buffa_codegen::generated::descriptor::FileDescriptorSet;
use protoc_gen_protovalidate_buffa::{emit, scan};

const PROTO_FILES: &[&str] = &[
    "uc/v1/common.proto",
    "uc/v1/catalog.proto",
    "uc/v1/schema.proto",
    "uc/v1/table.proto",
    "uc/v1/volume.proto",
    "uc/v1/function.proto",
    "uc/v1/model.proto",
    "uc/v1/view.proto",
    "uc/v1/proxy.proto",
];

fn main() -> Result<()> {
    let server_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let ui_dir = server_dir
        .parent()
        .context("server directory has no parent")?;
    let out_dir = PathBuf::from(std::env::var_os("OUT_DIR").context("OUT_DIR is not set")?);
    let descriptor_path = out_dir.join("uc_descriptor.bin");

    build_descriptor_set(ui_dir, &descriptor_path)?;

    connectrpc_build::Config::new()
        .files(PROTO_FILES)
        .descriptor_set(&descriptor_path)
        .include_file("_connectrpc.rs")
        .compile()
        .context("failed to generate ConnectRPC services")?;

    generate_validators(&descriptor_path, &out_dir.join("validation"))?;

    for proto in PROTO_FILES {
        println!(
            "cargo:rerun-if-changed={}",
            ui_dir.join("proto").join(proto).display()
        );
    }
    println!(
        "cargo:rerun-if-changed={}",
        ui_dir.join("buf.yaml").display()
    );
    println!(
        "cargo:rerun-if-changed={}",
        ui_dir.join("buf.lock").display()
    );
    Ok(())
}

fn build_descriptor_set(ui_dir: &Path, output: &Path) -> Result<()> {
    let local_buf = ui_dir.join("web/node_modules/.bin/buf");
    let buf = std::env::var_os("BUF")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            if local_buf.is_file() {
                local_buf
            } else {
                PathBuf::from("buf")
            }
        });

    let result = Command::new(&buf)
        .current_dir(ui_dir)
        .args(["build", "--as-file-descriptor-set", "-o"])
        .arg(output)
        .output()
        .with_context(|| format!("failed to run {}", buf.display()))?;

    if !result.status.success() {
        bail!(
            "buf build failed: {}",
            String::from_utf8_lossy(&result.stderr)
        );
    }
    Ok(())
}

fn generate_validators(descriptor_path: &Path, output_dir: &Path) -> Result<()> {
    let descriptor_bytes = fs::read(descriptor_path).context("failed to read descriptor set")?;
    let options = buffa_codegen::tooling_decode_options()
        .map_err(|error| anyhow::anyhow!("failed to create descriptor decoder: {error}"))?;
    let descriptor_set = options
        .decode_from_slice::<FileDescriptorSet>(&descriptor_bytes)
        .map_err(|error| anyhow::anyhow!("failed to decode descriptor set: {error}"))?;
    let request = CodeGeneratorRequest {
        file_to_generate: PROTO_FILES.iter().map(ToString::to_string).collect(),
        proto_file: descriptor_set.file,
        parameter: Some("proto_module=crate::proto".to_string()),
        ..Default::default()
    };

    let validators = scan::gather(&request).context("failed to scan validation rules")?;
    let files = emit::render_with_options(
        &validators,
        &emit::Options {
            proto_module: "crate::proto".to_string(),
        },
    )
    .context("failed to generate validators")?;

    fs::create_dir_all(output_dir).context("failed to create validator output directory")?;
    for file in files {
        let name = file.name.context("validator output had no file name")?;
        let content = file.content.context("validator output had no content")?;
        fs::write(output_dir.join(name), content).context("failed to write generated validator")?;
    }
    Ok(())
}
