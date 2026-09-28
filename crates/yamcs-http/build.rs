use std::io::Result;

fn main() -> Result<()> {
    let mut config = prost_build::Config::new();
    // Emits the nested module tree, so lib.rs pulls it in with a single include!
    config.include_file("mod.rs");
    config.compile_protos(
        &[
            "../../proto/yamcs/api/annotations.proto",
            "../../proto/yamcs/api/exception.proto",
            "../../proto/yamcs/api/httpbody.proto",
            "../../proto/yamcs/api/websocket.proto",
            "../../proto/yamcs/protobuf/mdb/mdb.proto",
            "../../proto/yamcs/protobuf/processing/processing.proto",
            "../../proto/yamcs/protobuf/pvalue/pvalue.proto",
            "../../proto/yamcs/protobuf/services/services.proto",
            "../../proto/yamcs/protobuf/yamcs.proto",
            "../../proto/yamcs/protobuf/yamcsManagement/yamcsManagement.proto",
        ],
        &["../../proto/"],
    )?;
    Ok(())
}
