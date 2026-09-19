fn main() {
    println!("cargo:rerun-if-changed=macos-agent-info.plist");
    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("macos") {
        let manifest = std::path::PathBuf::from(
            std::env::var_os("CARGO_MANIFEST_DIR").expect("manifest directory"),
        );
        println!(
            "cargo:rustc-link-arg-bin=machine-fabric-macos-agent=-Wl,-sectcreate,__TEXT,__info_plist,{}",
            manifest.join("macos-agent-info.plist").display()
        );
    }
}
