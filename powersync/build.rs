use hex_literal::hex;
use sha2::Digest;
use std::env;
use std::fs::File;
use std::io::Write;
use std::ops::Deref;
use std::path::PathBuf;

/// Downloads the core extension as a pre-compiled library.
///
/// While the core extension is also written in Rust, it requires unstable features and Rust
/// nightly. So, instead of adding a cargo dependency to the core extension, we download a compiled
/// binary here.
///
/// It should be noted that build scripts aren't really supposed to download binaries. This should
/// be fine for now because we're only using this crate to build the C++ SDK, but for a Rust SDK
/// we should look into ways to make the core extension support stable Rust.
fn main() {
    let os = env::var("CARGO_CFG_TARGET_OS").unwrap();
    let arch = env::var("CARGO_CFG_TARGET_ARCH").unwrap();
    let out = PathBuf::from(env::var_os("OUT_DIR").unwrap());

    let binary = PowerSyncCoreBinary::find(&os, &arch)
        .expect("No PowerSync binary found for the target platform and architecture.");

    if binary.hash_alg != "sha256" {
        panic!("Unsupported hash algorithm: {}", binary.hash_alg);
    }

    println!("Downloading {binary:?} into {out:?}");
    let response = reqwest::blocking::get(binary.url).expect("Failed to download core extension.");
    if !response.status().is_success() {
        panic!(
            "Could not download core extension: {}.",
            response.status().as_str()
        );
    }

    let bytes = response
        .bytes()
        .expect("Could not read core extension response.");

    let digest = sha2::Sha256::digest(&bytes);
    if digest.deref() != binary.digest {
        panic!("Checksum mismatch")
    }

    let file_path = out.join(match &*os {
        "windows" => "powersync_core.lib",
        "macos" | "linux" | "android" => "libpowersync_core.a",
        _ => panic!("Unsupported OS"),
    });
    let mut file = File::create(&file_path).expect("Could not create target file");
    file.write_all(&bytes)
        .expect("Could not write to target file");

    println!(
        "cargo:rustc-link-search={}",
        out.as_path().to_str().unwrap()
    );
}

struct PowerSyncCoreBinary {
    /// The [target operating system](https://doc.rust-lang.org/reference/conditional-compilation.html#target_os)
    /// of this binary.
    os: &'static str,
    /// The [target architecture](https://doc.rust-lang.org/reference/conditional-compilation.html#target_arch)
    /// of this binary.
    architecture: &'static str,
    /// The name this binary has in PowerSync releases
    filename: &'static str,
}

#[derive(Debug)]
struct ResolvedPowerSyncBinary {
    filename: &'static str,
    hash_alg: &'static str,
    digest: &'static [u8],
    url: &'static str,
}

impl PowerSyncCoreBinary {
    const fn new(os: &'static str, architecture: &'static str, filename: &'static str) -> Self {
        Self {
            os,
            architecture,
            filename,
        }
    }

    fn resolve(&self) -> ResolvedPowerSyncBinary {
        for (filename, url, alg, digest) in Self::HASHES {
            if *filename == self.filename {
                return ResolvedPowerSyncBinary {
                    hash_alg: alg,
                    digest,
                    filename,
                    url,
                };
            }
        }

        panic!("No hash found for {}", self.filename);
    }

    pub fn find(os: &str, architecture: &str) -> Option<ResolvedPowerSyncBinary> {
        for value in Self::VALUES {
            if value.os == os && value.architecture == architecture {
                return Some(value.resolve());
            }
        }

        None
    }

    const VALUES: &'static [PowerSyncCoreBinary] = &[
        // Linux
        Self::new("linux", "aarch64", "libpowersync_aarch64.linux.a"),
        Self::new("linux", "arm", "libpowersync_armv7.linux.a"),
        Self::new("linux", "riscv64gc", "libpowersync_riscv64gc.linux.a"),
        Self::new("linux", "x86_64", "libpowersync_x64.linux.a"),
        Self::new("linux", "x86", "libpowersync_x86.linux.a"),
        // Windows
        Self::new("windows", "aarch64", "powersync_aarch64.lib"),
        Self::new("windows", "x86_64", "powersync_x64.lib"),
        Self::new("windows", "x86", "powersync_x86.lib"),
        // macOS
        Self::new("macos", "aarch64", "libpowersync_aarch64.macos.a"),
        Self::new("macos", "x86_64", "libpowersync_x64.macos.a"),
        // iOS
        Self::new("ios", "aarch64", " libpowersync_aarch64.ios.a"),
        // TODO: Figure out CARGO_CFG_TARGET_OS for iOS simulators
        // Android
        Self::new("android", "aarch64", "libpowersync_aarch64.android.a"),
        Self::new("android", "arm", "libpowersync_armv7.android.a"),
        Self::new("android", "x86_64", "libpowersync_x64.android.a"),
        Self::new("android", "x86", "libpowersync_x86.android.a"),
    ];

    // Generate with node generate_hashes.js
    const HASHES: &'static [(&'static str, &'static str, &'static str, &'static [u8])] = &[
        (
            "libpowersync-wasm.a",
            "https://github.com/powersync-ja/powersync-sqlite-core/releases/download/v0.4.7/libpowersync-wasm.a",
            "sha256",
            hex!("925702a90fc527211f0c89ab3334a53a498a4c768533fef467a7b36b43d16f81").as_slice(),
        ),
        (
            "libpowersync_aarch64.android.a",
            "https://github.com/powersync-ja/powersync-sqlite-core/releases/download/v0.4.7/libpowersync_aarch64.android.a",
            "sha256",
            hex!("93439b5b6aca8ebe82ef4ccaaa8594e25436e7ae9cad5c5379a2dd1bf3fdfa7d").as_slice(),
        ),
        (
            "libpowersync_aarch64.ios-sim.a",
            "https://github.com/powersync-ja/powersync-sqlite-core/releases/download/v0.4.7/libpowersync_aarch64.ios-sim.a",
            "sha256",
            hex!("0f77b5d652038e1856a9681b1c8f287e75700510f60ef3f17c16cb4d7d84d21f").as_slice(),
        ),
        (
            "libpowersync_aarch64.ios.a",
            "https://github.com/powersync-ja/powersync-sqlite-core/releases/download/v0.4.7/libpowersync_aarch64.ios.a",
            "sha256",
            hex!("dd97e153e7d1a9211da451ddcc472406743dc9a5605aee4785cfe1a68dbf6e68").as_slice(),
        ),
        (
            "libpowersync_aarch64.linux.a",
            "https://github.com/powersync-ja/powersync-sqlite-core/releases/download/v0.4.7/libpowersync_aarch64.linux.a",
            "sha256",
            hex!("67fe67800a0007b1e56232313de0a2124952aa8cc376774ef12db0015e95df29").as_slice(),
        ),
        (
            "libpowersync_aarch64.macos.a",
            "https://github.com/powersync-ja/powersync-sqlite-core/releases/download/v0.4.7/libpowersync_aarch64.macos.a",
            "sha256",
            hex!("cd70f83603af9083e209c8a676aeb777651e53ac5d58fddfc751e31f12603b82").as_slice(),
        ),
        (
            "libpowersync_armv7.android.a",
            "https://github.com/powersync-ja/powersync-sqlite-core/releases/download/v0.4.7/libpowersync_armv7.android.a",
            "sha256",
            hex!("845c50c620a8d4a91268aab99866ac69214188409ddb930e6a9655b5e17feb40").as_slice(),
        ),
        (
            "libpowersync_armv7.linux.a",
            "https://github.com/powersync-ja/powersync-sqlite-core/releases/download/v0.4.7/libpowersync_armv7.linux.a",
            "sha256",
            hex!("a134cecf2aee740279899834c05987c228f7300a19af0ee64cbaf29825d408ef").as_slice(),
        ),
        (
            "libpowersync_riscv64gc.linux.a",
            "https://github.com/powersync-ja/powersync-sqlite-core/releases/download/v0.4.7/libpowersync_riscv64gc.linux.a",
            "sha256",
            hex!("f578b91dd7106e13a598529c0f9ac9594f16f35365a52c8df3e5f3dc3e30307f").as_slice(),
        ),
        (
            "libpowersync_x64.android.a",
            "https://github.com/powersync-ja/powersync-sqlite-core/releases/download/v0.4.7/libpowersync_x64.android.a",
            "sha256",
            hex!("b47b6febdcc7f1f64b822211386c475368d17b3ad2f14fb8fd2ebf9126e4b6a2").as_slice(),
        ),
        (
            "libpowersync_x64.ios-sim.a",
            "https://github.com/powersync-ja/powersync-sqlite-core/releases/download/v0.4.7/libpowersync_x64.ios-sim.a",
            "sha256",
            hex!("47633500d7b7fbdbf86ced698c6cb527a3d1272daf6d94c6c0ca6e5fe88b11f1").as_slice(),
        ),
        (
            "libpowersync_x64.linux.a",
            "https://github.com/powersync-ja/powersync-sqlite-core/releases/download/v0.4.7/libpowersync_x64.linux.a",
            "sha256",
            hex!("bf10fe4826614f92aa01fae18b0cb120ea803b5ab71dc0d6d85c5f10f17466cb").as_slice(),
        ),
        (
            "libpowersync_x64.macos.a",
            "https://github.com/powersync-ja/powersync-sqlite-core/releases/download/v0.4.7/libpowersync_x64.macos.a",
            "sha256",
            hex!("eb649ece12b997de9ac1a2f4c6a5356b0b4c0a05505d36827a0a179982fe344a").as_slice(),
        ),
        (
            "libpowersync_x86.android.a",
            "https://github.com/powersync-ja/powersync-sqlite-core/releases/download/v0.4.7/libpowersync_x86.android.a",
            "sha256",
            hex!("d789c5409e9404048e326fec85101fd61232cd36b06e53a4a30e2e8e3ce9c498").as_slice(),
        ),
        (
            "libpowersync_x86.linux.a",
            "https://github.com/powersync-ja/powersync-sqlite-core/releases/download/v0.4.7/libpowersync_x86.linux.a",
            "sha256",
            hex!("301e124791937f6e94399d898f67d558da9dfec2b685f956d4344a80150f6734").as_slice(),
        ),
    ];
}
