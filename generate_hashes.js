// This scripts generates hashes and download URLs referenced in powersync/build.rs.

const version = "v0.4.7";
const url = `https://api.github.com/repos/powersync-ja/powersync-sqlite-core/releases/tags/${version}`;

const response = await fetch(url);
if (!response.ok) {
  throw `Unexpected response for ${url}: ${await response.text()}`;
}

const hashes = [];

const payload = await response.json();
for (const asset of payload.assets) {
  if (asset.name.endsWith(".a")) {
    const[alg, digest] = asset.digest.split(":");

    hashes.push({
      name: asset.name,
      digest: digest,
      alg: alg,
      url: asset.browser_download_url,
    });
  }
}

for (const {name, alg, digest, url} of hashes) {
  console.log(`("${name}", "${url}", "${alg}", hex!("${digest}").as_slice()),`);
}
