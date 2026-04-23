#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "usage: $0 <release-tag> <platform-id>" >&2
  exit 1
fi

release_tag="$1"
platform_id="$2"
archive_name="beaverki-${release_tag}-${platform_id}.tar.gz"
package_root="dist/beaverki-${release_tag}-${platform_id}"

rm -rf "${package_root}" "${archive_name}"
mkdir -p "${package_root}/bin" "${package_root}/packaging"

cp target/release/beaverki-cli "${package_root}/bin/beaverki-cli"
cp target/release/beaverki-web "${package_root}/bin/beaverki-web"
cp -R skills "${package_root}/skills"
cp -R packaging/systemd "${package_root}/packaging/systemd"
cp -R packaging/launchd "${package_root}/packaging/launchd"
cp packaging/install.sh "${package_root}/packaging/install.sh"
cp packaging/README.md "${package_root}/packaging/README.md"
cp README.md LICENSE "${package_root}/"

tar -C dist -czf "${archive_name}" "beaverki-${release_tag}-${platform_id}"

echo "${archive_name}"
