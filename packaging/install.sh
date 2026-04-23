#!/usr/bin/env bash
set -euo pipefail

repo_owner="${BEAVERKI_REPO_OWNER:-torlenor}"
repo_name="${BEAVERKI_REPO_NAME:-beaverki}"
release_base_url="${BEAVERKI_RELEASE_BASE_URL:-https://github.com/${repo_owner}/${repo_name}/releases/download}"

tag=""
archive_url=""
install_root=""
config_dir=""
bin_dir=""
install_services=0

usage() {
  cat <<'EOF'
Usage:
  install.sh --tag <release-tag> [options]

Options:
  --tag <tag>                 Release tag to install, e.g. 2026-04-23.1
  --archive-url <url>         Direct archive URL override for testing or custom hosting
  --install-root <path>       App install root, default is platform-specific
  --config-dir <path>         BeaverKi config directory, default is platform-specific
  --bin-dir <path>            Directory for user-facing symlinks, default ~/.local/bin
  --install-services          Render user service templates for systemd or launchd
  --help                      Show this help

Environment overrides:
  BEAVERKI_RELEASE_BASE_URL   Base URL used with --tag when --archive-url is not provided
  BEAVERKI_REPO_OWNER         GitHub owner for the default release URL
  BEAVERKI_REPO_NAME          GitHub repository name for the default release URL
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --tag)
      tag="${2:-}"
      shift 2
      ;;
    --archive-url)
      archive_url="${2:-}"
      shift 2
      ;;
    --install-root)
      install_root="${2:-}"
      shift 2
      ;;
    --config-dir)
      config_dir="${2:-}"
      shift 2
      ;;
    --bin-dir)
      bin_dir="${2:-}"
      shift 2
      ;;
    --install-services)
      install_services=1
      shift
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 1
  fi
}

require_command curl
require_command tar

if [[ -z "${tag}" ]]; then
  echo "--tag is required" >&2
  usage >&2
  exit 1
fi

uname_s="$(uname -s)"
uname_m="$(uname -m)"

case "${uname_s}" in
  Linux)
    os_id="linux"
    default_install_root="${HOME}/.local/share/beaverki/app"
    default_config_dir="${HOME}/.config/beaverki"
    service_kind="systemd"
    ;;
  Darwin)
    os_id="macos"
    default_install_root="${HOME}/Library/Application Support/beaverki/app"
    default_config_dir="${HOME}/Library/Application Support/beaverki"
    service_kind="launchd"
    ;;
  *)
    echo "unsupported operating system: ${uname_s}" >&2
    exit 1
    ;;
esac

case "${uname_m}" in
  x86_64|amd64)
    arch_id="x86_64"
    ;;
  arm64|aarch64)
    arch_id="arm64"
    ;;
  *)
    echo "unsupported architecture: ${uname_m}" >&2
    exit 1
    ;;
esac

case "${os_id}-${arch_id}" in
  linux-x86_64|macos-x86_64|macos-arm64)
    platform_id="${os_id}-${arch_id}"
    ;;
  linux-arm64)
    echo "linux arm64 releases are not published yet" >&2
    exit 1
    ;;
  *)
    echo "no packaged release available for ${os_id}-${arch_id}" >&2
    exit 1
    ;;
esac

install_root="${install_root:-${default_install_root}}"
config_dir="${config_dir:-${default_config_dir}}"
bin_dir="${bin_dir:-${HOME}/.local/bin}"

archive_name="beaverki-${tag}-${platform_id}.tar.gz"
archive_url="${archive_url:-${release_base_url}/${tag}/${archive_name}}"

tmp_dir="$(mktemp -d)"
cleanup() {
  rm -rf "${tmp_dir}"
}
trap cleanup EXIT

download_path="${tmp_dir}/${archive_name}"
extract_root="${tmp_dir}/extract"
release_dir="${install_root}/releases/${tag}"
current_link="${install_root}/current"

mkdir -p "${extract_root}" "${install_root}/releases" "${config_dir}" "${bin_dir}"

echo "Downloading ${archive_url}"
curl -fsSL "${archive_url}" -o "${download_path}"

echo "Extracting ${archive_name}"
tar -xzf "${download_path}" -C "${extract_root}"

archive_root="${extract_root}/beaverki-${tag}-${platform_id}"
if [[ ! -d "${archive_root}" ]]; then
  echo "archive did not contain expected root directory: ${archive_root}" >&2
  exit 1
fi

rm -rf "${release_dir}"
mv "${archive_root}" "${release_dir}"
ln -sfn "${release_dir}" "${current_link}"
ln -sfn "${current_link}/bin/beaverki-cli" "${bin_dir}/beaverki"
ln -sfn "${current_link}/bin/beaverki-cli" "${bin_dir}/beaverki-cli"
ln -sfn "${current_link}/bin/beaverki-web" "${bin_dir}/beaverki-web"

env_file="${config_dir}/beaverki.env"
if [[ ! -f "${env_file}" ]]; then
  cat > "${env_file}" <<'EOF'
# Export the BeaverKi master passphrase for daemon-managed startup.
BEAVERKI_MASTER_PASSPHRASE=replace-with-your-passphrase
EOF
  chmod 600 "${env_file}"
fi

escape_sed_replacement() {
  printf '%s' "$1" | sed 's/[\/&]/\\&/g'
}

render_template() {
  local template_path="$1"
  local destination_path="$2"
  local install_dir_escaped
  local config_dir_escaped
  install_dir_escaped="$(escape_sed_replacement "${current_link}")"
  config_dir_escaped="$(escape_sed_replacement "${config_dir}")"
  mkdir -p "$(dirname "${destination_path}")"
  sed \
    -e "s/__BEAVERKI_INSTALL_DIR__/${install_dir_escaped}/g" \
    -e "s/__BEAVERKI_CONFIG_DIR__/${config_dir_escaped}/g" \
    "${template_path}" > "${destination_path}"
}

if [[ "${install_services}" -eq 1 ]]; then
  if [[ "${service_kind}" == "systemd" ]]; then
    render_template \
      "${current_link}/packaging/systemd/beaverki-daemon.service" \
      "${HOME}/.config/systemd/user/beaverki-daemon.service"
    render_template \
      "${current_link}/packaging/systemd/beaverki-web.service" \
      "${HOME}/.config/systemd/user/beaverki-web.service"
    if command -v systemctl >/dev/null 2>&1; then
      systemctl --user daemon-reload || true
    fi
  else
    render_template \
      "${current_link}/packaging/launchd/dev.beaverki.daemon.plist" \
      "${HOME}/Library/LaunchAgents/dev.beaverki.daemon.plist"
    render_template \
      "${current_link}/packaging/launchd/dev.beaverki.web.plist" \
      "${HOME}/Library/LaunchAgents/dev.beaverki.web.plist"
  fi
fi

cat <<EOF
BeaverKi ${tag} installed.

Install root: ${install_root}
Current app:  ${current_link}
Config dir:   ${config_dir}
Bin dir:      ${bin_dir}
Archive:      ${archive_url}

Suggested next steps:
  1. Edit ${env_file} and set BEAVERKI_MASTER_PASSPHRASE
  2. Ensure ${bin_dir} is on your PATH
  3. Run: beaverki setup init
EOF

if [[ "${service_kind}" == "systemd" ]]; then
  cat <<EOF
  4. Optional daemon service:
     systemctl --user enable --now beaverki-daemon.service
  5. Optional web UI service:
     systemctl --user enable --now beaverki-web.service
EOF
else
  cat <<EOF
  4. Optional daemon service:
     launchctl load ~/Library/LaunchAgents/dev.beaverki.daemon.plist
  5. Optional web UI service:
     launchctl load ~/Library/LaunchAgents/dev.beaverki.web.plist
EOF
fi
