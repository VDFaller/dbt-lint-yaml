#!/bin/sh

set -eu

PACKAGE_NAME="dbt-lint-yaml"
REPO_OWNER="VDFaller"
REPO_NAME="dbt-lint-yaml"
REPO="${REPO_OWNER}/${REPO_NAME}"
DEFAULT_DEST="$HOME/.local/bin"

TARGET_VERSION=""
RELEASE_JSON=""
ASSET_NAME=""
ASSET_URL=""

td=""

cleanup() {
    if [ -n "$td" ] && [ -d "$td" ]; then
        rm -rf "$td"
    fi
}

trap cleanup EXIT

log() {
    printf "install.sh: %s\n" "$1"
}

log_debug() {
    if [ "${DEBUG:-}" = "1" ]; then
        printf "install.sh: %s\n" "$1"
    fi
}

err_and_exit() {
    message="$1"
    shift || true
    if [ "$#" -gt 0 ]; then
        printf "install.sh: %s -- %s\n" "$message" "$*" >&2
    else
        printf "install.sh: %s\n" "$message" >&2
    fi
    exit 1
}

need() {
    if ! command -v "$1" >/dev/null 2>&1; then
        err_and_exit "required command not found" "$1"
    fi
}

help() {
    cat <<'EOF'
Usage: install.sh [options]

Options:
  --version <VER>   Install a specific version (defaults to latest release)
  --target <TRIPLE> Install for a specific target (defaults to host platform)
  --to <DIR>        Install into the provided directory (default: ~/.local/bin)
  --update          Overwrite an existing installation
  --help            Show this message

Examples:
  ./install.sh                       # install latest release for current platform
  ./install.sh --version 0.2.0       # install version 0.2.0
  ./install.sh --update              # reinstall or update to the latest release
EOF
}

parse_args() {
    UPDATE=false
    VERSION=""
    TARGET=""
    DEST="$DEFAULT_DEST"

    while [ "$#" -gt 0 ]; do
        case "$1" in
            --version)
                [ "$#" -ge 2 ] || err_and_exit "--version expects an argument"
                VERSION="$2"
                shift
                ;;
            --target)
                [ "$#" -ge 2 ] || err_and_exit "--target expects an argument"
                TARGET="$2"
                shift
                ;;
            --to)
                [ "$#" -ge 2 ] || err_and_exit "--to expects an argument"
                DEST="$2"
                shift
                ;;
            --update|-u)
                UPDATE=true
                ;;
            --help|-h)
                help
                exit 0
                ;;
            *)
                err_and_exit "unknown argument" "$1"
                ;;
        esac
        shift
    done
}

normalize_dest() {
    case "$DEST" in
        /*) ;;
        *) DEST="$PWD/$DEST" ;;
    esac
}

check_dependencies() {
    need curl
    need tar
    need install
    need mktemp
    need uname
    if ! command -v jq >/dev/null 2>&1; then
        err_and_exit "jq is required to parse GitHub release metadata. Please install jq and re-run."
    fi
}

normalize_version() {
    value="$1"
    if [ -z "$value" ]; then
        echo ""
        return
    fi
    echo "${value#v}"
}

detect_target_platform() {
    if [ -n "$TARGET" ]; then
        echo "$TARGET"
        return
    fi

    os=$(uname -s | tr '[:upper:]' '[:lower:]')
    arch=$(uname -m)

    case "$os" in
        linux)
            case "$arch" in
                x86_64)
                    echo "linux-x86_64-musl"
                    ;;
                *)
                    err_and_exit "unsupported linux architecture" "$arch"
                    ;;
            esac
            ;;
        *)
            err_and_exit "unsupported operating system" "$os"
            ;;
    esac
}

fetch_release_metadata() {
    requested_version="$1"
    api_base="https://api.github.com/repos/$REPO/releases"

    if [ -n "$requested_version" ]; then
        normalized=$(normalize_version "$requested_version")
        tag="v$normalized"
        url="$api_base/tags/$tag"
        log "Fetching release metadata for $tag"
    else
        url="$api_base/latest"
        log "Fetching latest release metadata"
    fi

    RELEASE_JSON=$(curl -sSfL -H "Accept: application/vnd.github+json" "$url") || \
        err_and_exit "failed to retrieve release metadata" "$url"

    tag_name=$(printf "%s" "$RELEASE_JSON" | jq -r '.tag_name // empty')
    [ -n "$tag_name" ] || err_and_exit "release metadata missing tag name"

    TARGET_VERSION="${tag_name#v}"
    log "Selected version $TARGET_VERSION"
}

select_asset() {
    version="$1"
    target_platform="$2"
    ASSET_NAME="${PACKAGE_NAME}-${version}-${target_platform}.tar.gz"
    download_url=$(printf "%s" "$RELEASE_JSON" | jq -r --arg NAME "$ASSET_NAME" '.assets[]? | select(.name == $NAME) | .browser_download_url // empty')

    [ -n "$download_url" ] || err_and_exit "release does not contain asset" "$ASSET_NAME"

    ASSET_URL="$download_url"
    log "Found release asset $ASSET_NAME"
}

check_current_install() {
    binary_path="$DEST/$PACKAGE_NAME"
    if [ ! -x "$binary_path" ]; then
        echo ""
        return
    fi

    version_output=$("$binary_path" --version 2>/dev/null || true)
    version=$(printf "%s" "$version_output" | awk '{print $2}' | head -n 1)
    printf "%s" "$version"
}

ensure_destination() {
    if [ ! -d "$DEST" ]; then
        mkdir -p "$DEST" || err_and_exit "failed to create install directory" "$DEST"
    fi
}

install_binary() {
    td=$(mktemp -d 2>/dev/null || mktemp -d -t install)
    archive="$td/$ASSET_NAME"

    log_debug "Downloading $ASSET_URL"
    curl -sSfL -o "$archive" "$ASSET_URL" || err_and_exit "failed to download asset" "$ASSET_URL"

    tar -C "$td" -xzf "$archive" || err_and_exit "failed to extract archive" "$archive"

    binary_path=$(find "$td" -type f -name "$PACKAGE_NAME" -perm -111 | head -n 1)
    [ -n "$binary_path" ] || err_and_exit "extracted archive does not contain $PACKAGE_NAME"

    install -m 755 "$binary_path" "$DEST/$PACKAGE_NAME" || err_and_exit "failed to install binary" "$DEST/$PACKAGE_NAME"
}

print_path_hint() {
    case ":$PATH:" in
        *:"$DEST":*)
            return
            ;;
        *)
            cat <<EOF

NOTE: $DEST is not on your PATH.
Add it with:
  export PATH="\$PATH:$DEST"
or append that line to your shell configuration file.
EOF
            ;;
    esac
}

main() {
    parse_args "$@"
    normalize_dest
    check_dependencies

    target_platform=$(detect_target_platform)
    fetch_release_metadata "$VERSION"
    select_asset "$TARGET_VERSION" "$target_platform"

    ensure_destination

    current_version=$(check_current_install)
    if [ -n "$current_version" ]; then
        if [ "$current_version" = "$TARGET_VERSION" ]; then
            log "$PACKAGE_NAME $TARGET_VERSION is already installed at $DEST"
            exit 0
        fi
        if [ "$UPDATE" != "true" ]; then
            err_and_exit "$PACKAGE_NAME $current_version already exists at $DEST" "use --update to overwrite"
        fi
        log "Updating $PACKAGE_NAME from $current_version to $TARGET_VERSION"
    else
        log "Installing $PACKAGE_NAME $TARGET_VERSION"
    fi

    install_binary
    print_path_hint
    log "$PACKAGE_NAME $TARGET_VERSION installed to $DEST"
}

main "$@"
