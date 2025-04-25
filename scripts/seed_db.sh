#!/bin/bash

# dsbulk_loader.sh - Script to install DSBulk and load data into Cassandra

# Configuration variables
DSBULK_VERSION="1.11.0"
DSBULK_URL="https://downloads.datastax.com/dsbulk/dsbulk-${DSBULK_VERSION}.tar.gz"
INSTALL_DIR="/opt"
DSBULK_HOME="${INSTALL_DIR}/dsbulk-${DSBULK_VERSION}"
DATA_FILE="/sparkdata/en.openfoodfacts.org.products.csv"
KEYSPACE="mykeyspace"
TABLE="openfoodfacts"
DELIMITER="\t"
MAX_RECORDS=1894492

# Logging function
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

# Error handling function
error_exit() {
    log "ERROR: $1" >&2
    exit 1
}

# Check if running as root
check_root() {
    if [ "$(id -u)" -ne 0 ]; then
        error_exit "This script must be run as root"
    fi
}

# Add DSBulk to PATH and ensure it's available
ensure_dsbulk_available() {
    # Add to PATH if not already present
    if ! grep -q "dsbulk-${DSBULK_VERSION}" ~/.bashrc; then
        log "Adding DSBulk to PATH in ~/.bashrc"
        echo "export PATH=${DSBULK_HOME}/bin:\$PATH" >> ~/.bashrc
    fi
    
    # Source the updated PATH in current shell
    export PATH="${DSBULK_HOME}/bin:$PATH"
    
    # Verify dsbulk is available
    if ! command -v dsbulk >/dev/null 2>&1; then
        error_exit "DSBulk command still not available after PATH update"
    fi
}

# Install DSBulk
install_dsbulk() {
    log "Checking DSBulk installation..."
    
    # Check if already installed
    if [ -d "${DSBULK_HOME}" ]; then
        log "DSBulk already installed at ${DSBULK_HOME}"
        ensure_dsbulk_available
        return 0
    fi
    
    # Create installation directory
    mkdir -p "${INSTALL_DIR}" || error_exit "Failed to create ${INSTALL_DIR}"
    
    # Download DSBulk
    log "Downloading DSBulk from ${DSBULK_URL}"
    if ! wget -q "${DSBULK_URL}" -O "/tmp/dsbulk-${DSBULK_VERSION}.tar.gz"; then
        error_exit "Failed to download DSBulk"
    fi
    
    # Extract to installation directory
    log "Extracting to ${INSTALL_DIR}"
    if ! tar -xzf "/tmp/dsbulk-${DSBULK_VERSION}.tar.gz" -C "${INSTALL_DIR}"; then
        error_exit "Failed to extract DSBulk archive"
    fi
    
    # Clean up downloaded archive
    rm -f "/tmp/dsbulk-${DSBULK_VERSION}.tar.gz"
    
    ensure_dsbulk_available
    log "Installation verified: $(dsbulk --version)"
}

# Load data into Cassandra
load_data() {
    log "Starting data load process..."
    
    if [ ! -f "${DATA_FILE}" ]; then
        error_exit "Data file not found at ${DATA_FILE}"
    fi
    
    log "Loading data from ${DATA_FILE} to ${KEYSPACE}.${TABLE}"
    
    # Use full path to dsbulk to be absolutely sure
    "${DSBULK_HOME}/bin/dsbulk" load \
        -url "${DATA_FILE}" \
        -k "${KEYSPACE}" \
        -t "${TABLE}" \
        --connector.csv.delimiter "${DELIMITER}" \
        --schema.allowMissingFields true \
        --codec.nullStrings "NULL" \
        --connector.csv.maxCharsPerColumn -1 \
        --connector.csv.maxRecords "${MAX_RECORDS}"
    
    if [ $? -eq 0 ]; then
        log "Data load completed successfully"
    else
        error_exit "Data load failed"
    fi
}

# Main execution
main() {
    check_root
    install_dsbulk
    load_data
}

main "$@"