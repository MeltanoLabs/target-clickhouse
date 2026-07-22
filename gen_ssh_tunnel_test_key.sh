#!/usr/bin/env bash
# Generates an ephemeral keypair for the ssh_tunnel test infra (tests/test_ssh_tunnel.py
# + the `ssh-tunnel` compose profile). Not committed to the repo -- regenerate freely,
# it only ever authenticates against the throwaway, network-isolated bastion those
# tests spin up.
set -euo pipefail

KEY_DIR="$(dirname "$0")/.ssh_tunnel_test_keys"
mkdir -p "$KEY_DIR"

if [ ! -f "$KEY_DIR/id_ed25519" ]; then
  ssh-keygen -t ed25519 -f "$KEY_DIR/id_ed25519" -N "" -q \
    -C "target-clickhouse ssh_tunnel test key (ephemeral, regenerate freely)"
  echo "Generated $KEY_DIR/id_ed25519"
else
  echo "$KEY_DIR/id_ed25519 already exists, leaving it as-is"
fi
