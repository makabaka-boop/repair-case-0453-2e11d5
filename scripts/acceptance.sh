#!/usr/bin/env bash
# End-to-end acceptance over the internal compose network, including a full
# restart of both API instances.
#
#   ./scripts/acceptance.sh
#
# Steps:
#   1. build images and start db + api1 + api2
#   2. run the verify one-shot (SEED phase): protocol matrix + cross-instance
#      race, and persist fixture batch IDs in the verify-state volume
#   3. restart BOTH API instances (database keeps running)
#   4. run the verify one-shot (RESTART phase): acknowledgements, gaps and the
#      sealed verdict must be identical
set -euo pipefail

cd "$(dirname "$0")/.."

compose() { docker compose "$@"; }

echo "==> building and starting db, api1, api2"
compose up -d --build db api1 api2

echo "==> acceptance phase 1: protocol, conflicts, races, seed fixtures"
compose run --rm -e VERIFY_PHASE=seed verify

echo "==> restarting BOTH API instances (database stays up)"
compose restart api1 api2

echo "==> acceptance phase 2: verdicts after restart"
compose run --rm -e VERIFY_PHASE=restart verify

echo "==> ACCEPTANCE PASSED"
echo "(services still running; stop with: docker compose down -v)"
