#!/usr/bin/env bash
# リポのテスト一式。CI と同じ `make test`（生成物の作り直し・fmt・vet・envtest を含む）。
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"
make test
