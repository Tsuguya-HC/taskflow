#!/usr/bin/env bash
# リポのテスト一式。CI と同じく go.mod の版の Go で `make test`（生成物の作り直し・fmt・vet・envtest を含む）
# と `make lint` を回す。golangci-lint は go.mod より新しい Go の標準ライブラリを解析できずに panic する
# （2026-10-03 実測: Go 1.27.1 で `make lint` が staticcheck の buildir の panic で止まる。go.mod の 1.26.0 では通る）。
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"
GOTOOLCHAIN="go$(awk '/^go /{print $2}' go.mod)"
export GOTOOLCHAIN
make test
make lint
