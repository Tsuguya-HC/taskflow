# taskflow

フェーズ管理エージェント実行基盤（CRD `flow.tgy.io` とコントローラ）。利用側は home-cluster。

- 設計の現在の姿は `docs/design.md`、決定の理由と経緯は `docs/adr/`
- 未決事項は GitHub Discussions

## 提供側 / 利用側の線引き

design.md §2「誰の責務か」の表が正。コントローラが持つのは**遷移・判定の回収・実行の同一性・上限・掃除**だけ。
権限・Pod の形・プロンプト・store・通知は利用側。

拒否条件や検査を足す前に問う: **その検査が無いとコントローラの配管が定義できないか**

- 定義できない（uid 未設定 → 別の uid を選べない、workspace 未マウント → 答えの置き場が無い）→ 提供側
- 定義はできるが結果が悪いだけ（root、privileged、egress）→ ポリシー層（PSA / Kyverno）の話。拒否しない

レビュー指摘は「正しいか」と「誰の持ち物か」を別々に判定する。セキュリティ指摘は「対処しない」と
言いにくいので、正しい事実がそのまま提供側に入り込みやすい。

## 作業

- コードの変更は `/code-review-cycle` を通してから commit / push
- `make test`（envtest）。`*_types.go` を触ったら `make manifests generate`
- ローカルの `make lint` は Go 1.27 × golangci-lint で staticcheck が panic する。CI の aqua 版が正
- 利用側（home-cluster の handler / kustomize ref）は別 PR で追従する。CRD → コントローラ → handler の順
