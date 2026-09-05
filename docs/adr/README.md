# ADR — 決定の記録

設計の**現在の姿**は `docs/design.md`。ここには**なぜそうなったか**を 1 決定 1 ファイルで置く。
design.md を書き直すと経緯が本文に埋まるので、覆した判断は ADR で残す。

- 決定文 10〜20 行: status / 決定 / 覆したもの（あれば）/ 覆すには / ポインタ
- 経緯・実測は issue / PR / design.md の該当節に置いて参照する
- status は `accepted` / `superseded`。番号は連番、再利用しない
- 既存の判断（design.md §11 の却下表）は遡って起こさない。次に触ったものから

| # | status | 決定 |
|---|---|---|
| [0001](0001-inject-sidecars.md) | accepted | prepare / publish サイドカーはコントローラが注入する |
| [0002](0002-per-task-workspace-pvc.md) | accepted | フェーズ間の引き渡しは Task ごとの PVC、claim は flow が書く |
| [0003](0003-run-view-and-sweep.md) | accepted | subPath 無し = この run のビュー、残骸は prepare が明示リストで掃除 |
| [0004](0004-run-id-counts-runs-not-attempts.md) | accepted | runID は決着した run を数える。インフラ再試行では動かない |
| [0005](0005-vocabulary-at-the-mount-root.md) | accepted | 語彙はマウント直下。`out/` という中間階層は置かない |
| [0006](0006-taskflow-admission-webhook.md) | accepted | TaskFlow の構造検査は admission webhook。証明書は配置側から与える |
| [0007](0007-no-resolved-spec-hashes.md) | accepted | 解決済み spec のハッシュを持たない。走行中の run は Job の immutability が守る |
