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
