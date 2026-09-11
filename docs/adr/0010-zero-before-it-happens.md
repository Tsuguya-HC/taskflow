# ADR-0010 宣言された終端は、起きる前から 0 として出す

- **status**: accepted（2026-09-12、人間の承認）
- **根拠**: issue #125。`taskflow_task_outcomes_total` の子 series が最初の `Inc()` で値 1 として
  生まれるため `increase(...[5m])` が立ち上がりを見られず、**人間に届けるための alert が、該当する
  終端が実際に起きた 2 回とも鳴っていなかった**（本番 Prometheus で実測）

**決定**:

1. **宣言された終端を、起きる前から 0 として出す。** 対象は `taskflow_task_outcomes_total`
   （flow ごとに「終端フェーズ 1 つにつき 1 行」と、予約語 `Escalated` / `Failed` の 2 行）と、
   `finally` を宣言した flow の `taskflow_finally_outcomes_total`（`Declared` / `NoAnswer` の 2 行）の
   両方。同じ「最初の `Inc()` で 1 として生まれる」問題を抱えている点で同型で、片方だけ塞いでも
   もう片方でまためったに起きないものほど見えない、が成り立つ。severity / outcome はどちらも
   フェーズ単位で一意に決まるので、**直積にはしない**。系列数は宣言された flow の数とその終端数で
   決まり、上限は git が持つ
2. **打つのは Task が flow を解決した時。`TaskFlow` の reconciler は新設しない。** 0 が要るのは
   「その flow の Task が終端に着く**前**」であって「TaskFlow が存在する瞬間」ではない。Task の
   reconcile は終端より必ず前に走るので、既存の経路に相乗りすれば足りる — フレームワーク自身の
   終端で既に止まって finally を待っている Task がコントローラ再起動後に最初に通る、予約フェーズの
   早期 return 分岐でも同じ関数を呼ぶ（冪等なので二重に呼んでも害はない）。watch を 1 本増やしても
   届く先は同じで、**それが無いと配管が定義できない**わけではない（CLAUDE.md の物差し）
3. **消えた flow の 0 系列は掃除しない。** 0 の系列は alert を鳴らさず（実測）、コントローラが
   入れ替われば消える。掃除のためだけに TaskFlow を watch して削除を検知する配管は、
   払うものが得るものを超える。cert-manager は同じ問題に 13 か月かけて pull 型 Collector へ
   全面移行したが、それは Certificate ごとに series が増える設計だったからで、**こちらは
   git にある flow の数で閉じている**
4. **`<unresolved>` の 1 行は起動時に出す。** `flow` が解決しなかった場合に出るのは
   `(<unresolved>, Failed, Failed)` ただ 1 通りで、flow に依存しない。`fail()` は必ず `Failed` に
   着くので、phase と severity がこれ以外の値と組むことがない
5. **gauge は足さない。** 「今どれだけの Task が人間を待っているか」は counter ではなく gauge の
   問いで、Argo が `argo_workflows_count`（名前は counter・実体は gauge）を作り直した issue #12589 と
   同型の取り違えがここにもある。ただしそれは **この ADR とは別の判断**。counter を正しくすることは
   後から gauge を足す道を塞がない

**覆したもの**: 無し。design.md §5 の表は「metric は `EndingRunning` 以外**常に**出る」と書いており、
これはもともと「終端の種類で出し分けない」という意味で実装と一致していた。合っていなかったのは
その直後の「**まだ宣言していない flow がダッシュボードで見つかる**」の方で、起きるまで series が
無ければ見つからない。文書が約束していたものを実装が満たす側に寄せる。

**なぜ先行者が誰もやっていないのに、やるのか**: 一次資料で 5 件当たって、**counter を 0 初期化して
いる成熟したプロジェクトは 1 件も見つからなかった**。Prometheus 本体の自己監視 mixin は
`increase(counter[5m]) > 0` を無防備に使い、Tekton は問題として認識した形跡すらない。それでも
彼らが困らないのは、対象の counter が頻繁に動くか、取りこぼしても次の機会に気づけるからで、
**taskflow の `Failure` / `Escalated` はどちらでもない**。design.md §9 が「人間が見るべき瞬間」と
定義したものは、その定義上めったに起きず、そして metric は**人間に届く唯一の経路**になっている
（§5 の表で Event は `Failure` だけ、条件は kubectl を見に行った人にしか届かない）。
めったに起きないものを取りこぼす向きに壊れているのは、ここでは相場の問題ではなく約束の問題。

公式の `instrumentation.md`「Avoid missing metrics」は 0 の export を推奨しているが、
**ラベル値が実行時にしか分からない場合には触れていない** — 記録された唯一の限界がそこで、
`taskflow` のラベルは TaskFlow が git で宣言するものなので有限・既知であり、その限界に当たらない。
利用側だけで閉じる道が無いことも確かめた（prometheus/prometheus#1673 は「一般には解けない」として
2017 年に close、`unless ... offset` はメンテナ本人に「哲学に合わないハック」と言われている）。
upstream の最終的な答えは provider 側（OpenMetrics の created timestamp ingestion）で、
**コントローラでの 0 初期化はその簡易版にあたる**。3.14.0 時点でも feature flag 裏の experimental なので、
それが既定になったら 2 と 4 は要らなくなる。

**覆すには**: created timestamp ingestion が既定で効くようになったとき（0 初期化は不要になる）。
あるいは flow あたりの終端数が git で管理できない規模になったとき — その時に問うべきは
「0 を減らすか」ではなく「counter のままでよいか」（決定 5）

**未解決**:

- **非 leader の replica には 0 系列も出ない。** Task を reconcile するのは leader だけなので、
  0 を打つのも leader だけ。`increase()` は `pod` ラベルを含む系列ごとに評価されるので、leader 側で
  立ち上がりが見えれば alert は鳴る（leader 交代の直後に起きる終端まで含めて実測で確認した）。
  ただし「どの replica が leader か」を metric から読む手段は無い。Argo は `is_leader` gauge を
  出して Prometheus 側の join に委ねているが、ここでは要るかどうかがまだ分からない
- 0 初期化した系列が、コントローラの再起動をまたいでどう見えるかは promtool の合成系列で確認しただけで、
  本番での rollout を挟んだ観測はこれから
