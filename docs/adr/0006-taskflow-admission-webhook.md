# ADR-0006 TaskFlow の構造検査は admission webhook が行い、証明書は配置側から与える

- **status**: accepted（2026-09-05、人間の承認）
- **根拠**: issue #17。design.md §5「厳格検証（admission / CEL）」が機構を 2 つ並べたまま
  決めていなかったので、どちらで何を弾くかを確定させる

**決定**:

1. **TaskFlow の create / update は ValidatingAdmissionWebhook が検査する。** design.md §5 の表の
   うち、**単一の TaskFlow 内で閉じるもの全部**を Go で 1 箇所に書く（`start` からの到達性、終端に
   到達する経路の存在、同じ binding での判定ディレクトリ重複、`Failed` を `next` の行き先に、
   予約語を `bindings` のキーに、`start` が未束縛、判定ディレクトリ名の形と `.prepared-by` の予約）
2. **graph 系を CEL では書かない。** CEL は推移閉包を書けない。取れる代案は 2 つあり、どちらも
   採らない — 「誰も指していないフェーズ」だけを見る弱い版は本体から切り離された閉路を取り
   こぼす。フェーズ数に上限を入れて N 段展開する版は、実装の都合で API に「1 flow のフェーズは
   最大 N 個」を課したうえ、式の正しさが実行するまで分からなくなる（P9 が宣言側に禁じたものを
   検証側で自分がやる形になる）
3. **証明書は配置側が与える。** taskflow は `--webhook-cert-path` の下のファイルを読むだけで、
   cert-manager を知らない（`cmd/main.go` は scaffold の時点で既にこの形）。
   `ValidatingWebhookConfiguration` は caBundle 空で配り、cert と caBundle をどう供給するかは
   home-cluster の裁量（design.md §14 の分担そのもの）。これで増える依存は
   `admissionregistration.k8s.io/v1` だけ、すなわち core API のみで、§7 の
   「**サードパーティ依存はゼロ**」は保たれる
4. **別オブジェクトを参照する検査は webhook に入れない。** handler の実在と `spec.phase` の一致は
   定義時に弾いてはいけない — ArgoCD は TaskFlow と TaskHandler を同じ sync で撒くので、handler が
   まだ無いことを理由に TaskFlow を拒否すると適用順で詰む。design.md §8「遅延バインディングの
   代償」の立場（framework は実在確認をしない）を維持し、実行時の `brokenFlow` → `Failed` のまま
5. **admission が走ったことに依存する実装にしない。** `internal/transition` の実行時検査は
   webhook が入っても残す。編集済みの flow が走行中のタスクの足元に残るため（#19）

**覆したもの**: なし。design.md §5 が未決のまま並べていた 2 機構を確定させただけ

**覆すには**: CEL で推移閉包が書けるようになったとき（決定 2 が消える）。あるいは webhook の
不在が TaskFlow の作成を実際に妨げた実測が出たとき（決定 1 の置き場所が変わる。§5 の見出しが
「矛盾したら作らせない」である以上、`Accepted` condition への後退は代案にならない）

**未解決**:

- ~~`failurePolicy` を `Fail` にするか~~ → **`Fail`**（実装時、#17）。`Ignore` を採らないのは、
  コントローラが落ちている間だけ検査が黙って消える形になるからで、これは「設定は書いてあるが
  効いていない」層そのものになる。

  **「コントローラは replica 1 なので rollout 中は数秒拒否される」という当初の懸念は、配置側が
  引き取った**（home-cluster PR #803）。webhook を入れたことで「コントローラが落ちる」の意味が
  reconcile の遅延から TaskFlow の書き込み拒否へ変わるので、あちらが `replicas: 2` +
  `PodDisruptionBudget(minAvailable: 1)` + ノードをまたぐ `podAntiAffinity` + `maxSurge: 0` /
  `maxUnavailable: 1` を入れている。可用性は配置側の持ち物であって、この ADR が決めることでは
  なかった。

  **残る窓は 2 つで、どちらも実測していない**: cert を注入する側（home-cluster では cainjector）が
  `caBundle` を埋めるまでの間と、drain と rollout が重なった場合。2026-09-05 の初回投入では
  どちらも踏まずに通ったが、**1 回通っただけで、意図的に起こして測ってはいない**
- **Task にも webhook を掛けるかは決めない。** Task を作るのは cron / Argo Events / エージェントで、
  TaskFlow を作る GitOps とは経路も頻度も違う。まとめて決める根拠がまだない
- profile の必須フェーズ（#18）は機構の問題ではなく未決。profile はフェーズ**名**を要求する形で
  書かれているが、名前は利用側が決める（`調査`/`報告` と `レビュー`）。Discussion #2
