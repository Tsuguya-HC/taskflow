# フェーズ管理エージェント実行基盤（設計メモ）

status: draft / 未着手
date: 2026-08-21

クラスタ内で自律エージェントを「非決定論な部分だけ隔離し、周りは全部決定論」で回すための基盤。
既存の Argo Workflows / Kata / Kyverno / CNP / SeaweedFS S3 の上に薄く乗せる。

---

> 覆した判断とその理由は `docs/adr/` に 1 件 1 ファイルで残す。この文書は現在の姿。

## 1. 目的

エージェントに投げるタスクを **フェーズを持つ状態機械** として扱い、以下を毎回書かずに済ませる。

- フェーズごとの最小権限（調査フェーズは push できない、を構造的に保証）
- 判定不能を承認に化けさせない（fail-closed）
- 差し戻しループの回数制限
- 実行後の掃除（K8s 内 / 外）
- エスカレーション経路

**本質的な価値は「無人で回せるようになること」。** cron に載せて忘れられる状態を作るのが目的で、
個々の機能ではない。

---

## 2. 設計原則

| # | 原則 | 理由 |
|---|---|---|
| P1 | **エージェントは制御フローを持たない** | 判定（verdict）だけ返す。次フェーズはコントローラの遷移表が決める。プロンプト内に分岐があるとテスト不能・無限ループ不能 |
| P2 | **コントローラはポリシーを持たない** | SA / Role / CNP を生成しない。既存のものを名前で参照するだけ。生成すると全権限の和集合を持つ最大の攻撃対象になる |
| P3 | **Pod は使い捨て、ボリュームが真実** | プロセス状態に耐久性を賭けない。ノード再起動が日常的に起きる前提 |
| P4 | **制御フローの入力は成果物であって発話ではない** | 最終メッセージは切れる・拒否される・ドリフトする。ファイルの有無だけを見る |
| P5 | **etcd には制御状態だけ。中身は入れない** | ログ・結果・レポートは S3 / PG。status は参照のみ。etcd 障害を 2 回踏んでいる |
| P6 | **判定不能は絶対に pass に倒さない** | 答えが 1 つに定まらないなら直接 Escalated |
| P7 | **コントローラは LLM を知らない** | プロンプト・モデル・API キー・store は全てユーザーの pod spec の話。コントローラの語彙は「フェーズ・Job・verdict」だけ |
| P8 | **厳格に検証し、矛盾したら即座に終わる** | デフォルト値・暗黙のマージ・推測による修復をしない。構造的な矛盾は修復せず `Failed`。曖昧なまま進むより止まる方が安い |
| P9 | **宣言に式を書かせない** | 遷移は `next` のテーブル引きだけ。条件式・算術・テンプレート分岐を入れると、正しさが実行するまで分からなくなる |

### 式を持たないこと（P9）

**値の差し込みは許す。分岐と算術は許さない。**

| | 許す | 許さない |
|---|---|---|
| 遷移 | `next: {報告: ok, 調査: more}` | `when: "... == pass && budget > 0"` |
| 予算 | 実行時にコントローラが数える | `{{=asInt(budget) - 1}}` |
| 入力 | `input` の値をプロンプトへ差し込む | 入力の値による分岐 |

理由は**検証可能性の一点**。テーブルは create 時に全件検査できる — 未知の verdict、
到達不能なフェーズ、`next` の欠落、同じディレクトリを指す 2 ステータス。式は**走らせるまで正しさが分からない**。

§19 で測ったとおり、Argo では `- 1` を `+ 1` と書いても止まらず、`when` 2 本が補集合になって
いるかも誰も見ない。表現力があることと、書き間違いを検出できることは別で、
**式を足すたびに検出できる範囲が狭くなる。**

そして、**条件式が必要になった時点でそれは Argo でやればいい。**

Argo は式を持ち、合成が開いていて、想定外のことができる（§19）。そこに勝とうとして
CRD 側へ式を足すのは、**劣化した Argo を作ること**でしかない。共存の意味が消える。

| 要るもの | 使うもの |
|---|---|
| 分岐・合成・想定外のこと | Argo |
| 実行前に検証できること | この framework |

境界は好みではなく、**それぞれが何のためにあるか**で決まっている。だから
「Argo でできること」を後から取り込みたくなっても、**条件式だけは取り込まない**。
取り込んだ時点で、この framework は Argo の劣化版になり、
同時に「規約 + linter で足りる」という選択肢に戻る（Discussion #1 が問うている差そのもの）。

### 誰の責務か

設計中に何度も「これはコントローラの仕事か」を取り違えたので明示しておく。

| 誰 | 何を決めるか | どこで担保されるか |
|---|---|---|
| **handler の作者** | 権限・Pod の形・何を実行するか | git + PR レビュー |
| **TaskFlow の作者** | トポロジ（どのフェーズを誰が埋め、どこへ行くか） | git + PR レビュー |
| **RBAC** | **どの SA がどの flow を起動してよいか**（namespace 経由） | 素の RBAC |
| **コントローラ** | 遷移・判定の回収・実行の同一性・上限・掃除 | コード |
| **プロンプト** | 考え方（何を見るか、何を重要とするか、どう報告するか） | ConfigMap |

**設定とプロンプトの分界**は実測に基づく（§17）:

| | 設定で持つ | プロンプトで持つ |
|---|---|---|
| 何を | トポロジと権限 | 考え方 |
| 検証 | 実行前に admission で | 実行しないと分からない |
| 破れるか | 破れない | モデル次第 |

プロンプトは「**確かめさせる**」には強く（根拠の併記要求は毎回効いた）、
「**保証させる**」には弱い（微妙な事実は 3 回中 1 回）。だから安全性に関わるものを
設定側に置くのは思想ではなく測定に基づく判断。

ただし**設定側に置けるのはトポロジと権限だけ**で、それも大半は git のレビューが担保する。
コントローラが審査すべきものは思っていたより少ない。

---

## 3. レイヤ構成

| 層 | 担当 | 実体 |
|---|---|---|
| 意図・フェーズ・監査 | Task | 自作コントローラ |
| 実行環境の形 | （将来）SandboxTemplate | 上流 kubernetes-sigs/agent-sandbox |
| 1 フェーズの実行 | Job | **batch/v1（組み込み・依存なし）** |
| タスクの起票・定期実行 | 任意 | Argo CronWorkflow / CronJob / 人間（コントローラの関知外） |
| 権限・ネットワーク | SA / Kyverno / CNP | home-cluster の GitOps（既存） |
| 人間の窓口 | horenso | 既存（出力先のひとつ） |

**コントローラは Argo に依存しない。** フェーズグラフをコントローラに移した時点で 1 フェーズの実行は
「1 ステップ」であり、ワークフローエンジンを必要としない。素の Job がちょうどその primitive。

Argo Workflows は引き続きタスクの起票（CronWorkflow / Argo Events）に使うが、それは**利用側の都合**であって
コントローラの依存ではない。CronJob でも人間の `kubectl create` でも等価に動く。

---

## 4. API

### 型と実体を分ける

**タスク型**（cnp-check とはどういうフローか）と**タスク実体**（今夜の 1 回）は別物。
CronJob→Job、WorkflowTemplate→Workflow と同じ関係にする。

同居させると、実体を作る側が毎回グラフを丸ごと再埋め込みすることになり、
**グラフを変えたら全ての生成元を直す**羽目になる。運用として壊れている。

そして自律運転では**実体を書くのが機械**になる。31 行の YAML を機械に書かせれば
31 行ぶんの間違え方があるが、`flow` 名 + `input` の 4 行なら壊し方がほとんど無く、
残りは admission が検証する。**分割は「あると綺麗」ではなく必須。**

### API group と kind（確定）

```
apiVersion: flow.tgy.io/v1alpha1

kind: TaskFlow      # 型     — トポロジ（どのフェーズを誰が埋め、どこへ行くか）
kind: TaskHandler   # 型     — 誰が埋めるか（権限・Pod の形・何を実行するか）
kind: Task          # 実体   — flow と input だけ
```

実体を `AgentTask` から **`Task`** に変えた。`AgentHandler` → `TaskHandler` の理由
（handler は lint / CI / 人間になりうるので誤称）が実体側にもそのまま当てはまり、
**エージェントが実行するとは限らないものを `Agent*` と呼ぶ**のは同じ誤りだった。
接頭辞が `Task*` と `Agent*` に割れていたのも、その名残でしかない。

group は `flow.tgy.io`。**リソースとして存在するのは Flow / Handler / Task の 3 つで、
「フェーズ」は `TaskFlow` の中のフィールドでしかない**ため、`phases.tgy.io` は
リソースでないものを主語に置くことになる。group は `argoproj.io` / `tekton.dev` / `batch` と同じく
その API 面の主語を指すもので、ここでの主語は flow。

`tasks.tgy.io` も技術的な衝突は無いが、完全修飾が `tasks.tasks.tgy.io` になるだけで意味が増えない。
クラスタに `Task` を名乗る kind は無く（Argo のものは `WorkflowTask*` 接頭辞）、
`kubectl get task` は曖昧にならない（実測）。

**この決定は CRD を 1 行も書いていない時点で行った。** 書いた後だと API バージョンの話になる。

### TaskFlow（型 / **namespaced**、GitOps）

```yaml
kind: TaskFlow
metadata: {name: cnp-check}
spec:
  profile: investigate        # 必須フェーズを規定する検証スキーマ（コントローラ組み込みの enum）
  bindings:                   # 各フェーズを誰が埋めるか + 次にどこへ行くか（両方必須）
    調査:                      # ステータス名は利用側が決める
      handler: cnp-reader
      next:                   # 「このステータスへ行くには、このディレクトリに書く」
        報告: ok
        調査: more
    報告:
      handler: discord-notify
      next: {おわり: sent}     # おわり は束縛が無い = そこで止まる
  reworkBudget: 2
  maxInFlight: 2              # この flow の同時実行数上限
  ttl: {succeeded: 1h, failed: 168h}
```

厳格検証（到達性・必須束縛・予約語・自己レビュー禁止）は **TaskFlow の作成時に一度だけ**走る。

### Task（実体 / namespaced）

```yaml
kind: Task
spec:
  flow: cnp-check
  input:                      # タスク固有の入力。in/input.json に落ちる + プロンプトのテンプレート変数
    scope: "all namespaces"
  dedupKey: "issue-1234"      # 任意。同じキーの実行中タスクがあれば作らない
```

**実体は 4〜6 行。** 生成元（cron / Argo Events / エージェント）はグラフを知らなくてよく、
グラフが変わっても無変更で済む。

### namespace を権限の階層にする

`spec.flow` は **常に同一 namespace で解決する**。参照先の namespace を指定するフィールドを作らない。

これで「どの flow を起動できるか」が「**どの namespace に Task を作れるか**」に還元され、
**素の RBAC で表現できる**。RBAC はフィールド値では絞れない（`resourceNames` は既存オブジェクトの
get/update/delete 用）ので、当初は ValidatingAdmissionPolicy を持ち出していたが、
問題の方を namespace に寄せれば admission policy は要らない。

```
agent-tasks-safe/    ← Issue 起票エージェントの SA に create を付与
  TaskFlow:    rss-fix, doc-audit
  TaskHandler: 読み取り or 単一リポ push のみ

agent-tasks-infra/   ← 人間と CI だけ create 可
  TaskFlow:    infra-modify
  TaskHandler: クラスタ書き込み権限あり
```

「namespace を跨げない」は policy ではなく **API の形**（参照に namespace フィールドが無い）なので、
CEL も webhook も無しに破りようがない。

副次的に、**その namespace の中だけを見れば何ができるか分かる**状態になる — どの handler があり、
どの SA を使い、どの CNP と PSA が掛かるか。クラスタの他の部分が既に namespace 単位で
切られているのと同じ粒度で、新しい概念を持ち込まずに済む。

代償は**階層をまたいで同じ flow を使うとき定義が重複する**こと。暗黙に共有されるより、
権限階層ごとに明示的に書かせる方が正しいと判断する。重複が苦になったら kustomize で
namespace ごとに生成すればよく、cluster-scoped な種類を足す理由にはならない。

### 階層をまたぎたくなったら、framework の外でやる

タスクの連鎖が権限階層をまたぐ必要が出たら、**cross-namespace の参照を足さない**。
成果物を S3 に置き、Argo Events で信号を送り、**受け手側の namespace が自分のタスクを起こす**。

機能が無いこと自体が安全性になっている。`nextTask` や cross-ns の flow 参照を提供すると、
低権限側が高権限側を起動できるようになり、namespace で解いた問題がそのまま戻る。

> 直接参照だと**送り手が決める**。S3 + event なら**受け手が決める**。権限境界としては後者が正しい。

機構は既にある（`runner: External` と同じ形で、Argo Events + SeaweedFS S3 + webhook）。
新設するものは無い。

注意点は 1 つ。**`ownerReference` も namespace を跨げない**ので、チェーンの GC と追跡は
framework の外になる。独立した 2 本の Task になり親子関係が残らないので、
`input` に相関 ID を持たせてログとレポート側で辿れるようにしておく。

**先例として `ClusterWorkflowTemplate` がある。** 当初 cluster-scoped にしようとしていたのは
あれと同じ「cluster-scoped なテンプレートを namespace から参照する」形で、癖もそのまま引き継ぐ
— 誰が使ってよいかを絞れない、RBAC の見通しが悪くなる、GitOps でどのアプリが所有するのか
決まらない。手元に先例があるので繰り返さない。

実体は `status.currentRun.flowHash` に解決済み型のハッシュを持つ。
走行中に型が変わったら `Failed`（P8）。以下の旧 `spec` フィールドは TaskFlow 側へ移動:
`profile` / `bindings` / `reworkBudget`
status:
  phase: Review
  runID: 3                    # 単調増加。パスと子リソース名に使う
  reworkBudget: 1
  currentRun: {phase: Review, runID: 3, deadline: ..., workflowName: ...}
  artifacts: {store: "s3://.../<task-uid>/", pr: "..."}
  history: [...]              # 上限付きリングバッファ
  conditions: [...]
```

### TaskHandler（**namespaced**, GitOps）

フェーズを埋める「箱」。先に作っておいて、使うときに名前で指名する。

```yaml
kind: TaskHandler
metadata: {name: claude-reviewer}
spec:
  phase: Review
  runner:
    type: Job                         # 組み込み。将来 Sandbox を足す口
  workspace:                          # 判定の回収に使うボリュームの所在（§7）
    volume: work
    mountPath: /workspace
  jobTemplate:                        # ← 自前の最小型（§4「自前型の理由」）
    template:
      spec:
        runtimeClassName: kata
        serviceAccountName: agent-readonly   # 既存 SA を参照するだけ（生成しない）
        restartPolicy: Never
        securityContext: {runAsUser: 65533}  # 必須。run ディレクトリはこの uid に対して閉じられる
        volumes:
          - {name: work, emptyDir: {}}
        containers:
          - name: agent
            volumeMounts: [{name: work, mountPath: /workspace}]
            ...
  timeout: 30m
  maxInfraRetries: 2
```

**フィールドはこれで全部。** プロンプト・モデル・API キー・store・通知先は 1 つも無い。
宣言ディレクトリを敷く init コンテナと判定を読み戻すサイドカーは**コントローラが Job 生成時に足す**ので、
handler の作者はその存在を知らなくてよい（§7）。

### handler は AI 専用ではない（P7 の見返り）

`jobTemplate` を丸ごと持つので、**lint / test / 任意のスクリプトがそのまま handler になる**。
`golangci-lint` を走らせて `/workspace/pass/` か `/workspace/attention/` に書くコンテナと、
LLM エージェントとの間に、コントローラから見た区別は存在しない。

`runner` は 3 種類:

| runner | 何をするか | 用途 |
|---|---|---|
| `Job` | `jobTemplate` を Job として起動 | **既定。** エージェント、lint、test、任意のスクリプト |
| `External` | **何も起動しない。** deadline を持ち、verdict サブリソースへの書き込みを待つ | 人間レビュー、GitHub Actions の結果、外部システム |
| `Sandbox` | （将来）長命 runner | — |

### Argo を runner に採らない

一度落とし、戻し、また落とした。理由づけが毎回変わっているので経緯ごと残す。

1. 「コントローラに Argo 依存が焼き付く」— 雑。包む単位の話と混ざっていた
2. 「フェーズ単位で包めば依存は問題にならない」— **依存が許容できるかは設計側が決めることではなかった**
3. **多段は 1 Pod で足り、Argo runner の固有価値は既存 YAML の再利用だけ。それは依存に見合わない**

`claude-code-generic` が複数ステップでやっていること（`check-existing-pr` → `claude-code` →
`create-signed-pr` → `notify`）は、実質ワークスペースを共有する逐次実行であり、
**cnp-check で実証済みの 1 Pod 構成**（initContainers → main → SIGTERM を trap する sidecar）で足りる。
むしろ 1 Pod・共有ボリュームなので、#568 で踏んだ「emptyDir がステップ間で共有されない」が
構造的に発生しない。失うのは既存 WorkflowTemplate の再利用だけで、中身はスクリプトなので移植で済む。

Argo runner を入れると、コントローラが `argoproj.io` の型を知り、Workflow を作り、status を watch し、
対象 namespace の RBAC を持つことになる。**Argo が無い環境で動かないもの**になり、
`batch/v1` + `core/v1` だけという性質（§14 の public 化の根拠でもある）が崩れる。

将来どうしても必要になれば unstructured で書けば型の import は不要なので扉は閉じないが、**既定からは外す**。

### 包む単位はフェーズであってフロー全体ではない

runner の選択とは別に、**包む単位**という論点自体は残る。
仮に何らかのワークフローエンジンに載せるとしても、フロー全体を 1 つのテンプレートに包んではいけない。

| 包む単位 | 制御ロジックの置き場 | 検証 |
|---|---|---|
| フロー全体を 1 つの WorkflowTemplate に | `when:` 式と `{{=asInt(budget) - 1}}`（YAML の中） | **実ワークフローを走らせるしかない** |
| **フェーズ 1 つずつを Workflow に** | **コントローラの遷移表** | **全ペアを単体テスト** |

フェーズ単位で包めば、**`when:` も再帰も予算の算術も YAML に一切現れない**。
コントローラが「今は Review、handler は X」と決め、その 1 フェーズを submit するだけで、
投げられる Workflow は毎回一直線になる。それでいて Argo の artifact / retry / UI / exit hook は残る。

§19 で実測した通り Argo は循環も権限差し替えも外部 verdict も**書ける**。
問題は書けるかではなく、**書いたものを単体テストできるか**だった。

### runner は誰の視界に入るか

`runner` の可視性（`Job` / `External` / 将来の `Sandbox` の間でも同じ）:

| 層 | `runner` が見えるか |
|---|---|
| `Task`（実体、エージェントが書く） | **見えない**（`flow` と `input` だけ） |
| `TaskFlow`（型） | **見えない** |
| `TaskHandler`（人間が git で書く、6 個程度） | 見える。**handler ごとに 1 回だけ** |

フェーズ・タイムアウト・TTL・`status` の形・fail-closed の挙動は runner によらず共通なので、
`kubectl get task` の見え方もデバッグ手順も失敗の仕方も変わらない。

**消せない差が 1 つだけある**: handler が表現できることが違う（`Job` は 1 Pod、`External` は何も起動しない）。
これは隠すべきではない。隠すと「なぜ handler A でできて B でできないのか」が説明不能になる。
継ぎ目はここに置く。

`External` によって**人間 / GitHub CI / 任意の外部システムが 1 つの機構に統合される**。
verdict を書くのが人間の `kubectl patch` でも Argo Events 経由の webhook でも、
コントローラから見れば同じ。

### 1 フェーズに複数 handler

コードのレビューは lint + test + エージェントレビューを**全部**通したい。

```yaml
Review:
  handlers: [lint, test, agent-review]
  next: {検証: ok, 実装: rework, Escalated: stuck}
```

合成規則は**固定**（設定可能にすると DSL 化する）:

> **最も不利な結果が勝つ。** 単一の答えを出せなかった handler が 1 つでもあれば、フェーズ全体が
> 単一の答えを出せなかった扱いになり、直接 Escalated。

決定論的・全域・設定項目ゼロ。P1 とも一貫する。

**安いものを先に、はフェーズ順で解く。** フェーズ内に順序や `stopOnFailure` を足さない。

```
Implementing → Checks(lint, test) → Review(agent, human) → Verifying → Done
```

数秒で終わる決定論的なゲートを前段のフェーズに置けば、新しい機構を増やさずに済む。
全部ユーザーの pod spec 側（ConfigMap volume / `envFrom: secretRef` / サイドカー）に畳める。

結果、**コントローラは Claude を知らないし、LLM ですら知らない**（P7）。
「コンテナを走らせて verdict を受け取り、必要なら差し戻す」だけの汎用機構になる。
カナリア検証やマイグレーションの検証ステップにもそのまま使える。public リポにする理由もここで強くなる。

**`batchv1.JobTemplateSpec` は使わず、自前の最小型（`template.metadata` / `template.spec` だけを
持つ `JobTemplate`）で埋める。** 理由は 2 つ。一つは、`JobSpec` の大半（`backoffLimit` /
`ttlSecondsAfterFinished` / `activeDeadlineSeconds` / `completions` / `parallelism`）が§4 の
不変条件を壊す予約フィールドで、フィールドごと型から無ければ間違えようがない。もう一つは、
その `ObjectMeta`（Job にも埋め込まれた Pod テンプレートにも出てくる）を `controller-gen` が
properties 無しで出力し、structural schema がそこにぶら下がる pod のラベルを刈ってしまうことを
実測で確認したため（サーバに保存された値が `{}` だった。§20 CNP がラベルで選択する以上、
黙って消えるラベルは default-deny namespace で「通信が消える」という症状になる）。
代わりに使う `EmbeddedObjectMeta` は `labels` / `annotations` だけを素直な struct フィールドとして
持つので、この問題自体が起きない。
`kubectl explain taskhandler.spec.jobTemplate.template.spec.containers` が効き、覚えることが増えない。
コントローラ側は deep-copy して名前・owner・ラベルを立て、volume と env を注入して create するだけになる。

**コントローラは Pod の中身を組み立てない。** 注入するのは以下だけ:

- `ownerReferences`（Task 所有）と決定論的な Job 名
- ラベル `flow.tgy.io/task-uid`（**帳簿**。値は UID なのでラベル値の文字種制約に収まる）。
  **Job と Pod テンプレートの両方に付ける** — Job 側はコントローラが自分の Job を見つけるため、
  Pod 側は Task 単位で Pod を引くため。2026-08-26 まで Job にしか付いておらず、
  `kubectl get pods` や hubble で 1 つの Task の Pod をまとめて見るには
  `batch.kubernetes.io/job-name` 経由（= 先に run 番号を知る）しかなかった。
  完了検知が Pod を引くのはこのラベルではなく `batch.kubernetes.io/controller-uid`
  （その run の Job の Pod だけが要る）で、そこは変わらない

- env: `FLOW_TASK_UID` / `FLOW_PHASE` / `FLOW_INPUT`（`spec.input` の JSON）
- annotation: `flow.tgy.io/phase`（UTF-8 のまま）、`flow.tgy.io/run-id`、
  `flow.tgy.io/prev-run-id`（初回は付けない）
- `backoffLimit: 0`（Job 内リトライを止める。インフラ起因の再実行は同じ runID のまま
  attempt を進めた名前で Job を作り直す。ADR-0004）
- `activeDeadlineSeconds`（handler の `timeout` から）

**フェーズ名はラベルに入れない。** ステータス名は利用側の自由文字列なので、
`調査` のような値は K8s のラベル値として通らない（実測: `Invalid value: "調査"`）。
それ以前に、**ポリシーが選択するためのラベルを framework が決めるのは越権**（§20）。
ポリシーが何を選ぶかは利用側が `jobTemplate` に書く。フェーズ名は annotation に置けば
UTF-8 のまま入り、長さの制約も無い。

**Job 名も同じ理由でフェーズ名を直接使えない**（RFC 1123）。
`<task>-<runID>-<フェーズ名のハッシュ 8 桁>` にする。インフラ再試行では runID が動かないので、
2 回目以降は `<task>-<runID>-r<attempt>-<フェーズ名のハッシュ>`（ADR-0004）— 初回の綴りは変えない。task 名が長くて 63 文字に収まらない
ときは、切り詰めた task 名の横に**全体のハッシュ 8 桁**を添える — 名前の判別部分は末尾に
来がち（`generateName` のサフィックス等）なので、尻尾を捨てるだけだと途中まで同じ
2 つの task が同じ Job 名に衝突する。読みたい人は annotation を見る。

決定論的な名前は、コントローラが再起動して同じ run を作り直しても `AlreadyExists` に
なる、というところまでしか保証しない。**決定論的であって排他的ではない** — Job を
create できる誰かが先にその名前を取れる。だからコントローラは名前の下に居る Job を
採用する前に必ず ownerReference（controller かつ Task の UID 一致）を検証し、他人の
Job なら結果を流用せずエラーにする。冪等性は「名前が同じ」ではなく
「名前 + 所有権検証」で成り立っている。

`FLOW_INPUT` は env なのでサイズ上限がある。大きい入力はユーザーが store 経由で取りに行く
（それも pod spec の話であってコントローラの関知外）。

**`$(...)` はリテラルで届く（利用側の契約）。** K8s は env の値の `$(VAR_NAME)` を、同じ
コンテナで先に解決された変数（envFrom の全部と、リストで前にある env）へ展開する。
`spec.input` とフェーズ名は作者が自由に書ける文字列なので、コントローラは注入時に
`$(` を `$$(` へエスケープする。`spec.input` に書いた `$(FOO)` はコンテナに文字列
`$(FOO)` のまま届き、**展開されることは決してない** — 展開に頼る入力は書けないし、
handler の secret が `FLOW_INPUT` / `FLOW_PHASE` 越しに漏れることもない。注入 env を
リスト先頭に置く並びも維持しているが、それが効くのは env リスト経路だけで、envFrom で
引いた secret は先頭の env からでも解決できる。**穴を塞いでいるのはエスケープ**。

### run 番号は annotation で渡す（env ではない）

**値は提供するが、どのコンテナに引き込むかは利用側が決める。**

```yaml
- name: publish
  env:
    - name: RUN_ID
      valueFrom:
        fieldRef:
          fieldPath: metadata.annotations['flow.tgy.io/run-id']
```

`fieldRef` が読むのは **Pod の** annotation であって Job のではない。なのでコントローラは framework の
annotation（`flow.tgy.io/*`）を Job と Pod template の両方に付ける（2026-08-25 まで Job にしか
付いておらず、上の YAML は実は届いていなかった）。ラベル `flow.tgy.io/task-uid` も同じく両方に付く。
template 側が `flow.tgy.io/` を名乗っていたら**ラベルも annotation も**上書きせず拒否する
（書いた値と違うものが走る方が悪い）。

run 番号が要るのは**配管**であってエージェントではない:

| | 要るか | なぜ |
|---|---|---|
| init / publish サイドカー | 要る | S3 のような backend では `results/<runID>/` へ上げる先を知る必要がある |
| エージェント（main） | **要らない** | `<path>/results/1/ok` を開けば読める。数える必要が無い |

しかもエージェントに渡すのは**避けたい**。実測に基づく理由がある — implement フローで
レビュアーに**残りの差し戻し回数を伝えない**ようにしたのと同じで、「あと 1 回しかない」を
知ると判定の基準が動く。run 番号にも「3 周目だからそろそろ通そう」が起こりうる。

**全コンテナに env を注入すると、エージェントに見せない選択肢が利用側から消える。**
annotation なら「配管だけが読む」が書ける。

flow workspace（PVC backend）での per-run パスの扱いは §7「ワークスペースのレイアウト」と
[ADR-0002](adr/0002-per-task-workspace-pvc.md) 決定 5 を参照。あちらではコントローラが生成時に
`subPath: work/<runID>` を決定論的に焼き込み、封印後は publish が rename で `results/<runID>` に
移す — annotation の fieldRef を挟む余地も、どちらを採るか利用側が選ぶ余地も無い。上の
「annotation で渡すか env で渡すか」は、あくまで handler が自分の作るコンテナに run 番号を
引き込みたい場合の一般論であって、framework 自身の prepare / publish の配線には効かない。

RuntimeClass・SA・volume・エージェントのコンテナは全部 home-cluster 側の YAML のまま。
P2 の分離が保たれる（prepare / publish はコントローラの側に移った — §7）。Pod 層（`template.spec`）は `corev1.PodSpec` 標準そのもの、Job 層だけが
自前型（予約フィールドの節を参照）で、どちらも依存は K8s core API のみ —
**サードパーティ依存はゼロ**のまま。

### 予約フィールド（担保の実態は 2 種類に分かれる）

JobTemplateSpec をそのまま開放すると、設計の不変条件をユーザーが壊せてしまう。
黙って上書きすると「書いた値と違う」で混乱するので、書けないようにするか拒否するかのどちらかにする。

| フィールド | 拒否する理由 | 担保の実態 |
|---|---|---|
| `backoffLimit`（0 以外） | リトライ機構が 2 つになる。再試行してよいのは「何も走らなかった」ときだけで（`collect.Ran`）、それを判定し attempt を数えるのはコントローラ。Job 内リトライは走った handler を数えずに黙ってやり直す | **型から除去。** `JobTemplate` に対応するフィールドが無く、書けない |
| `ttlSecondsAfterFinished` | verdict 回収前に Job が消える。掃除は Task の TTL + ownerRef に一本化 | 同上 |
| `activeDeadlineSeconds` | `spec.timeout` が唯一の真実。コントローラが Job に書き込む（kubelet 側でも効かせる二重化） | 同上 |
| `completions` / `parallelism`（1 以外） | 1 run = 1 verdict が壊れる | 同上 |
| `restartPolicy: OnFailure` | 同上（Pod 内再起動で run ディレクトリに前回の残骸が残る）。`Never` のみ許可 | **reconcile 時に Go コードで拒否**（`internal/runner/job.go` の `checkReserved`）。Task が動く時点で `Failed` になるのであって、`TaskHandler` の作成自体は防げていない |

上の 5 種は型からフィールドが消えているので、CRD の validation で「拒否する」対象がそもそも
存在しない。CEL で書けているのは `phase` に `Escalated` / `Failed` を予約するルールだけ
（`taskflows` 側の CEL validation は 0 件）で、`restartPolicy` を admission で落とす仕組みは無い。
admission での拒否（`TaskHandler` の作成自体を止める）は #17 の範囲。

書き間違いが何になるかは fieldValidation 次第（envtest 実測 2026-08-24）: Strict（kubectl の
既定）では `unknown field "spec.jobTemplate.backoffLimit"` の明示エラー。Warn / Ignore では
**黙って刈られて保存される** — 書いた本人は効いていると思い込める。ただし CronJob の形
（`jobTemplate.spec.template`）は全モードで弾かれる: `spec` が刈られた結果、必須の `template`
が欠けて required エラーになる。黙殺が成立するのは「非 strict クライアント + 予約フィールドの
追記」の組み合わせに限られる。

**JobSpec の残り全フィールドの処遇。** 型除去は「検討して除外した」と「視界から消えて
考慮漏れした」の区別を消す（実例: `podReplacementPolicy` は K8s 1.28 で増えたが、この表を
書くまで検討の机に載らなかった）。なので全量を一度ここに書く。**K8s を上げて JobSpec に
新フィールドが増えたら、この表に 1 行足してから判断する**:

| フィールド | 処遇 |
|---|---|
| completionMode / backoffLimitPerIndex / maxFailedIndexes / successPolicy | Indexed Job 系。1 run = 1 verdict に反するので不要 |
| selector / manualSelector | Job controller の既定に任せる。手動 selector は事故の元でしかない |
| suspend / managedBy | キューイング・外部管理（Kueue 等）の領分。必要になったら handler にではなくコントローラが焼く |
| podFailurePolicy | インフラ起因失敗の分類（§5）はコントローラの責務。使うならコントローラが焼く |
| podReplacementPolicy | **検討中（未対応）**。既定の TerminatingOrFailed は terminating 中に代替 Pod を作るため、同一 runID の Pod が並走しうる（KEP-3939 に明記。JobSet の KEP-467 は同じ理由で Failed を強制）。`restartPolicy: Never` を強制した理由と同型の侵害。コントローラが Failed を焼く案を、実クラスタでの並走再現待ちで保留。**並走しても結論は汚れない**ことは回収側で担保済み（§7「完了検知」— 全 Pod を走査し、答えが 2 つなら Escalated）。タイムアウト経路（deadline + 猶予で Escalated）も実装済みで、残る実測は stuck-terminating の実クラスタ再現のみ |

profile は **コントローラ組み込みの enum**（3 つ目の CRD にはしない）。profile が規定するのは
**どのフェーズの束縛が必須か**であって、フェーズの語彙そのものではない。それでも YAML で
勝手に増やせるようにはせず、profile を増やすのは「コード変更 + テスト」で
あるべき。

実際の一式は §16 を参照。

**人間レビュアーも同じ形の箱にする。** sandbox を持たず `completion: HumanPatch` なだけ。
コントローラから見れば AI も人間も GHA も「review フェーズを埋める何か」で区別がない。
差し替えは `bindings` の 1 行。

---

## 5. フェーズと遷移

**ステータス名は利用側のもの。** framework は語彙を持たない。下は 1 つの例にすぎない:

```
Pending → Triaging → Planning ⇄ PlanReview
                                    ↓
                            Implementing ⇄ Review
                                    ↓
                                Verifying → Done
                                    ↓
                            Escalated（人間へ）/ Failed
```

初版はこの語彙を framework が固定していた（理由は「CNP / Kyverno がこの名前に対して書かれるから」）。
**それは利用側のドメインを framework が決めることだった。** ポリシーを書くのは利用側で、
どんな名前に対して書くかも利用側が決めればよい。§2 の責務表に照らせば取り違えで、
`Pod の形` をコントローラに持たせようとしたのと同じ誤り。

framework が持つ名前は **2 つだけ**:

| 名前 | いつ | 束縛できるか | `next` の行き先にできるか |
|---|---|---|---|
| `Escalated` | 答えが 1 つに定まらなかった（0 個 / 2 個以上 / 時間切れ / 宣言に無いディレクトリ） | ✗ | **✓** |
| `Failed` | flow 自体が壊れている（束縛の無いフェーズ、同じディレクトリを指す 2 ステータス） | ✗ | ✗ |

**`Escalated` は行き先としてだけ宣言できる。**

```yaml
next: {報告: ok, Escalated: escalate}
```

予約の趣旨は「`Escalated` / `Failed` に **handler を束縛**させない」= 答え無しを成功経路に
流させないことなので、**行き先**にするのはその心配に当たらない。行き先として宣言すると
`escalate/` が敷かれ、エージェントは「判定できない」を**書いて**表明できる（回収規則は
そのまま。非空がちょうど 1 つ）。区別が付く:

| エージェントがしたこと | outcome | 何が残るか |
|---|---|---|
| 何も書かなかった / 途中で死んだ / 時間切れ | `NoAnswer` | 沈黙。理由はフレームワーク側の推測しかない |
| `escalate/` に書いた | `Declined` | 本人の理由とレポート |

これは後退の修正でもある。Step 0 の Workflow には `escalate/` があって理由付きで返っていたのに、
CRD 化で「書かない」しか無くなっていた。**実際に 2026-08-26 の run 1 は max-turns 切れだったが、
history からは意図的な escalate と区別できなかった。**

`Failed` は行き先にもできない。`Failed` は flow の破損であって handler の言い分ではなく、
**定義が自分について「壊れている」と結論する**筋合いが無い。`next` が `Failed` を名乗ったら
それ自体が破損なので、`Structural` で `Failed` に落とす（宣言された辺として通さない）。

**束縛の無いステータスが終端。** `Done` を特別扱いしない — 行き先を持たないなら、そこで止まる。

### 終端の意味は flow が宣言する（`spec.terminals`）

終端が**どこか**は束縛の有無で決まるが、そこに着いたのが**良い知らせかどうか**は framework には分からない。
`失敗` も `おわり` も利用側が選んだただの名前で、framework の語彙には無い。だから flow が言う:

```yaml
terminals:
  完了: Success
  失敗: Failure
```

- キーは**束縛の無いフェーズ**でなければならない（束縛があるのに終端と言うのは矛盾。CEL で create 時に拒否）
- `Escalated` / `Failed` は書けない（framework 自身の終端で、意味は flow のものではない。同じく CEL）
- **省略可。省略は `Success` ではない。** 宣言の無い flow は terminals が存在しなかった頃と同じ沈黙のままで、
  終端は `Undeclared` として報告される（P8 — 黙って成功に倒さない）。1 つ宣言しても残りを宣言する義務は無い

終端の種類に応じて framework が声を出す。4 つの出力は同じ条件で出るわけではない:

| 出るもの | いつ | 詳細 |
|---|---|---|
| Kubernetes Event | `Failure` のときだけ | `Warning` / reason `HandlerFailed` / メッセージに handler の理由 |
| 条件 | `Escalated` / `Failed` / `Failure`（人間が見に来る必要がある終端） | `Ready=False`。reason は `Failure` のとき `HandlerFailed`、他の 2 つは `res.Outcome`（`Failure` の outcome は **`Declared` のまま** — 宣言された辺を通っただけで、機構は何も壊れていない。人間が最初に見るべきは「結論が悪い」の方） |
| TTL | 同じ 3 値で `ttl.failed`、残り（`Success` / `Undeclared`）は `ttl.succeeded`（§10） | |
| metric | **`EndingRunning`（終端でない）以外は常に** — `Undeclared` も含む | `taskflow_task_outcomes_total{flow, phase, severity}` |

**終端の「意味」は 5 値**（`transition.Ending`）: `Success` / `Failure`（flow の宣言）、
`Escalated` / `Failed`（framework 自身の 2 つ）、`Undeclared`（宣言が無い）。
metric の `severity` は 5 値すべてを常に出すので、**「まだ宣言していない flow」がダッシュボードで見つかる**。
`flow` ラベルは実在する TaskFlow の名前か、参照先が存在しなかったことを表す固定値 `<unresolved>` の
どちらかを取る。

これが無いと、エージェントが「この対象は壊れている」と**明示**しても framework は素通りする。
Step 0 の cnp-check は通知サイドカーが自前で判定していたが、それは利用側が毎回書き直す羽目になる。

**循環するのが本質。** Argo Workflow の DAG は非巡回なので差し戻しが表現できない。
これがコントローラを書く最大の理由。

### 遷移関数

遷移表はコントローラが持たない。**`bindings[phase].next` が辺を宣言し、それが必須。**

```
next(bindings, phase, directory, visited, budget) → phase
```

コントローラ組み込みなのは**失敗系だけ**：

```
非空ディレクトリがちょうど 1 つでない  → Escalated   （0 個も 2 個以上も時間切れも同じ）
宣言に無いディレクトリ                → Escalated
束縛の無いフェーズ                    → Failed
同じディレクトリを指す 2 ステータス    → Failed（行き先が決まらない）
next が Failed を名乗る               → Failed（予約語。宣言された辺として通さない）
```

`next` が宣言した `Escalated` への辺だけは組み込みではなく **flow の表に従う**（上記）。
訪問済み判定も予算も見ない — `Escalated` は終端で、その後に走る run が無いため。

**失敗系は宣言で上書きできない。** ここを可変にすると P6（判定不能を pass に倒さない）が
YAML 一行で破れる。

### なぜ辺を外に出すのか

グローバルな遷移表だと `(Review, pass)` の行き先が profile によって変わり、遷移関数に profile 引数が
必要になる（初版はここを取り違えてバグっていた）。**辺を束縛側に置くとその分岐が存在しなくなる。**

固定されているのはフェーズ**名**の語彙ではなく、**遷移の枠組みと失敗系**（`Escalated` / `Failed`）
だけ。CNP / Kyverno は「利用側が自分で決めた名前」に対して書ける — 辺を束縛側に置いたことで
自由になったのは辺そのものであって、フェーズの名前を含めた束縛の集合全体が利用側のものになる。

### 予算消費は宣言させず、実行時に判定する

`next` で自由に辺を張れると、減少する量のない循環が書けてしまう。
`consumesBudget: true` のような注釈にすると付け忘れる。

> **遷移先が「このタスクで既に訪問済みのフェーズ」なら、自動的に rework 辺とみなして
> `reworkBudget` を 1 減らす。0 なら遷移せず `Escalated`。**

注釈不要・迂回不可能。停止性が宣言の正しさに依存しなくなる。
`reworkBudget: 0` のタスクは「一度も後戻りしない」という意味になり、
**前進する辺しか通らない限り影響を受けない**（初版の無条件ルールはここがバグだった）。

### profile は「必須フェーズ」を強制する

状態機械ではなく検証スキーマになるが、単に「どのフェーズが存在するか」ではなく
**どのフェーズが必須か**を規定する。プロンプトでは絶対に保証できないものが admission で保証できる。

- `implement` profile は **Review の束縛を必須**にする。Implementing → Done を直結させた
  タスク定義は作れない
- クラスタ状態を変える profile は **Verifying を必須**にする（`/dev-watch` の自動化）

**「Review と Implementing に同じ handler を指定できない」は入れない。** 一度は入れかけたが、
`serviceAccountName` は handler の `jobTemplate` に書かれ、それを書くのも束縛するのも
git のレビューを通る。**レビューを通ったうえでの選択**にコントローラが異議を唱える筋合いがない。
`lint` を Checks と Verifying の両方に置くような正当なケースも塞いでしまう。
P8 の「矛盾したら拒否」は構造的矛盾に対するものであって、判断の是非には及ばない。

タイムアウトも特別扱いせず `NoAnswer` の一種として同じ Escalated 経路に流す。経路は 1 本だけ。

### 厳格検証（admission / CEL）— 矛盾したら作らせない

デフォルト値も暗黙のマージも持たない（P8）。以下はすべて **create 時に拒否**する。

| 条件 | 拒否理由 |
|---|---|
| `next` が無い binding がある | 必須。デフォルトの行き先を推測しない |
| profile に含まれるフェーズに binding が無い | 実行中に「行き先はあるが担い手が無い」で止まる |
| profile に無いフェーズの binding がある | 意図の取り違え。黙って無視しない |
| `next` の行き先が未束縛でも終端でもない | 実行時の停止を作成時のエラーに変える |
| `next` のキーに `Failed` が現れる | 予約語。flow の破損は handler の言い分ではない（`Escalated` は行き先としてなら可） |
| `terminals` のキーが束縛のあるフェーズ | 終端ではないものを終端と言っている（**CEL で実装済み**） |
| `terminals` のキーが `Escalated` / `Failed` | 予約語。framework 自身の終端の意味は flow のものではない（**CEL で実装済み**） |
| 同じ binding 内で 2 ステータスが同じディレクトリを指す | 実行時に行き先が決まらない |
| ディレクトリ名がパス要素として不正（`/` や `..` を含む） | 作れない |
| ディレクトリ名が `.prepared-by` | 予約語。prepare が Pod マークを置く場所（[ADR-0004](adr/0004-run-id-counts-runs-not-attempts.md)） |
| handler の `spec.phase` と binding のキーが不一致 | 取り違え |
| 開始フェーズ（`spec.start`）から到達できないフェーズがある | 孤島。書き間違い以外にありえない |
| 束縛の無いステータス（＝終端）に到達する経路が 1 本も無い（`Escalated` / `Failed` 自体は除く） | 成功しえないタスク |
| jobTemplate の予約フィールド（§4 の表） | 不変条件を壊す |

**`Task.spec` は作成後 immutable**（CEL の `oldSelf` で拒否）。
実行中にグラフが書き換わる race を丸ごと消す。変更したければ作り直す。

### 実行時の矛盾は修復せず `Failed`

「エージェントが下手を打った」と「仕様が壊れている」を区別する。

| 種類 | 例 | 結果 |
|---|---|---|
| エージェント起因 | verdict が無い / ディレクトリが 2 つ / 未知のトークン | 直接 **Escalated**（人間が見る） |
| 構造起因 | handler が実行中に編集・削除された / 束縛の無いフェーズを指した | **Failed**（修復を試みない） |

handler の変更検知は `status.currentRun.handlerHash` に解決済み spec のハッシュを置く。
不一致なら即 `Failed`。実行中のタスクの足元で定義が変わったなら、それはもう別のタスク。

### 循環に必要なもの

- **減少する量**：`reworkBudget` は減るだけ、絶対に増やさない。循環が複数あるなら辺ごとに別予算
- **runID**：フェーズ名だけでは実行を識別できなくなるため。子リソース名を
  `<task>-<runID>-<フェーズ名のハッシュ>` と決定論的にする（`generateName` は使わない。
  形式と所有権検証は §4）。
  遅れて到着した古い run の verdict は runID 不一致で黙って捨てる

### カウンタは 2 本

| カウンタ | 増える条件 | 用途 |
|---|---|---|
| `runID` | **決着した run の後だけ**（rework も前進も。インフラリトライでは動かない） | パス・子リソース名の識別子 |
| `reworkBudget` | rework の時だけ減る | ループ終了保証 |

`runID` が数えるのは**タスクが決着させた run** であって、その run を起動するのに何回かかったかでは
ない（ADR-0004）。何も決まっていない試行に番号を払うと `results/` の棚に穴が空き、穴はそこに穴が
あると既に知っている者にしか読めない。同じ run の 2 回目を区別するのは Job 名の attempt。

インフラ起因の失敗（OOMKilled / evicted / ImagePull / ノード落ち）は **rework 予算を消費しない**。
区別できない場合は安全側（Escalated）。

---

## 6. verdict の取得

### ディレクトリで表す

パーサが存在しない = パースエラーが存在しない。`ls` が判定になる。

**ディレクトリの集合は `next` の宣言そのものから来る。** framework は宣言されたものだけを作る。

```yaml
next: {報告: ok, 調査: more}
```

```
/workspace/            ← handler のマウント直下 = この run のディレクトリ。語彙はここに並ぶ
  ok/                  ← 空
  more/
    report.md          必須（自由記述）
    findings.json      任意。あれば使う、壊れていても判定は覆らない
    _done              最後に書く。これが無いディレクトリは無視
```

マウントと語彙の間に階層は置かない（[ADR-0005](adr/0005-vocabulary-at-the-mount-root.md)。
初版は `out/` を挟んでいて、棚を読む側がそれを落として Escalated を踏んだ）。

- **宣言が 3 つの役目を同時に果たす** — ①辺 ②作るディレクトリ ③エージェントの語彙
- **非空がちょうど 1 つ**でなければ直接 Escalated。0 個も 2 個以上も同じ扱い
- `_done` マーカーを最後に書くことで、書き込み途中を読む競合を防ぐ
  （**v1（Job runner）では sidecar が同一 Pod 内で読むため実質不要**。長命 runner を足すときに効く）
- 詳細（findings.json）は任意。**steer するビットと、間違いうるビットを分離する**

### パーミッションで語彙を強制する

```
/workspace/        prepare の uid  0555   ← 書けない
/workspace/ok/     prepare の uid  0777   ← next の宣言から生成
/workspace/more/   prepare の uid  0777
```

`mkdir /workspace/NG` が EACCES で失敗する。**バリデーションがカーネルに降りる。**

そして語彙外のトークンは**そもそも表現できない**。宣言に無いディレクトリは存在しないので、
「未知のトークンを出す」という事象が起こらない。初版は `pass` / `rework` / `escalate` を
framework が固定し、パーミッションで 4 つ目を防いでいたが、**宣言から生成すれば
固定する必要そのものが消える。**

### 「わからない」の逃げ道を必ず用意する

2 値しか許さないとモデルは迷ったとき必ずどちらかを捏造する。不確実性を合法的に表現する
出口がないため。`escalate` を常に用意し、プロンプトでも「迷ったら escalate」と明示する。

**その出口は flow が `next: {..., Escalated: escalate}` と宣言して作る**（§5）。
framework が全フェーズに勝手に生やしはしない — ディレクトリは宣言から生成される、が
唯一の規則だから。宣言しなければ「何も書かない」だけが逃げ道になり、それは沈黙と
区別が付かない（`NoAnswer`）。**逃げ道を用意するかどうかは flow の選択で、
用意した場合にだけ理由が残る**（`Declined`）。

### verdict の伝達は 2 ホップ

pod の中身が全部ユーザー定義になるため、**コントローラが store を読む設計は採らない**
（コントローラが S3 認証情報を持つことになり、P7 が壊れる）。

| 区間 | 手段 | その区間で守りたい性質 |
|---|---|---|
| エージェント → サイドカー | **ディレクトリ**（`next` の宣言から生成、例: `/workspace/{ok,more}/`） | LLM の散文・フォーマット崩れに耐える。パーミッションで語彙を強制 |
| サイドカー → コントローラ | **termination message**（`/dev/termination-log`） | K8s ネイティブ。認証情報も store 依存も不要。両端が決定論的プログラム |

サイドカーは書かれたディレクトリを検証して、その名前を `/dev/termination-log` に書く。
コントローラは `state.terminated.message` を読むだけ。

**どのコンテナを読むかは決め打ちしない。** Pod には init・エージェント・サイドカーが並び、
どれがサイドカーかを framework が知る手段は無い（handler の作者が書くので）。
代わりに**判定ディレクトリと同じ規則**を使う:

> **全コンテナ（init 含む）を走査し、1 行目が宣言されたディレクトリ名と完全一致する
> ものがちょうど 1 つ。それ以外は答えが無かった扱い。**

- **パーサを持たない**ので、パースエラーも存在しない（§6 の判定ディレクトリと同じ理由）
- 2 行目以降は人間が読む理由。**遷移には使わない**
- サイドカーが失敗を報告したいときは、宣言に無い文字列を書けばよい
  （`publish failed: ...`）。それは自動的に「答えが無い」= `Escalated` になるので、
  **失敗の伝え方を別に設計しなくてよい**
- 宣言されたディレクトリ名は flow の `next` から来るので、コントローラは
  照合表を自分で持たない

- 上限 4KB。verdict + 一行の理由には十分。**レポート本体はここに載せない**（store か Loki）
- termination message が無い / 読めない / 語彙外 → 直接 Escalated
- store への publish はユーザーのサイドカーの仕事であり、コントローラの関心事ではない

これで**コントローラの外部依存は `batch/v1` と core/v1 だけ**になる。

**この判断は実物で裏を取っている（2026-08-22）。** 既存の `adjudicator` WorkflowTemplate は
workflow レベルの `emptyDir` で判定を渡そうとしていたが、emptyDir は Pod スコープなので
**別ステップの Pod には一度も届いていなかった**（[#568](https://github.com/Tsuguya-HC/home-cluster/pull/568) で修正）。
「共有ストレージで判定を渡す」は素朴に見えて壊れやすい。オーケストレータ自身のメタデータ
チャネル（Argo なら output parameter、Job なら termination message）に載せる方が確実。

### 2 行目が出る先

termination message の 2 行目は 3 箇所に出る: `status.history[].reason`、`Ready` 条件の
message、そして `Failure` 終端では Warning Event の note（§5）。Event は Task とは別のオブジェクト
なので、Task を読む権限とは別の権限で読まれる。

`internal/collect/sanitize.go` の制御文字除去と 1024 rune の切り詰めは、端末インジェクションと
長さ爆弾を防ぐためのもので、中身が何かは見ていない。

### なぜ最終メッセージをパースしないか

- 長い agentic run では出力上限で切れることがある
- 安全分類器の拒否は HTTP 200 のまま `stop_reason: "refusal"` で返り中身が空になる
- 旧来のプリフィル強制（assistant turn を `{` で始める）は現行モデルでは 400

スキーマ制約（Claude Code の `--json-schema` 等）は形を保証するが**中身の正しさは保証しない**。
判定の頑健さはディレクトリで、詳細度は任意の JSON で、という二層にする。

---

## 7. 実行モデル

### Pod 構成

```
initContainer:  過去の run を /results/ に用意し、next の宣言から書き込み先を作る
main:           エージェント（ローカルのボリュームに書くだけ。S3 認証情報も egress も持たない）
sidecar:        SIGTERM を trap → 書かれたディレクトリを検証し、termination message に名前を返す
                中身は /results/<runID>/ へ封じる
```

### ワークスペースのレイアウト

**framework が持つのはレイアウトであって場所ではない。** 基点は handler が決める。

handler が書くのは `spec.workspace: {volume, mountPath}` — 判定の回収に使うボリュームの名前と、
それをどこに見せるか。**prepare / publish のコンテナはコントローラが Job 生成時に注入する**
（2026-08-30、#73）。利用側の pod spec には現れず、handler の作者はサイドカーの存在を知らなくてよい:

- **コントローラが渡すのは語彙**: `next` の宣言から取ったディレクトリ名を `FLOW_DIRECTORIES`
  （JSON 配列、ソート済み）として全コンテナに注入する。`FLOW_PHASE` と同じ扱いで、run 番号と違って
  エージェントが見てよい情報（どのみちファイルシステムに見えている）
- **コントローラが足すのは両端**: `flow-prepare`（init、run ディレクトリ `work/<runID>/` を作って
  宣言ディレクトリを敷き、閉じる）と `flow-publish`（ネイティブサイドカー、SIGTERM で封じて
  termination log に書く）を
  initContainers の先頭に置く。イメージは controller のフラグ `--sidecar-image`。template が同名の
  コンテナを持っていたら framework ラベルと同じく**上書きせず拒否**する。この 2 本だけが
  `FLOW_POD_UID`（downward API `metadata.uid`）を持つ — prepare が run に刻み publish が照合する
  同一性で、handler のコンテナには渡さない（ADR-0004 決定5）

2026-08-25 の時点では `spec.workspace.path` を「コントローラにそのフィールドの消費者が無い」として
却下し、片端の配線だけ利用側の pod spec に書かせていた。これは取り違えだった: §2 の責務表で
**判定の回収はコントローラの持ち物**であり、P7 が排除するのはプロンプト・モデル・API キー・store。
サイドカーは LLM を知らない固定の小さいプログラムで、P7 の対象ではない。両端とも taskflow の
コード（`internal/runner` と `cmd/sidecar`、`internal/contract` を共有）なのに片端だけ利用側に
30 行複製させるのは、handler を増やすたびに同じものを書かせることになる。今はコントローラが
消費者なので、フィールドを持つ理由が立つ。線引きは「**タスクに関することか**」— 何を実行するか・
どの権限で・どのプロンプトで、はタスクの話で利用者が決める。判定をどう回収するかはタスクの中身では
なく framework の配管で、利用者に見せる理由が無い。

**注入コンテナへ渡す動線。** SA は Pod 単位なので注入コンテナにも自動で届く。store は S3 ではなく
PVC になった（[ADR-0002](adr/0002-per-task-workspace-pvc.md)）ので、認証情報の口（`envFrom`）は
一度も要らないまま終わった — 消費者の無いフィールドを先に置かない、という 08-25 の判断そのものは
正しかった。

実装は `internal/sidecar`（純粋関数 `Prepare` / `Seal`）と `cmd/sidecar`（`prepare` / `publish` の
2 サブコマンド、同一イメージ `Dockerfile.sidecar`）、注入は `internal/runner`。

**フェーズ間の引き渡しは Task ごとの PVC**（[ADR-0002](adr/0002-per-task-workspace-pvc.md)）。
flow が `spec.workspace.volumeClaimTemplate`（corev1 の **spec のみ**。省略時は CRD 既定
`{RWX, 1Gi}` が admission で実体化、storageClassName 省略はクラスタ default SC）を書くと、
コントローラが Task ごとに claim を 1 つ作る — 名前は Task UID 由来の決定論（同名 Task の
作り直しと再バインドしない）、ownerRef = Task で TTL 削除に同乗。handler は予約 volume 名
**`flow-workspace`** を `spec.workspace.volume` に書いて乗るだけでよい。**subPath を書かない
マウントは readOnly でも「この run のビュー」**として、生成時に literal `subPath: work/<runID>` が
焼かれる（[ADR-0003](adr/0003-run-view-and-sweep.md)。自分の run を読むだけの notify のような
サイドカーも runID を知らずに済む）。書き込み可マウントに handler が自分で SubPath / SubPathExpr を
書くのは拒否。走行中の書き込み先は `work/<runID>/<宣言ディレクトリ>/`（handler からは
`/workspace/ok/` 等、マウント直下）。publish が seal で
verdict を確定させた後、同一ボリューム内の rename で `work/<runID>` を `results/<runID>` に移す —
別ボリュームを跨がないので原子的。**`results/` には封印済みの run だけが並ぶ**、というのが読み側の
意味論。publish は flow-workspace モードでは唯一 volume の root を書き込み可でマウントする（rename
が work/ と results/ の二つの棚を跨ぐため）— agent の書き込みは work/<runID> の subPath に閉じた
まま。rename が失敗したら termination message を書かずに非 0 で exit し、既存の infra retry 経路に
落ちる（移動できていないのに verdict だけ通ると下流が読めないディレクトリを指すことになるため）。
再試行は同じ runID に戻ってくるので（[ADR-0004](adr/0004-run-id-counts-runs-not-attempts.md)）、
Pod オブジェクトが消えたあとも生きているゾンビ publish が、次の attempt の書きかけを同じパスから
棚へ運びうる。prepare が run 直下の `.prepared-by` に自分の Pod UID を刻み（downward API、注入 sidecar
だけに渡る）、publish は SIGTERM 後・封印前に照合して、他 Pod の run なら封印も rename もせず
失敗を報告する。
孤児の窓（rename 成功後・message 送信前のクラッシュで History に無い完了 run が results/ に残る）は
実害が薄いので許容する。env / subPathExpr は使わない、run 番号はエージェントの env には出ない
（annotation `flow.tgy.io/run-id` は配管の記録のまま）。過去を読むフェーズは同じ volume を readOnly +
**`subPath: results`（明示）**でマウントする — 無指定は「この run」に焼かれるので、棚が欲しい
マウントだけが棚と言う（[ADR-0003](adr/0003-run-view-and-sweep.md)）。マウント直下の `<runID>/` を
開けば封印済みの run だけが並び、今走っている run は work/ にいるので見えない。readOnly の明示
subPath は checkWorkspace の拒否対象ではない（拒否は書き込み可マウントの SubPath / SubPathExpr だけ）。

**残骸は次の run の prepare が掃除する**（同 ADR-0003）。prepare は work/ を 1 階層上でマウントし、
自分の run ディレクトリを作ってから、コントローラが計算した sweep リスト（何が生きているかを知る
のはコントローラだけ。直列の今は自 runID 未満の全部で、並列化の日はリスト計算だけが変わる）にある
`work/<id>` を消す。封印済みは rename で work/ に居ないので、消えるのは封印できず死んだ run の
残骸だけ。消せなければ（NFS の sillyrename ゴースト等）エラー = その run は始まらず infra retry へ
— 係争中のボリュームで新しい仕事を始めない。Escalated で止まった Task には次の run が来ないので、
その残骸は検死素材として TTL まで残る。vct を書かない
flow は従来どおり template の volume で完結する（opt-in）。

**Pod の中で走るバイナリと controller が共有する名前（env / annotation / label）は依存ゼロの
`internal/contract` にだけ置く。** `cmd/sidecar` は k8s.io 系を一切 import しない
（`go list -deps ./cmd/sidecar | grep k8s.io` が空）。Pod の中で走るバイナリの依存グラフは
そのまま攻撃面になるので最小に保つのが理由で、副産物として controller 側の Job 構築の都合で
sidecar イメージを再ビルドしなくて済む。

**エージェントは自分が何周目かを知らない。**

| パス | 権限 | 中身 |
|---|---|---|
| `<path>/<宣言されたディレクトリ>` | エージェントが書ける | この run の結果（`/work/ok`, `/work/more` …） |
| `<path>/results/<runID>/<name>` | 読み取り専用 | 過去の全 run |

**基点を `TaskFlow` ではなく `TaskHandler` に置く。** マウントパスは Pod の形であり
（§2 の責務表では handler の作者のもの）、handler は複数の flow で使い回せる。
`/work` を前提に書かれた handler が、flow 側の指定で壊れるのはおかしい。
同じボリュームを別のパスにマウントするだけなので**フェーズごとに違っても成立する**
（1 周目が `/work` で書き、2 周目が `/data` でマウントしても中身は同じ）。

書き込み側は平らで runID を含まない。履歴は `<path>/results/` の下に run ごとに積まれる。
3 周目のエージェントが 1 周目の報告を読むのは `<path>/results/1/ok/report.md` を開くだけで、
**framework が過去の場所を教える必要も、エージェントが番号を数える必要も無い。**

> **宣言が見えるのはファイルシステムそのもの。** `ok/` と `more/` が存在して、それ以外は無い。
> 語彙外のトークンを出せないのは、禁じているからではなく**存在しないから**。

`prepare` が敷くもの（Step 0 の `cnp-check-cwf.yaml` で実証した形を Go に移し、ADR-0005 で
`out/` の階層を外した）。run ディレクトリ `work/<runID>/` を prepare 自身が作り、そこが handler の
マウント直下になる — template 持ち込みの emptyDir でも flow の PVC でも同じ:

```
<run>/           prepare の uid   0555   ← 閉じる。宣言に無い mkdir は EACCES
<run>/ok/        prepare の uid   0777   ← 誰でも書ける。「誰でも」= 同じ Pod のコンテナ
<run>/more/      prepare の uid   0777
<run>/.prepared-by                       ← Pod UID（ADR-0004）。閉じた中に一緒に封じる
```

prepare が volume を `subPath: work` でマウントして `<runID>/` を自分で作るのは、kubelet が作った
subPath ディレクトリは root 所有で chmod が EPERM になるため（2026-08-30 実測）。自分で作れば
閉じられる。handler のマウントは `subPath: work/<runID>` に焼かれ、init が main より先に走るので
kubelet はその時点で存在するディレクトリを作り直さない（2026-09-05 実測、runc / kata とも
handler から見た root は `dr-xr-xr-x prepare-uid`）。閉じた run を publish が棚へ rename するときは
ディレクトリ自身への書き込み権限が要る（`..` の書き換え）ので、publish は同 uid で 0755 に開けて
rename し、棚の上で 0555 に閉じ直す（owner のみなので agent には開かない）。

閉じるのは uid で、**prepare の uid はコントローラが選ぶ**。同じ uid なら chmod で開け直せるので
閉じたことにならない（2026-08-30 実測: agent を prepare と同じ 65532 で走らせると閉じた
ディレクトリの `chmod 0775` も `mkdir evil` も通り、65533 なら EPERM / EACCES）。だから runner は template が名乗る uid
（Pod レベルと各コンテナ）を全部避けて 65532 から下へ探し、注入コンテナの `runAsUser` に書く。
handler の作者に「65532 を使うな」と覚えさせない。宣言ディレクトリを group 書き込みではなく
0777 にしたのも同じ理由 — fsGroup を prepare の group に合わせる規則を消すため。emptyDir は Pod で
閉じているので、「誰でも」が指す範囲は group 方式と変わらない。

**handler に残る条件は 2 つ**、どちらも欠けると runner は BuildJob を拒否する（P8 に従い推測せず拒否
する — Vault Agent Injector が container レベルだけ見て拒否し #261 で不評を買ったのと違い、Pod
レベルの宣言で足りる）:

1. **Pod の `securityContext.runAsUser` を書くこと。** 未設定だと uid は各イメージの USER で決まり、
   コントローラからは見えない。distroless nonroot のような普通の base が sidecar と同じ 65532 で
   走るので、「たまたま同じ」が handler のどこにも現れずに起きる。
2. **handler のコンテナのうち少なくとも 1 つが workspace の volume を書き込み可でマウントしていること。**
   （どのパスにマウントするかは handler の自由 — 同じ volume なら別パスでも成立する。）マウントが
   無ければ宣言ディレクトリを読むことも答えを書くこともできず、run は最初から沈黙が確定している。
   それが分かるのが Job が終わって `publish` が「答え無し」を報告したときでは遅すぎるので、
   BuildJob の時点で拒否する。

root で走る handler を runner が拒否することはない — それは Pod のセキュリティポリシー（PSA /
Kyverno）の層の仕事で、コントローラはポリシーを持たない（§2 P2）。uid 分離が守っているのは
エージェント（操られうる側）が prepare の閉じた run ディレクトリを開け直せないことであって、handler の
作者が root を選ばないことではない。

`prepare` は繰り返し実行しても既存の中身を消さない。

**`<run>/<name>` が symlink に差し替えられていたら `prepare` は拒否し、`publish` は答え無しにする**
（両方とも `os.Lstat` で判定し、`os.Chmod` や `os.ReadDir` に symlink を追わせない）。同一 Pod の
emptyDir ではエージェントは run 直下に書けないので踏まないが、§9 の PVC backend（rework で run を
跨いで生き残る）では先行 attempt が仕込んだ symlink を後続の `prepare` が chmod で開ける、あるいは
`publish` がその先を読むことで「非空の宣言ディレクトリがちょうど 1 つ」を偽装できてしまう。
PVC を実装する前に閉じておく。

`publish` は SIGTERM を受けてから `<out>` を見る（ネイティブサイドカーは main 終了後に SIGTERM で
刈られる。それが「run が終わった」の唯一の合図）。規則はコントローラ側と同じ 1 本 — **非空の宣言
ディレクトリがちょうど 1 つ**、それ以外は答え無し。中身は読まない（エントリの存在だけが信号）。
答えは termination log に「1 行目 = ディレクトリ名、2 行目 = 何を書いたか」で書き、答えが無いときは
理由をそのまま 1 行目に書く。理由は宣言名と一致しないので、コントローラ側では自動的に「答え無し」
になる — 失敗を伝える第 2 のプロトコルは要らない（§6）。`prepare` が失敗したときも同じチャネルに
`prepare failed: ...` と書くので、status には「走らなかった」ではなく「準備で落ちた」と残る。

backend の違い（S3 prefix / PVC）は、この 2 つのパスをどう用意するかに閉じる
（PVC なら subPath マウント、S3 なら init が `/results` へ引き落として sidecar が上げる）。
**framework が知るのはパスの形だけで、中身にも backend にも触らない。**

**権限分離が本命の理由。** エージェントは書ける場所がローカルの 3 ディレクトリだけ。
サイドカーは store の認証情報を持つが LLM を持たない、固定の小さいプログラム。

> エージェントが判定を**書く**、サイドカーが判定を**封する**。

**ただし境界は資格情報 1 枚しかない。** Cilium の identity は **Pod 単位**なので、サイドカーが
store へ到達するために開けた egress は agent コンテナからも到達できる。コンテナ単位の
ネットワーク分離は CNP では表現できない。止めているのは認証情報だけ、と理解しておく。

同様に、agent コンテナは LLM API への egress と API キーを必ず持つ。**系で最も価値の高い
資格情報が最も信用していないコンテナにある**という構造は避けられない。緩和するならキーを
持つプロキシを挟んで agent には見せない形にするが、それも**ユーザーの pod spec の話**
（P7）であってコントローラの設計事項ではない。

- ネイティブサイドカー（`initContainers` + `restartPolicy: Always`）は main 終了後に SIGTERM で刈られる。
  「後に走る」のではなく「trap して flush する」形になる
- **ペイロードは小さく保つ**（grace period 内に上げ切る）。ログ本体は publish しない（Loki にある）
- ノード死亡・OOM なら termination message が書かれない → 直接 Escalated。
  **fail-closed が実装努力ゼロで成立する**
- **ネイティブサイドカーの終了コードは Job の完了判定に影響しない。** publish が失敗しても
  Job は Complete になる。結果は Escalated なので安全側だが、
  「エージェントが verdict を出さなかった」と「publish が失敗した」が区別できない。
  サイドカーは失敗時も termination message に理由を書いて区別できるようにする
- サイドカーは判定を運ぶだけで遷移は決めない（P1）。**どのディレクトリに書かれたかは
  termination message で返し**、中身（レポート・作業ツリー）はワークスペースへ封じる。
  コントローラは Job の完了を watch して termination message を読むだけで、
  **ワークスペースの中身にも backend にも触らない**（§11 で「コントローラが store を list
  する」案を却下している。触ると S3 認証情報と store の種類を知ることになり P7 に反する）

**Job 固有の注意:** `backoffLimit` / `ttlSecondsAfterFinished` / `activeDeadlineSeconds` /
`completions` / `parallelism` / `restartPolicy` は予約フィールドとして admission で拒否する（§4 の表）。
インフラ起因の再実行はコントローラが**同じ runID・次の attempt の名前で Job を作り直す**
（[ADR-0004](adr/0004-run-id-counts-runs-not-attempts.md)。何も決まっていない試行に番号は払わない。
前の attempt の残骸は prepare の `MakeRun` が消し、ゾンビ publish が次の attempt の run を棚へ
運ばないよう prepare が run 直下の `.prepared-by` に Pod UID を刻み、publish は封印前に照合する）。

### 終了必須（長命 Sandbox は v1 では採らない）

理由：

1. **長命は per-phase 権限モデルと両立しない。** 走行中の Pod の SA は変えられず、
   Cilium identity もラベル書き換えで綺麗に切り替わらない。フェーズを跨ぐと権限の和集合を持つことになる
2. **価値のピークとコストのピークが同じ場所にある。** 状態を保ちたいのは人間レビュー待ちの数日、
   だがそこは Kata VM を数日空回しすることになる。払えるのはフェーズ内の数分だが、そこは 1 回で終わる
3. 長命で欲しいものの大半は volume に落ちる（作業ツリー → PVC、ビルドキャッシュ → キャッシュ PVC、
   前の推論 → 引き継ぎファイル）。本当に残る差分は「走っているエージェントへのリアルタイム割り込み」だけ
4. v1 から agent-sandbox（v1alpha1）依存を落とせる。Kata の RuntimeClass 付き Job で足りる

**後から足すための不変条件（1 つだけ）:**

> **Sandbox の寿命は runID の寿命を超えない。**
> フェーズ内で生存するのは純粋な追加。フェーズを跨いだ瞬間に権限モデルごと再設計になる。

### 完了検知

```
run 終了を観測（Job の watch / _done / deadline）
  → verdict を list で回収
  → 遷移表に食わせる
  → 次の runID を採番して dispatch（決定論的な名前 + 所有権検証で冪等）
```

**終了コードは verdict ではない。** exit 0 でも verdict ディレクトリが空なら判定不能。
ルールは 1 本：run が終了していて、かつ非空ディレクトリがちょうど 1 つ。それ以外は全部直接 Escalated。

タイムアウトは Workflow の `activeDeadlineSeconds` に頼らず、
コントローラ側の `status.currentRun.deadline` + requeue-after で一元管理する
（handler の種類によらず経路を 1 本にするため）。

**実装（`internal/controller`、2026-08-25）。** Job の終了は `Complete` / `Failed` 条件で見る。
Pod は `batch.kubernetes.io/controller-uid` ラベルで引く（Job controller が付ける。
`spec.selector` を読まないのは、手動 selector を許していないので同じものだから）。
終了した Job の扱いは 3 分岐で、上から順に決まる:

| Job の状態 | 扱い | 理由 |
|---|---|---|
| `Failed` / `DeadlineExceeded` | **読まずに** Escalated（`NoAnswer`） | 途中で切られた run がディレクトリを書いていても、それは結論ではない |
| `Failed` かつ**どのコンテナも Terminated に達していない** | インフラ起因。`maxInfraRetries` まで**同じ runID・次の attempt の名前で**作り直し（[ADR-0004](adr/0004-run-id-counts-runs-not-attempts.md)）、尽きたら Escalated | pull 失敗・未スケジュール等、handler が何もしていない失敗だけがコントローラの再試行対象。**走ってから黙った run は再試行しない**（それは handler の沈黙であり人間の仕事） |
| それ以外（`Complete`、または走ってから `Failed`） | termination message を回収して遷移表へ | 終了コードは verdict ではない。agent が exit 1 でも sidecar が `ok` を封していれば `ok` |

Terminated の判定は init コンテナも含む。init が exit 1 で落ちたなら handler 自身のコードが
走っているので、インフラ起因ではない。

Pod が 2 つある場合（KEP-3939、terminating 中の代替）は全 Pod の全コンテナを平らに走査し、
規則は変えない — ちょうど 1 つだけが宣言ディレクトリを名指ししていること。両方が答えたら
それは「答えが 2 つ」で Escalated になる。**同一 runID の Pod 並走を防いでいるわけではなく、
並走しても結論が汚れないようにしている**（§4 の `podReplacementPolicy` の行を参照）。

deadline は Job の `creationTimestamp + activeDeadlineSeconds` から読む（時計から計算しない。
コントローラが再起動しても同じ瞬間に着地する）。Job 自身の deadline と同じ値なので、通常は
Job controller が先に `DeadlineExceeded` を報告する。コントローラ側の経路は **その報告が来なかった
とき**のためにあり、deadline + 猶予 1 分（`deadlineGrace`）で requeue して、まだ終わっていなければ
読まずに Escalated にする。Pod が terminating で固まって Job の `Failed` 条件が付かない
（1.31 以降、`Failed` は全 Pod 終了後にしか付かない）ケースがこの経路の対象。
**Job は消さない**（RBAC に delete は無い）。Escalated 後も Job は Task の TTL で一緒に消えるまで残り、
人間が中を見られる。

同時に分かったこと: apiserver（1.36）は Job の status 遷移を検証する。`Complete=True` には
`SuccessCriteriaMet=True` と `startTime` / `completionTime` が、`Failed=True` には
`FailureTarget=True` が先に要る。envtest で Job controller の代わりに status を書くときは
この順序ごと書かないと 422 になる（`internal/controller/task_finish_test.go` の `finish`）。

termination message の 2 行目以降（`internal/collect` が読む free text）は agent 由来の
未信頼テキストとして扱う。status に書く前に、制御文字（`\n` `\t` 以外の非印字。Unicode の
空白分離文字 Zs — 全角スペース等 — は非印字扱いされるが除去せず残す）を取り除き、1024 rune
に切り詰める（`internal/collect/sanitize.go`）。termination-log がそのまま `kubectl describe`
に出る経路は CVE-2021-25743 と同じで、agent が制御シーケンスを書けば運用者の端末まで届く。
`HistoryEntry.Reason` の CRD `maxLength=2048` は rune 単位（apiserver は `utf8.RuneCount` で
判定する）で、コード側の 1024 rune という上限より大きく取ってあり、`transition.Detail` を
前置しても検証に落ちない余裕を残す。1 行目（ディレクトリ名）の完全一致判定には一切触れないので、
§6 の「パーサを持たない」原則とは無関係。

---

## 8. 権限・ポリシーの外出し

コントローラが書き込むラベルは**1 種類だけ**（Job と Pod の両方に付くが、名前は 1 つ）。

```yaml
labels:
  flow.tgy.io/task-uid: <uid>       # 帳簿。ポリシーのためではない
annotations:
  flow.tgy.io/phase:   調査          # UTF-8 のまま
  flow.tgy.io/run-id:  "2"
```

**ポリシーが選択するラベルは handler の `jobTemplate` が持つ。**
フェーズごとに handler があるので、それがそのままフェーズ別のラベルになる — しかも
利用側の語彙で。

```yaml
kind: TaskHandler
spec:
  phase: 調査
  jobTemplate:
    template:
      metadata:
        labels: {role: cnp-reader}   # 名前も語彙も利用側のもの
```

あとは既存の仕組みが反応する。

| 何を | 誰が決めるか |
|---|---|
| ネットワーク | **クラスタのポリシー機構**。何を選択するかは利用側が決める |
| Pod の中身（SA / RuntimeClass / securityContext） | **handler の `jobTemplate` が宣言** |
| profile の選択可能範囲 | admission |

**framework はネットワークポリシーの機構を知らない。** Cilium でも Calico でも
NetworkPolicy でも、あるいは何も無くても動く。Pod がポリシーに選ばれるために何を
持つべきかはクラスタ側の性質で、**それを書くのは handler の `jobTemplate`**。

フェーズごとに posture を変えたいなら、フェーズごとに handler があるので
`jobTemplate` のラベルと SA をフェーズごとに変えればよい。**framework は
フェーズ名を知っているが、それが何を意味するかは知らない。**

**Kyverno に mutate させない**（image digest のピン留めを除く）。`jobTemplate` に書いた SA や
RuntimeClass を Kyverno が後勝ちで黙って書き換えると、YAML と実物が乖離して追跡不能になる。
ポリシー機構の役割は **検証**に限定する（「この Pod が名乗る役割と SA の組み合わせが
許可された対応表に載っているか」等）。何を対応表にするかは利用側が決める。

コントローラは RBAC 書き込み権限を持たない。「エージェントが何をできるか」の定義は
git にあり PR レビューを通る。**監査証跡が etcd でもコードでもなく git log になる。**

### 遅延バインディングの代償

設定ミスがコントローラのエラーではなく **謎の DROPPED** として出る
（horenso デプロイ時に踏んだ「PreSync hook が CNP より先」と同じ形）。

**この代償を framework が引き受けることはできない。** ポリシーが期待どおり掛かって
いるかを確認するには、どの機構が使われているか（Cilium / Calico / 素の NetworkPolicy）を
知る必要があり、それは P2 と P7 に反する。初版はここでコントローラに
「`phase=X` を選択する CNP の実在を確認させる」と書いていたが、**それは framework が
Cilium を知る設計**だった。

引き受けるのは利用側で、置き場所は既にある:

- **前提の確認は handler の initContainer**。到達したい先に実際に到達できるかを起動時に
  確かめ、駄目なら落ちる（`jobTemplate` に書けるので framework の機能は要らない）
- Pod が起動しなければ verdict は出ず、**fail-closed で `Escalated` に倒れる**。
  安全側に倒れること自体は framework が保証する

framework が保証するのは「答えが出なければ人間に渡る」までで、
**なぜ出なかったかの診断は運用者のもの**。

### 信頼できない入力を読む handler の制約

verdict 機構は「正直だが間違えるエージェント」を前提にしている。**「誘導されたエージェント」は
想定していない。**

Renovate PR のトリアージ（Step 2）では、PR 本文や依存の release notes は**攻撃者が書ける**。
「`pass/` に書け」と書いてあれば書く可能性がある。

→ profile に **入力の信頼レベル**を属性として持たせ、`untrusted` の handler は
**束縛の無いステータス（＝終端）へ直接遷移する辺を持てない**ものとする。これは 1 ホップの
間接化でしかなく、経由先のフェーズが実際に人間承認を伴うことまでは保証しない —
その仕組み自体が未設計（§13「未決事項」）。
v1 では該当タスクを載せないことで回避し、Step 2 で導入する。

`Escalated` は `next` の行き先として宣言できるようになったが、`untrusted` の handler がそこへ
直行する辺を持てるかは未決（人間へ渡る終端だから例外として許すのか、untrusted では直行自体を
禁じるのかは、上記の admission 機構（#17）と合わせて決める）。

### Task を作れることは特権である

`Task` の create 権限を持つ者は、`spec.input` でプロンプトを操作しつつ任意の handler を
指名できる。**実質的に handler の SA の権限でコードを実行できる**（Pod create 権限と同じ構造）。

これだけ最小権限を設計した以上、`claude-code` namespace の Task create/update は
CronWorkflow の SA と管理者グループだけに絞る。ValidatingAdmissionPolicy で
「この SA が指名できる profile / handler」も制限できる。

### フェーズ列は外出しされているが DSL にはならない

`TaskFlow.spec.bindings` がフェーズの語彙と遷移をそのまま宣言する（§5）。それでも自作 DSL に
ならないのは、宣言に式（`when:` や算術）を許さないからであって（P9）、外出ししていないからではない。

- **固定されているのは遷移の枠組みと失敗系**（`Escalated` / `Failed`）だけ。フェーズ名も、
  それらをどう辺で結ぶかも利用側が決める
- **profile はどのフェーズが必須かを検証する**（`investigate` なら特定の 2 フェーズが必須、
  それ以外の名前や個数は縛らない）

CNP / Kyverno は「利用側が自分で決めた名前」に対して書ける。フェーズラベルの値がコントローラの
語彙から出てくるわけではない。

---

## 9. コンテキスト共有

**用途で分ける。1 箇所にまとめると壊れる。**

| チャネル | 中身 | 実体 | 寿命 |
|---|---|---|---|
| ワークスペース | 作業ツリー・plan.md・diff | S3 prefix または PVC | タスク中 |
| 経緯・メモ（対人） | 各フェーズの報告・判断理由 | horenso posts | 永続 |
| 横断知識 | 過去タスクの教訓 | Qdrant（memory.infra） | 永続 |
| 制御状態 | phase / bindings / 参照 URL | Task.status | タスク中 |

### 「人間への報連相」と「フェーズ間の引き継ぎ」は別物

| | 対人 | 対フェーズ |
|---|---|---|
| 量 | 少なく要約されている | 多い・生 |
| 形 | 散文 | 構造化・ファイル |
| 寿命 | 永続 | タスク終了でゴミ |
| 落ちたら | 困るが進行はできる | **進行が止まる** |

引き継ぎを horenso に置くと **horenso がエージェント実行の SPOF になる**。
既定の引き継ぎは **ワークスペースのファイル**（追加依存ゼロ、コントローラ再起動に耐える）。

### store は profile ごとに既定を変える

| profile | store | 理由 |
|---|---|---|
| investigate | **S3 prefix** | 短命・読み取り専用・並列可。PVC の attach サイクルが無駄、掃除も lifecycle に丸投げできる |
| implement | PVC | git ツリーが要る。rework で生き残る必要がある |

レイアウトに runID を含める：`<path>/results/<runID>/<directory>/report.md`（`<path>` は handler が決める）
（`<directory>` は宣言した `next` の値が選ぶ、その run のディレクトリ名）
→ 遅れて到着した古い run が書いても別の場所になり、現在の判定を汚染しない。

**エージェントから見えるのは書き込み側の平らなパス**（`<path>/ok` 等）だけで、runID は現れない
（§7「ワークスペースのレイアウト」）。

### 実装はユーザーの pod spec 側（コントローラの機能ではない）

以下は「推奨パターン」であってコントローラのフィールドではない（P7）。
init コンテナとサイドカーで実現する。

backend の違いは init/publish サイドカーが吸収する（CSI 的な発想）。

- `workspace` → PVC をそのまま
- `horenso` → init で posts を `in/` に落とす、終了時に `out/report.md` を POST
- `qdrant` → 起動時に類似タスクを引いて `in/prior-art/` に置く

**store を差し替えてもプロンプトは 1 文字も変わらない。**

### 人間への報告は絞る

全フェーズで報告させると板がノイズで埋まる。人間が判断する必要があるときだけ：

- **`Escalated` / `Failed`**（framework が認識する失敗系。どのフローでも共通）
- **`terminals` で `Failure` と宣言された終端**（§5）。framework が Warning Event を出し、
  `Ready=False` になる（metric は終端の種類を問わず常に出るので、この一件に限った話ではない）。
  **どの名前が `Failure` かは flow が言う**ので、framework は相変わらず語彙を持たない
- **flow が定義した終端ステータス**（例の `PlanReview` / `Done` はその一例であって、
  framework が知っている名前ではない。どこで報告するかは flow の binding が決める）

implement→review のピンポンは `.agent/` の中で完結させ、人間には見せない。

---

## 10. 掃除

| 層 | 手段 |
|---|---|
| K8s 内（Workflow / PVC / ConfigMap） | **ownerReferences** で Task 所有 → カスケード |
| Task 自身 | **TTL**（succeeded 1h / failed 168h）※ etcd 保護のため必須 |
| K8s の外（S3 prefix / git ブランチ / Harbor タグ） | **`task-uid` + 定期 sweep**（1 本に統合） |

### S3 lifecycle は使わない（2026-08-22 実測）

当初は bucket lifecycle に丸投げする想定だったが、実測して却下した。

- SeaweedFS **4.42 は `PutBucketLifecycleConfiguration` を受け付ける**。PUT が通り、GET で
  ルールがそのまま返る。**動いているように見える**
- 一方 upstream の [issue #6619](https://github.com/seaweedfs/seaweedfs/issues/6619) が **open のまま**で、
  「prefix に一致する全オブジェクトを、経過日数に関係なく削除する」と報告されている
- 4.42 が該当するかは 24 時間以上の経過観察なしには確認できない。**確認できないまま設定すると、
  実行中タスクの成果物が消える**（fail-closed で Escalated にはなるが、原因が分からない）

### Task の TTL（実装済み）

- 終端に着いた瞬間に **`status.expiresAt`** を焼く。規則は 1 本 — **人間が来て見る必要がある終端は
  `ttl.failed`、それ以外は `ttl.succeeded`**。該当するのは終端の意味 5 値（§5）のうち 3 つ:
  `Escalated`（フレームワークが行き詰まって着いた場合でも `next` で宣言された辺を通って着いた場合でも）、
  `Failed`、そして **`terminals` で `Failure` と宣言された終端**。`Success` と `Undeclared` は `ttl.succeeded`。
  どちらを使うかは誰が宣言したかではなく、人間が見る必要があるかどうかで決まり、それは flow ではなく
  フレームワークが決める
- 期限を Task 自身に持つので、**Escalated 後に flow が編集・削除されても消える時刻は変わらない**（予約フェーズが
  flow 無しで終端なのと同じ理由）。flow が存在せず `Failed` になった Task は読む TTL が無い間は残る。これは意図した
  仕様で、**同名の flow が後から現れれば次の reconcile で backfill され、その TTL で消える**（下記）
- `ttl` は CRD default（`succeeded: 1h` / `failed: 168h`）で埋まる。「必須」は validation ではなく default で満たす
- コントローラは `expiresAt` を過ぎた Task を UID 前提条件付きで delete する。Job は ownerReference で追従する
- この機能より前に終端へ着いた Task（`expiresAt` が無い）は、次の reconcile で早期 return する分岐の直前に
  flow の TTL から一度だけ焼く（backfill）。この reconcile 時点でも flow が無ければ焼かずに残り、上と同じく
  flow が後から現れるのを待つ

→ **S3 の掃除も `task-uid` の sweep に寄せる。** 元々 git ブランチ・Harbor タグ用に sweep が必要なので、
掃除機構が 1 本に減る。「対応する Task が存在しない `task-uid` の prefix を消す」だけで、
経過日数の判定すら要らない（TTL で Task が消えた時点で対象になる）。
- finalizer は **best-effort + デッドライン**。N 分試して駄目なら外し、漏れは sweep に拾わせる。
  掃除の正しさを finalizer に賭けると、GitHub API が 500 を返しているだけで object が
  永久に Terminating で刺さる

---

## 11. 却下した案と理由

| 案 | 却下理由 |
|---|---|
| horenso をフェーズ間引き継ぎに使う | horenso がエージェント実行の SPOF になる。対人と対マシンは別チャネル |
| コントローラが SA / Role / CNP を生成する | コントローラが全エージェント権限の和集合を持つ最大の攻撃対象になる |
| フェーズ列を宣言的に定義できるようにする | 自作 DSL 化 → Argo Workflow の劣化再実装 |
| 長命 Sandbox でフェーズを跨ぐ | per-phase 権限モデルが崩壊。価値のピークとコストのピークが一致しない |
| verdict をエージェントの最終メッセージからパース | 切れる・拒否される・ドリフトする。成果物から取る |
| verdict ディレクトリを `ok` / `ng` にする | 遷移表のキーとの変換表がいずれズレる |
| 差し戻し先をエージェントに決めさせる | 制御フローがプロンプト内に入り、テスト不能・停止性が保証できない |
| Argo Workflow の再帰テンプレートで差し戻しを表現 | DAG は非巡回。回数も追えず UI も読めなくなる |
| 判定不能を pass 扱い | 論外。fail-closed |
| handler が Argo の WorkflowTemplate を参照する | コントローラに Argo 依存が焼き付く。フェーズグラフをコントローラに移した時点で 1 フェーズ = 1 ステップであり、Job で足りる |
| Job の `backoffLimit` でインフラリトライ | リトライ機構が 2 つになる。再試行の可否（何も走らなかったか）と attempt の勘定はコントローラの仕事で、Job 内リトライは走った handler を数えずにやり直す |
| 予約フィールドを黙って上書き | 「書いた値と違う」で混乱する。admission で落として理由を返す |
| S3 の掃除を bucket lifecycle に任せる | SeaweedFS は API を受け付けるが upstream に未修正の全件削除バグ（#6619）。動くように見えるのが最も危険。sweep に統合 |
| handler に `promptConfigMapRef` / `contextStores` / `publishTo` を持たせる | コントローラに LLM 固有の語彙が漏れる。全部ユーザーの pod spec に畳める（P7） |
| コントローラが store を list して verdict を取る | コントローラが S3 認証情報と store の種類を知ることになる。termination message で足りる |
| API キーをコントローラが管理 | 普通に `envFrom: secretRef`。ユーザーの pod spec の話 |
| グローバルな遷移表をコントローラが持つ | `(Review, pass)` の行き先が profile 依存になり、遷移関数に profile 引数が要る。辺を束縛側に置けばこの分岐自体が消える |
| 予算消費を `consumesBudget: true` で宣言 | 付け忘れる。訪問済みフェーズへの遷移を実行時に検出すれば迂回不可能 |
| `next` に行き先のデフォルトを持たせる | 隠れた挙動。必須にして書かせる（P8） |
| 実行時の構造矛盾を修復する | 曖昧なまま進むより止まる方が安い。`Failed` にして作り直させる |
| フェーズ内に順序や `stopOnFailure` を持たせる | 安いゲートを前段のフェーズに置けば済む。機構を増やさない |
| 複数 handler の合成規則を設定可能にする | DSL 化する。「最も不利な verdict が勝つ」で固定 |
| flow の起動可否を ValidatingAdmissionPolicy で縛る | `TaskFlow` を namespaced にして同一 namespace 解決に限れば、素の RBAC で足りる。CEL も webhook も不要 |
| 同一 handler の複数フェーズ束縛を禁止（自己レビュー禁止） | 権限は handler 作者の責務で、束縛も git のレビューを通る。正当な再利用（lint を Checks と Verifying に）も塞ぐ |
| プロンプトから事実の説明を外し、モデルの知識に委ねる | **実測で 3 回中 1 回しか正解しない**（§17）。導出できるという仮定が誤り |
| prepare / publish サイドカーを利用側の pod spec に書かせる | 判定の回収はコントローラの責務（§2）で、両端とも taskflow のコード。片端だけ handler ごとに 30 行複製させていた。P7 が排除するのは LLM 固有の語彙であってサイドカーではない（#73、§7） |
| 注入コンテナの uid を固定して「その uid を使うな」と handler に課す | Istio 1337 / Linkerd 2102 はそうしていて、同 uid で迂回した事故（linkerd2 #14796）を誰も検出していない。コントローラが template の uid を避けて選べば規則自体が消える |

---

## 12. 段階

### Step 0（先にやる）— コントローラを書かない

cnp-check を **plain な 2 ステップ Argo Workflow** で 1〜2 週間回す。

やること：init のディレクトリ作成 + パーミッション、サイドカーで封する、verdict ディレクトリ 3 つ、
S3 publish、プロンプト調整。

やらないこと：CRD、遷移表、コントローラ。

理由：未検証なのは **「verdict が実際に役に立つか」だけ**。CRD と遷移表は役に立つと分かった後なら
確実に元が取れる純粋な配管。**先に不確実な方を潰す。**

判断材料：**指摘の有用性**（判定が本当のことを言っているか）と **escalate 率**（人間を呼ばずに
決められているか）。どちらも能力の指標であり、コストの指標ではない。

かつてここに「1 回あたりのトークン消費」を挙げていたが、**外した**。ホームラボには償却すべき予算が
無く、認証はサブスクなので `total_cost_usd` は誰も払っていない換算値になる。支払っていない金額で
門を判定する形になっていた。実行の重さが問題になるとすれば 5 時間のレート枠だが、**それは当たれば
分かる**（`rate_limit_event` が出る）ので、当たってもいない制約を先に測る理由が無い。

### Step 1 — コントローラ

`TaskFlow` + `TaskHandler` + `Task`、runner は `Job` のみ、profile は `investigate` のみ。
循環の骨格（遷移表 / runID / reworkBudget / history）は最初から入れるが、
investigate では 1 つも発火しない。**正しさは単体テストで確かめ、実クラスタでは
一番安全なタスクだけ回す。**

### Step 2 — 横展開

`investigate-alert.sh` のフェーズ化、cluster-observe、Renovate PR トリアージ、
trivy 指摘の仕分け、GHA でやっているレビュー系の移設。

※ workflow を書き換える PR は構造的に GHA でレビュー不能という既知の壁があるが、
クラスタ内で回せば GHA のトークンモデルに縛られないためその問題自体が消える。

### Step 3 — implement 系

Implementing / Review の循環を有効化。PVC store、rework 予算、長命 runner の要否を再評価。

---

## 13. 未決事項

- profile の粒度（`investigate` / `implement` / `infra-modify`？）※ CRD ではなくコントローラ組み込みの検証スキーマで確定
- ワークスペース PVC のライフサイクル（完了で消す / rework のために残す、NFS か SeaweedFS か、RWX の要否）
- ~~コントローラの実装言語~~ → **Go + kubebuilder / controller-runtime に確定**（Discussion #9）。
  ワークスペース唯一の Go になるが、CRD コントローラの生態系がここに集中しており、
  envtest / CEL / RBAC 生成を自前で組み直す方が高くつく
- ~~新リポの名前~~ → **`taskflow` に確定**。`agent-phases` は、この設計が既に外した誤りを 2 つ
  含んでいた（`agent` = `AgentTask` から外した誤称、`phases` = group 名として却下した理由）
- ~~`AgentHandler` の改名~~ → **`TaskHandler` に確定**（handler は lint / CI / 人間になりうるので誤称だった）。
  ~~やるなら CRD を 1 行も書いていない**今**~~ → 実体側も含めて `TaskFlow` / `TaskHandler` / `Task`、
  group は `flow.tgy.io` で確定済み（§4）
- `Escalated` から戻る辺を定義するか（現状は終端。人間は新規タスクを作り直すことになり履歴が切れる）
- profile の信頼レベル属性（`trusted` / `untrusted`）の導入時期
- ~~開始フェーズの決め方~~ → **明示フィールドに確定**（`TaskFlow.spec.start`、`+kubebuilder:validation:MinLength=1`
  で必須）。検討していた候補は 2 つ：
  - 明示フィールド（採用）— P8「デフォルト値・暗黙のマージ・推測による修復をしない」と素直に整合する
  - 推論（`next` の値として一度も現れないフェーズを開始とみなす）— 書く量は減るが、開始フェーズ自身が
    rework の戻り先になっている flow では候補が消える（P8 の禁じる「推測」に当たる）ため見送った
- **`untrusted` handler に人間承認を必ず経由させる仕組み**（§8「信頼できない入力を読む handler の制約」）。
  現状決まっているのは「束縛の無いステータスへの直接の辺を禁じる」ことだけで、経由先のフェーズが
  実際に人間承認を伴うかは何も保証しない。「このフェーズは人間承認ゲートである」という属性と、
  `untrusted` からの全経路がそこを通ることを admission で検証する仕組みが要る。v1 では該当タスクを
  載せないことで回避する（§8）


---

## 14. リポジトリ構成

コントローラは Go のビルド・テスト・イメージを持つため、マニフェストと values しか置かない
home-cluster には同居させない。**新規リポを 1 つ作る。**

| リポ | 中身 |
|---|---|
| 新規（`taskflow`） | コントローラ本体、CRD 定義（`config/crd/`）、遷移表と単体テスト、サイドカー、イメージビルド |
| home-cluster | `apps/phases.yaml`、`TaskHandler` の箱、phase 別 CNP、Kyverno ポリシー |

**「エージェントが何をできるか」は home-cluster 側（PR レビューを通る）、「どう動くか」は新リポ。**
P2（コントローラはポリシーを持たない）の分離がリポ境界と一致する。

- 命名は `horenso` / `digest-cooldown` の前例に倣って bare name
- home-cluster が既に public でクラスタ構成を晒しているため、追加で守る秘密がなく public でよい
- 立ち上げは `/repo-setup --new`（ruleset / 共有 lint / CI Gate / renovate / aqua）
- **注意**: イメージは Harbor + cosign 署名 + Kyverno mutateDigest 経路に乗る。
  retention が digest を刈って Pod が復帰不能になった件があるので、retention ルールを先に確認する

---

## 15. 運用上の注意（設計と別に効いてくる）

1. **escalate 率が実質的な健全性指標。** 3 割エスカレーションすると人間が読まなくなり、
   fail-closed が運用上 fail-open に化ける。verdict ごとに Prometheus メトリクスを出す
2. **handler のプロンプトの質は、この設計では 1 ミリも改善しない。** 配管が綺麗でも
   レビューが凡庸なら意味がない。唯一の未検証領域であり、Step 0 で潰す対象
3. **トークン消費が見えない。** rework ループ付きで cron に載ると効いてくる。
   `status` にコストを持つのは後付けが面倒なので最初から。
   **実測値: 裁定 1 回（sonnet-4-6 / 2 ターン / 37 秒）で $0.095。** 1 日 1 回の cron なら月 $3 程度、
   rework が 3 往復すると 1 タスク $0.3〜0.4 になる計算
4. **CRD が肥大する。** `template.spec`（`corev1.PodSpec`）を標準のまま埋め込むため、生成される CRD YAML は CronJob 並みになる。
   `kubectl apply` の last-applied-configuration 注釈は 262144 バイト上限なので素朴に apply すると失敗する。
   ArgoCD 側で `ServerSideApply=true`（または `Replace=true`）を Application に付けておく
5. **Kata + NFS の workspace では、handler が exit する前に `sync` する。** ゲストは NFS 上の
   workspace を virtio-fs 越しに見ており、**書き戻しが残ったままコンテナが exit すると
   ゲストエージェントが応答を失う**。shim は終了コードを取れず、Pod 内の**全**コンテナに 255 を
   付ける — ファイルは落ちているのに Job は Failed になり、infra retry を焼いてから Task が
   Failed で終わる。判定ディレクトリは正しく書かれているので、**書けたのに落ちる**という
   最も紛らわしい壊れ方をする。

   **2026-09-05 実測（wn-03 / kata-containers 3.30.0 / qnap-nfs RWX）**:

   | 中身 | 結果 |
   |---|---|
   | NFS PVC に 1 ファイル書いて即 exit | exit 255（5/5 再現） |
   | 同じ書き込みのあと `sync` | exit 0 |
   | 同じ書き込みのあと `sleep 1` | exit 0 |
   | 書いたファイルを消してから exit | exit 0 |
   | 読むだけ（`ls` / `cat`） | exit 0 |
   | mkdir だけ（メタデータのみ） | exit 0 |
   | runc で同じ書き込み | exit 0 |

   **これは framework 側では手当てできない。** 巻き添えは同じサンドボックス全体に及ぶので、
   `flow-publish` サイドカーが後から `sync` しても間に合わない（サイドカー自身も 255 になる）。
   main コンテナ 1 つ + native sidecar 1 つの構成で再現済み。したがって
   **handler の最後の 1 行が `sync` である**ことを、workspace を持つ flow の作法として要求する。
   emptyDir だけの handler には要らない（ページキャッシュがホストのメモリで閉じるため）。

---

## 16. 使用例（cnp-check 一式）

### home-cluster 側（箱を先に作る）

```yaml
apiVersion: flow.tgy.io/v1alpha1
kind: TaskHandler
metadata: {name: cnp-planner}
spec:
  phase: 調査
  runner: {type: Job}
  timeout: 20m
  maxInfraRetries: 2
  workspace: {volume: work, mountPath: /workspace}   # コントローラが prepare / publish をここに付ける（§7）
  jobTemplate:
    template:
      metadata:
        labels: {role: cnp-reader}               # 下の CNP がこれを選ぶ（framework は知らない）
      spec:
        runtimeClassName: kata
        serviceAccountName: agent-cnp-reader     # CNP と Pod の get/list のみ
        restartPolicy: Never
        securityContext:
          runAsUser: 65533                       # 必須。コントローラはこれと違う uid で run ディレクトリを閉じる
          runAsNonRoot: true
        volumes:
          - name: work
            emptyDir: {}
          - name: prompt
            configMap: {name: cnp-planner-prompt}   # プロンプトはユーザーの pod spec の話（P7）
        containers:
          - name: agent
            image: registry.infra.tgy.io/tools/claude-code:latest
            workingDir: /workspace
            # S3 認証情報も egress も持たない。書けるのは /workspace 直下の宣言済みディレクトリだけ
            volumeMounts:
              - {name: work, mountPath: /workspace}
              - {name: prompt, mountPath: /prompt, readOnly: true}
---
apiVersion: flow.tgy.io/v1alpha1
kind: TaskHandler
metadata: {name: cnp-reviewer}
spec:
  phase: 報告
  runner: {type: Job}
  timeout: 20m
  maxInfraRetries: 2
  workspace: {volume: work, mountPath: /workspace}
  jobTemplate:
    template:
      spec:
        runtimeClassName: kata
        serviceAccountName: agent-readonly   # 何も書けない
        restartPolicy: Never
        securityContext: {runAsUser: 65533, runAsNonRoot: true}
        volumes:
          - name: work
            emptyDir: {}
          - name: prompt
            configMap: {name: cnp-reviewer-prompt}
        initContainers:
          - name: notify                       # 終端なので人間にも出す。宛先は利用側の話（P7、§9）
            restartPolicy: Always
            envFrom:
              - {secretRef: {name: horenso-webhook}}
            volumeMounts: [{name: work, mountPath: /workspace, readOnly: true}]
        containers:
          - name: agent
            image: registry.infra.tgy.io/tools/claude-code:latest
            workingDir: /workspace
            volumeMounts:
              - {name: work, mountPath: /workspace}
              - {name: prompt, mountPath: /prompt, readOnly: true}
```

`prepare` と `publish` は**同一イメージの 2 サブコマンド**で、コントローラが Job 生成時に足す（§7）。
判定を封する側のコードが 1 箇所に集まり、handler には現れない。プロンプト（ConfigMap volume）も
人間への通知（`notify` サイドカー）も、コントローラの知らないユーザーの pod spec に畳まれている
（P7、§11 で却下した `promptConfigMapRef` / `contextStores` / `publishTo` はここに畳む）。

対応する CNP（**handler が `jobTemplate` に書いたラベル**で選択。これも home-cluster 側であり、
framework はこの存在を知らない）:

```yaml
apiVersion: cilium.io/v2
kind: CiliumNetworkPolicy
metadata: {name: agent-cnp-reader, namespace: claude-code}
spec:
  endpointSelector:
    matchLabels: {role: cnp-reader}
  egress:
    - toEntities: [kube-apiserver]
    - toFQDNs: [{matchName: api.anthropic.com}]
    # S3 は init/sidecar が触る。main には認証情報を渡さない
```

### 実際に投げるもの

型（一度だけ書く。home-cluster に置きレビューを通る）:

```yaml
apiVersion: flow.tgy.io/v1alpha1
kind: TaskFlow
metadata: {name: cnp-check}
spec:
  profile: investigate            # 調査 / 報告 が必須のスキーマ
  start: 調査
  bindings:
    調査:
      handler: cnp-planner
      next: {報告: ok, Escalated: escalate}   # 「判定できない」を書いて表明する出口（§5）
    報告:
      handler: cnp-reviewer
      next: {おわり: sent}
  terminals:
    おわり: Success                # 宣言しないと Undeclared のまま（§5）
  reworkBudget: 0                 # investigate に循環は無い。辺は前進しかしないので影響も受けない
  maxInFlight: 1
  ttl: {succeeded: 1h, failed: 168h}
```

実体（毎回作られる。cron でもエージェントでも人間でも同じ）:

```yaml
apiVersion: flow.tgy.io/v1alpha1
kind: Task
metadata:
  generateName: cnp-check-
  namespace: claude-code
spec:
  flow: cnp-check
  input:
    scope: "全 namespace"
    focus: "CNP 未カバーの Pod と、直近 24h の DROPPED フロー"
```

### 定期実行（利用側の都合。コントローラは関知しない）

```yaml
apiVersion: argoproj.io/v1alpha1
kind: CronWorkflow
metadata: {name: cnp-check, namespace: claude-code}
spec:
  schedule: "0 3 * * *"
  timezone: Asia/Tokyo
  workflowSpec:
    entrypoint: create
    templates:
      - name: create
        resource:
          action: create
          manifest: |
            apiVersion: flow.tgy.io/v1alpha1
            kind: Task
            metadata: {generateName: cnp-check-}
            spec:
              flow: cnp-check
              input: {scope: "全 namespace"}
```

スケジュール用の CRD は作らない。CronJob で `kubectl create` しても等価。
**生成元はグラフを知らない**ので、フローを変えても cron 側は無変更で済む。

### status（報告 実行中）

```yaml
status:
  phase: 報告
  runID: 2
  reworkBudget: 0
  currentRun:
    phase: 報告
    runID: 2
    jobName: cnp-check-x7f2-2-64442e2b      # <task>-<runID>-<フェーズ名のハッシュ 8 桁>（§4）
    deadline: "2026-08-21T12:20:00Z"
  artifacts:
    store: "s3://agent-tasks/cnp-check-x7f2/"
  history:
    - {phase: 調査, runID: 1, directory: ok, outcome: Declared, ref: "s3://.../1/"}
  conditions:
    - {type: Ready, status: "True", reason: HandlerResolved}
```

### kubectl から見える形（additionalPrinterColumns）

`Task` の printcolumn は Flow / Phase / Run / Budget / Age（`kubectl get task`、`agenttask` という
resource 名は存在しない）。`Profile` は `TaskFlow` 側の列であって `Task` には無い。

```
NAME             FLOW        PHASE      RUN   BUDGET   AGE
cnp-check-x7f2   cnp-check   報告        2     0        4m
cnp-check-m91k   cnp-check   おわり      2     0        22h
alert-inv-p03z   cnp-check   Escalated  3     0        2h
```

`おわり` は `Escalated` と違ってフレームワークの予約語ではない — この flow の作者が終端に選んだ
ただの名前で、他のどのフェーズとも扱いは同じ（§5「`Done` を特別扱いしない」）。

### 人間が判定を入れる場合（構想。未実装）

`runner.type: External` で人間がレビューする経路はまだコードになっていない。3 CRD とも
`+kubebuilder:subresource:status` しか持たず、専用の `verdict` サブリソースは存在しない。
実装するときのイメージ：

```bash
# 将来案。現状の CRD には無いので今は動かない。
kubectl patch task cnp-check-x7f2 --subresource=verdict --type=merge   -p '{"directory":"ok","runID":2,"reason":"確認済み"}'
```

**`runID` を必須にする。** 一致しない patch は拒否する。人間が古い画面を見て承認した場合に、
既に次の run へ進んだタスクを巻き戻さないため（機械の遅延 verdict と同じ扱い）。
このサブリソースへの PATCH だけを Kanidm グループに RBAC で許可する想定。

---

## 17. Step 0 の実測（2026-08-22）

`cnp-check` を home-cluster に載せて 7 回走らせた結果。判定機構と、その上に乗るプロンプトを分けて評価する。

### 配管は 7 回とも一度も嘘をつかなかった

判定ディレクトリ・パーミッション束縛・output parameter による持ち出し・切り詰め注記のいずれも、
一度も誤動作していない。**外れたのはプロンプトだけ。**

これが効いた。どちらの層が壊れたかを毎回即断でき、プロンプトの改善に集中できた。
配管も揺れていたら切り分けは一つもできなかった。

> **機械は安定した基盤、プロンプトは可変のペイロード。** 基盤が正直だからペイロードを反復できる。

権限束縛は配線前に使い捨て Pod で検証済み：正規の分類には書ける、4 つ目の `mkdir` は EACCES、
`out/` の `chmod` は拒否、直下への野良ファイルも拒否。

### プロンプトの精度がそのまま出力の精度になる

| 回 | 変更 | 結果 |
|---|---|---|
| 1 | — | 検出は的確。**診断は 2 箇所とも誤り**。対処案が `toCIDR: 0.0.0.0/0`（FQDN 許可リストを任意 HTTPS に置換する改悪） |
| 2 | 根拠の併記を義務化、セマンティクスを明示、`0.0.0.0/0` を名指し禁止 | 誤診が消えた。確かめられない件を「未確認の推測」と明記する挙動が出た。**ただし明示したセマンティクスが誤っていた**ため別の誤りが出た |
| 3 | セマンティクスを修正（`ingressDeny` も強制を有効にする） | **独立検証と完全一致**（3 Pod、過不足なし）。危険度も方向も対処案も正しい |

2 回目の誤りは**指示側の誤り**であり、エージェントは指示に忠実だった。

### 事実の明示は必要（当初の判断は誤りだった）

「モデルは導出できるから事実は書くな、検証だけ要求しろ」と一度結論したが、n=1 の誤りだった。
セマンティクス説明を外した版を 3 回走らせた結果：

| 実行 | 検出 | 書いた判定根拠 |
|---|---|---|
| A | **3 Pod（正）** | 「`ingress` 許可リスト**または** `ingressDeny` を持たない場合」と正しく導出 |
| B | **30 Pod（誤）** | 「`ingressDeny` のみのポリシーは ingress を強制しない」 |
| C | **30 Pod（誤）** | `spec.ingress` の有無だけで判定。対処案が **`ingress: []` を追加**（no-op。効かない修正を deny-all と称する） |

**3 回中 1 回しか正解しない。** 危険なのは事実を与えることではなく、**間違った事実を与えること**。
したがって「与えない」ではなく「**与えたうえで、それが正しいことを検証可能にしておく**」。

### 既知解ケースを回帰チェックにする

プロンプトには単体テストが無い。git に入り PR レビューも通るが、壊れたかは実行するまで分からない
（`ingressDeny` の誤りも、書いた本人がレビューして通している）。

現実的な対策は**既知解ケース**。「ちょうどこの 3 Pod」が独立検証で確定しているので、
プロンプト変更時にそれを検出できるか 1 回走らせる。今回それで劣化を捕まえた。
クラスタ状態が変われば期待値も変わるので万能ではないが、明らかな劣化は捕まる。

### ばらつきは出力先で許容度が変わる

同一クラスタ状態に対する 3 回の実行で、**verdict は 3 回とも `attention` で安定**していた一方、
**中身は 3 Pod と 30 Pod の間で揺れた**。

| 出力先 | 耐性 | 理由 |
|---|---|---|
| PR / git | **高** | 人間レビューが挟まる。差分が残り、消えない |
| 人間向け通知（Discord） | **中〜低** | 最新しか読まれない。前回あった指摘が黙って消える |
| 行動を分岐させる verdict | **低** | 同一入力で結果が変わる＝**リトライで通せる** |

3 行目が設計に直結する。`Review → rework → Review` の循環は、verdict が揺れるなら
**承認が出るまで引き直すガチャ**になる。`reworkBudget` は回数を縛るだけで健全性を担保しない。

対策 2 つ（未実装、Step 3 までに要る）:

1. **rework は成果物を変えなければならない。** 再判定前に作業ツリーや PR head の SHA を比較し、
   変わっていなければ再判定させず `Escalated`。「引き直し」を構造的に潰す
2. **行動を分岐させる verdict は 1 サンプルで決めない。** N 回走らせて不一致なら安全側に倒す。
   コストは N 倍なので `infra-modify` 系だけに絞る

通知系には**状態**を持たせる。前回の findings のキー集合を保存し、**今回消えた指摘を明示する**。
ばらつきを消すのではなく可視化する方向。保存は読み取り専用の実行 SA ではなく publish 側の仕事になる。

### フェーズを足すべきかの判定基準

精度を上げた施策を分けると、**実行の内側から検出できるか**で線が引ける。

| 施策 | 効果 | 種別 |
|---|---|---|
| 根拠の併記を要求 | 誤診 2 件が消えた | **フェーズ内**で足りる |
| 事実の明示 | 1/3 → 3/3 | フェーズ内で足りる |
| 独立検証（正解との突き合わせ） | 網羅性の誤りを捕捉 | **別フェーズが要る** |

引用の欠落は「言えば自分で直せる」。「探索範囲が足りていたか」は内側からは分からない。

**ただしフェーズを増やせば上がる、ではない。** 同一プロンプトの 3 回並走は 3 / 30 / 30 Pod に割れ、
**多数決を取ると誤りが 2:1 で勝つ**。同じ盲点を持つ実行をいくら重ねても精度は上がらない。

> **フェーズは「別の盲点」を持っていて初めて効く。**

設計の 3 層はちょうどそうなっている:

| フェーズ | 問い | データ |
|---|---|---|
| Checks（lint / test） | 決定論的に壊れていないか | コード |
| Review | 判断として妥当か | 成果物 |
| **Verifying** | **実際に動くか** | **動いている系** |

Verifying の価値は「2 つ目の意見」ではなく**問いもデータも違う**こと。
qdrant の「書いてあるが効いていない」は、git を何回読んでも出てこなかった（§17 末尾）。

### 成功判定の強度（Verifying フェーズの設計に直結）

home-rss の復旧作業（別セッション、2026-08-22〜23）で出た実例。**「健全に見える」状態が
何段階も重なった。**

- 読み取り系 API（`/api/feeds`, `/api/stats`）は uuid をバインドしないので **200 を返す**
- fetcher の workflow は **Succeeded を返す**（フィード取得は成功し、insert だけが個別に失敗してログに落ちる）
- ArgoCD は `Synced / Healthy`
- 実際には uuid と timestamptz のバインドが全滅していた

しかも**エラーが 1 件ずつしか出ない**。uuid を直すと次に timestamptz が出る。
1 回通っただけでは終わりの証明にならない。

同型を本設計でも踏んでいる。`manifests/qdrant/netpol.yaml` は git にあり、レビューを通り、
マージされ、`docs/network-policies.md` にも稼働中と記載されていたが、**どのアプリからも
参照されておらず一度も適用されていなかった**（§17 末尾、PR #574）。ArgoCD は `Synced` を報告する。

| 強度 | 判定 | それが実際に証明していること |
|---|---|---|
| **弱** | workflow `Succeeded` | プロセスが 0 で終了した |
| | ArgoCD `Synced` | git の内容が API に適用された |
| | 読み取り系が 200 | その経路が通った |
| | デプロイ成功 | リソースが存在する |
| **強** | **書き込みパスを通し、保存先で結果を確認** | **意図した状態変化が起きた** |

右列が肝で、弱い側はどれも**それ自体については正しい**。誤りは観測ではなく、
そこから機構を推論した部分にある。

> **観測は自分自身についての証拠であって、そこから推論した機構の証拠ではない。**

これは今日の失敗すべてに共通する形だった:

| 観測 | 誤って推論した機構 |
|---|---|
| exit Pod 1 個のラベル | 「exit Pod はテンプレートのラベルを持たない」（他 ns で成立せず） |
| `kubectl get workflowtemplate \| grep` が 4 件 | 「全発生箇所は 4 箇所」（CronWorkflow に 2 件） |
| CNP spec を `"discord"` で grep して不一致 | 「discord を許可していない」（`toCIDR: 0.0.0.0/0:443` で通っていた） |
| workflow `Succeeded` | 「処理が意図通り完了した」（insert が全部落ちていた） |

**1 つのサンプル / 1 段の grep で見えた形を、機構の説明として書いてしまう。**
裏取り要求（§17）が効くのはここで、「何を確かめたか」を併記させると、
観測と推論の距離が本人にも読み手にも見える。

Verifying フェーズの handler は弱い側で満足してはならない。
「リソースが存在する」ではなく「**その経路を通した結果が残っている**」を確認させる。
これは prompt の指示ではなく、handler が実行するコマンドの設計の問題（`runner: Job` の
`jobTemplate` に書くもの）。

### 規約は、それを書いた本人が忘れる（2026-08-23 実測）

「per-template ラベルだけを付けると namespace のベースライン CNP から静かに外れる」という罠を、
**同じ日に 3 回踏んだ**。

| 回 | 対象 | 症状 |
|---|---|---|
| 1 | 再帰の probe | Pod が `1/2 NotReady` で 7 分、workflow は `Running` のまま |
| 2 | rss の CronWorkflow（他セッション） | exit hook が通知に失敗、無音 |
| 3 | `horenso-verify`（新規テンプレート） | `github-auth` が curl exit 28、exit hook も不達 |

3 回目の時点で、

- 1 回目の教訓を **`implement-wft.yaml` にコメントとして書いていた**
- **メモリに記録していた**（[[claude-code-agent]]）
- **PR 本文で他セッションに注意喚起していた**

それでも新しいテンプレートでは付け忘れた。

> **テンプレートごとに手で再適用する規約は、それを文書化した本人が忘れる。**

これは注意力の問題ではなく、**規約が置かれている場所の問題**。同じことをコントローラが
ラベル付与として一箇所で行えば、この失敗モードは存在しない（§4「コントローラが注入するもの」）。

Discussion #1（コントローラを書くか）に対する材料として、
「間違え方の数」が抽象論ではないことの実例になる。**書いた本人でも 3/3 で間違える。**

### エージェントに対する posture

今日の誤答はすべて整形され、根拠を挙げ、表まで付いていた。`ingress: []` を「deny-all」と
称した提案も、正解の出力と見た目で区別がつかない。

> **能力は高いと仮定してよい。ただし失敗が自己申告されないと仮定しなければならない。**

この設計の安全機構（fail-closed / 根拠の併記 / Verifying / 行動の人間ゲート / 既知解の回帰）は
すべて「無能を前提」ではなく「**誤りが検出不能であることを前提**」にしている。

系として、能力が高いなら**床を上げるより天井を上げる方が得**になる。アクセスを広げ、文脈を増やし、
難しい問題を渡すのは報われる。報われないのは、個々の出力を検査なしで信じること。

### コスト実測

| 項目 | 値 |
|---|---|
| 裁定 1 回（sonnet-4-6 / 2 ターン / 37 秒） | **$0.095** |

### 副産物として見つかった実バグ

Step 0 の目的は判定機構の検証だったが、点検そのものが実害を 3 件出した。

| 内容 | PR |
|---|---|
| `adjudicator` の判定が読めないとき `exit 0` していた（fail-silent） | #567 |
| `adjudicator` の判定が**ステップ間に一度も届いていなかった**（`emptyDir` は Pod スコープ） | #568 |
| **`manifests/qdrant/` がどのアプリからも参照されておらず、CNP が一度も適用されていなかった。** ArgoCD は `Synced` と報告し、`docs/network-policies.md` も稼働中と記載していた | #574 |

3 つ目は `cnp-doc-audit` では原理的に検出できない（マニフェストとドキュメント、つまり **git と git** を
突き合わせているため）。**クラスタ側を読む点検を作ったから見つかった。**

---

## 18. 自律運転（home-rss を試験場に）

最終的な狙いは **エージェントが自分で Issue を立て、実装し、解決するまでを無人で回す**こと。
試験場は home-rss — 別リポで影響範囲が閉じており、CI があり、public で、壊れても実害が小さい。

これは investigate 系の延長ではなく、**自分で仕事を作る系**であり、固有の要求が 3 つ増える。
`reworkBudget` は 1 タスク内の循環を縛るだけで、**タスクが増殖する方向**を何も縛っていない。

### ① 起票の冪等性

同じ Issue に対して実行のたびにタスクが増えると詰まる。

> `spec.dedupKey` を Issue 番号から導出し、**同じキーの未終了タスクがあれば作らない**（admission で拒否）

Argo の `concurrencyPolicy: Forbid` は cron 1 本に対する制御であって、
任意の生成元から作られる同一対象のタスクは面倒を見ない。

### ② 同時実行数と消費の上限

作る速度が閉じる速度を上回ると際限なく増える。

- `TaskFlow.spec.maxInFlight` — flow ごとの同時実行数上限
- 1 日あたりのトークン消費上限（`status` に実測コストを持つ話と繋がる。裁定 1 回 $0.095 が基準値）

### ③ 生成元が選べる flow の制限

「Task を作れることは特権」は、**作るのがエージェントになると現実の問題**になる。
Issue を立てるエージェントが `infra-modify` 系の flow を指名できてはいけない。

> `TaskFlow` / `TaskHandler` を namespaced にし、`spec.flow` を同一 namespace 解決に限る。
> あとは **素の RBAC**（どの namespace に Task を create できるか）で決まる（§4）

なお Implementing と Review は別 Pod でコンテキストを共有せず、レビュアーは `in/` の成果物しか
見ない。**すでに「自己」レビューではない**ので、同一 handler を禁止する規則は置かない（§5）。
「甘く見るな」はプロンプトの仕事。

### v2 候補: `TaskSequence`（階層をまたぐ連鎖）

§4 では「またぎたくなったら S3 + event で、受け手側が判断する」としたが、
**計画された連鎖**にはより素直な形がある。

```yaml
kind: TaskSequence          # コントローラと同じ namespace に置く
spec:
  steps:
    - {flow: rss-investigate, namespace: agent-tasks-safe, input: {...}}
    - {flow: human-approve,   namespace: agent-tasks-safe}
    - {flow: infra-modify,    namespace: agent-tasks-infra, input: {...}}
```

**認可のタイミングが変わる。** S3 + event が「受け手が毎回判断する」のに対し、
TaskSequence は**連鎖全体を定義時に一度 authorize する**。低権限側のタスクは
「完了する」以外の方法で高権限側を起こせず、連鎖は事前に承認済み。決めているのは
送り手でも受け手でもなく**連鎖の作者**であり、それは git のレビューを通る。

両者は排他ではない。**計画された連鎖は TaskSequence、反応的な結合は event**。

#### 置き場所はコントローラと同じ namespace

当初「触れる中で最も高い階層の namespace に置く」と考えたが、**それは規約であって強制されない**
（safe→infra の連鎖を safe に置いても止まらない）。構造で担保するなら 1 箇所に集める。

「連鎖を定義できるか」が**単一の RBAC 付与**になる。粗いが正直な粒度で、
**階層をまたぐ連鎖を定義する行為そのものが特権**であり、階層ごとに委譲するものではない。
エージェントは自 namespace の `Task` create だけを持ち、`TaskSequence` は持たない。

#### 別コントローラ・別 SA にする

TaskSequence は cross-namespace で Task を create する。これを v1 のコントローラに
持たせると「**コントローラを取れば定義済みの任意の flow を任意の namespace で起動できる**」に
なり、v1 の性質（コントローラはタスクを作らない）が後退する。

| コントローラ | 権限 |
|---|---|
| Task（v1） | 自 namespace の Job 作成と status 更新。**タスクは作らない** |
| TaskSequence（v2） | cross-ns の Task create のみ |

#### 枷 2 つ

1. **線形のリストに留める。分岐を持たせない。** 条件分岐や DAG を入れた瞬間、外側の層で
   Argo を作り直すことになる。順に実行し、pass 以外が出たら止まる、それだけ。
   分岐が要るならタスク内のフェーズでやる
2. **階層をまたぐ境目に `External`（人間）ステップを置く。** TaskSequence 自体が git レビューを
   通るので二重ではあるが、safe→infra を無人で越えられる構造は作らない

#### 純粋に追加である

`Task` / `TaskFlow` / `TaskHandler` は 1 フィールドも変わらない。紐付けはラベル 1 枚
（`flow.tgy.io/sequence`）。GC も**各 Task が自分の TTL を持つ**ので、
TaskSequence が cross-ns の掃除をする必要がない（`ownerReference` は namespace を跨げない）。

### Step の順序への影響

§12 では「investigate だけなら Argo で足りる、CRD はまだ早い」と結論したが、
**目標が自律 implement なら CRD の根拠は既に立っている**。循環・フェーズごとの権限差し替え・
外部 verdict（CI 緑待ち）が全部要り、いずれも Argo のパラメータでは表現できない。

ただし Step 0（判定機構を Argo で検証する）を先にやる順序は変わらない。
実際そこで配管の正しさとプロンプトの脆さが実測でき、CRD に持ち込むべきものが確定した。

---

## 19. Argo でやる場合のコスト（2026-08-23 実測）

**前提として、Argo でやろうと思えば全部できる。** 特化を作るのは「できないから」ではなく
「**大変だから**」であって、そこは最初から動いていない。この節はその大変さを実測で埋めるもの。

§12 / §18 で「循環・フェーズごとの権限差し替え・外部 verdict は Argo のパラメータでは表現できない」と
書いた箇所があったが、**その書き方が誤り**だった。3 つとも書ける。以下は「書けるか」ではなく
「書いたものをどう保証するか」の記録。

| 項目 | Argo での書き方（実測） | 保証の所在 |
|---|---|---|
| フェーズごとの権限差し替え | `templates[].serviceAccountName` + `templates[].metadata.labels` | **規約**（workflow レベルの `podMetadata` も併記しないと静かに止まる） |
| 循環 | 再帰テンプレート + `{{=asInt(budget) - 1}}` + `when`（予算 2 で 3 周し `exhausted` に落ちるまで実行確認） | **規約**（`- 1` を `+ 1` と書いても止まらない。`when` 2 本の補集合性も誰も見ない） |
| 外部 verdict | `suspend: {}` + `argo resume` | 素直。adjudicator の `kubectl replace` は**不要だった** |
| TTL / 同時実行制限 / 階層跨ぎ | `ttlStrategy` / `synchronization` / Argo Events | 素直 |

**書けないものは 1 つも無く、書き間違えられないものも 1 つも無い。**
制御ロジックが `when:` の文字列と `{{= }}` の算術に落ちるので、**単体テストできない**
（確かめる手段は実ワークフローを走らせることだけ）。設計が遷移表を純粋関数にした理由がここ。

周辺も同様に埋まっている: TTL は `ttlStrategy`、同時実行の制限は `synchronization`（mutex / semaphore）、
階層をまたぐ連鎖は Argo Events。

### 残る差は「できるか」ではなく「毎回間違えずに書けるか」

実測中に**自分で踏んだ**。再帰の probe で per-template ラベルだけを付け、
workflow レベルの `podMetadata` を省いた結果:

```
probe-cycle-ndc4g-implement-...   1/2 NotReady   7分
workflow status: Running
```

namespace の CNP は `claude-code: "true"` で選択するので、per-template ラベルだけの Pod は
**ベースラインのカバレッジから静かに外れ**、argoexec が API server に届かず**ただ止まった**。
失敗ですらない。

> **Argo では 2 箇所（workflow レベルと template レベル）を毎回正しく書く必要があり、
> 片方を忘れると静かに止まる。**

同じ性質の事故を今日 3 件やっている（#567 fail-silent / #568 emptyDir がステップ間で共有されない /
#570 executor RBAC の付け忘れ）。**どれも手書きの配管のバグ**で、Argo の限界ではない。

### 結論

**CRD の根拠は「Argo にできないことがある」ではない**（それは最初から前提ではなかった）。 より正確にはこうなる:

> **Argo の合成は開いているので、局所的な読解が権威にならない。**
> **CRD はフェーズ語彙を固定し `bindings` で閉じるので、到達可能な集合が定義上そこに全部ある。**

Argo の合成手段は `templateRef`（cluster-scoped 可）・`resource` テンプレートによる Workflow 生成・
再帰・Sensor による submit・そして **`workflowDefaults` による全ワークフローへの注入**。
到達可能な状態空間が静的に閉じない。

`image-build` の穴（§19 冒頭）がその実例だった。`discord-notify` は ClusterWorkflowTemplate で
`workflowDefaults` 経由で全 namespace に注入されるので、**image-build のマニフェストを何度読んでも
exit Pod が現れることは書いていない**。誰も書いていないステップが、誰も参照していないテンプレートから
注入され、その namespace で宣言されていない通信を必要とする。**合成が開いていることがバグを不可視にした。**

ただし**開いていることは Argo の価値でもある**。想定外のことをやれるのが強みで、CRD はそれを意図的に
捨てている。だから置き換えではなく共存になる — 推論が必要な部分（エージェントが書く、何が起こりうるかを
知る必要がある）だけ閉じた領域を作り、決定論的な周辺処理（ビルド、バックアップ、tofu apply）は
Argo のままにする。

具体的な差としては:

1. **間違え方の数**。回収ロジック・transport・fail-closed・executor RBAC・ラベルの二重管理が、
   書く場所ごと消える
2. **運用の一様性**。TTL / dedup / in-flight 上限 / 外部リソースの掃除を、タスク種ごとに書き直さない
3. **問い合わせの単位**。「今どのタスクが走り、何を結論したか」が 1 つのオブジェクトになる

いずれも「Argo では不可能」ではなく「**Argo では毎回組み直す**」。
これは §17 で出した「行数では勝てない、勝つのは書き間違えられる場所が無いこと」と同じ結論に、
別の経路で戻ってきている。

### 本設計のうち、オーケストレータに依存しない部分

今日 Step 0 で検証した中身は**すべて Argo 上でそのまま動いている**:

- 判定ディレクトリ + パーミッションによる語彙の強制
- fail-closed（答えが 1 つに定まらなければ直接 Escalated）
- 根拠の併記要求
- 成功判定の強度（書き込みパスを通す）

**これらは CRD を待つ理由にならない。** 先に Argo 上で積み上げてよい部分であり、
実際そうしている（cnp-check）。CRD を書くかどうかは、上の 1〜3 が
タスク種の数に見合うかという**量の判断**であって、能力の判断ではない。

## 20. 装置の失敗 7 件（horenso-verify、2026-08-23 実測）

書き込みパスを通す verify フェーズ（horenso の branch を使い捨て PostgreSQL に対して起動し、
日本語長文を往復させてバイト単位で比較する）を Argo 上に作った。**最初の `pass` までに 7 回落ちた。**

エージェントの判断は 1 度も間違えていない。7 件すべて配管である。

| # | 失敗 | 装置由来 |
|---|---|---|
| 1 | `spec.entrypoint is required`（`argo submit --from` は `spec.arguments` の既定値も無視する） | ● |
| 2 | emissary executor が entrypoint を解決できない（docker.io へ egress が無い）→ `command` / `args` の明示が要る | ● |
| 3 | `podMetadata` 忘れ → namespace の CNP がその Pod を選ばない（**1 日に 3 回**） | ● |
| 4 | `serviceAccountName` 無し → `default` SA が workflowtaskresults を作れず exit 64 | ● |
| 5 | `/src` に clone（非 root で書けない） | |
| 6 | イメージに python3 が無い | |
| 7 | パイプ越しの `\|\| fail` が clone の失敗を拾わない | |

**7 件中 4 件が装置由来。** そして 4 件は同じ形をしている:

> **使う側が決めるべき「内容」と、提供側が持つべき「機構」が、同じ 1 枚の YAML に混ざっている。**

内訳はさらに 2 つに割れる:

| | 失敗 | 何が漏れているか |
|---|---|---|
| 純粋に提供側の漏れ | #1 `entrypoint` / #2 emissary の解決 | 実行機構の都合を、フロー作者が知らないと書けない |
| 内容は使う側だが場所が無い | #3 ラベル / #4 SA | **何を書くかは使う側のもの。宣言を 1 箇所で受ける口が無い** |

### #3 が一番はっきりしている

要求は「この namespace の Pod は `claude-code: "true"` を持たないと CNP に選ばれない」で、
これは**クラスタ側の不変条件**であってフローごとに選ぶ設定ではない。にもかかわらず:

- 宣言する場所が workflow の `podMetadata` と template の `metadata` に分かれ、**挙動が違う**
  （後者だけだと namespace CNP から外れる、§19）
- 一度書いて全フローに効かせる場所が無い
- **submit 時に何も言われない。** Pod は起動し、症状は「トークンが読めない」「DNS が引けない」に化ける

コメントにも書き、メモリにも書き、別セッションにも警告した上で、**同じ日に 3 回踏んだ。**
注意力の問題として扱う限り再発する。**忘れられる場所に置いてあることが問題**で、
これは設計で消せる種類のものである。

### 設計への帰結

**この節の最初の版では「Pod の身元はコントローラが付与すべき」と書いた。これは誤り**で、
§2 の責務表（handler の作者 = 権限・**Pod の形**・何を実行するか / コントローラ = 遷移・回収・
同一性・上限・掃除）に正面から反する。CNP に何と書くかはクラスタ側の問題であって、
framework が知ってよいことではない。§2 が冒頭で「設計中に何度も『これはコントローラの仕事か』を
取り違えた」と書いているのと同じ取り違えを繰り返した。

正しくはこうなる。CRD の値打ちは表現力でも所有権の移動でもなく、**宣言を受ける口の数**である。

| | 今 | 直すべきは |
|---|---|---|
| Pod の形（ラベル・SA） | 使う側のもの。だが **template ごとに、意味の違う 2 箇所へ**書かされる | 内容は使う側のまま。**1 箇所で受けて全タスクに適用する** |
| 実行機構（entrypoint 解決・executor の RBAC） | 使う側が知っていないと書けない | runner の内部に閉じる（ここは提供側の仕事） |

前者は「CronJob の `jobTemplate` の部分だけ使いたい」と同じ形をしている。
**提供側は Pod の中身に一切関与せず、受け取って全タスクに適用するだけ。**
使う側は 1 回書く。書く場所が 1 つなら、2 箇所のうち片方を忘れることが起きない。

§4「namespace を権限の階層にする」との関係も整理しておく。階層が決めるのは
**どの SA がどの flow を起動してよいか**（素の RBAC）であって、**Pod に何を書くかではない**。
その 2 つを混同したのが最初の版の誤りだった。

### 記録: 装置が製品に化けた

同じ日に、実証実験の装置だったはずの Argo 側を**製品として整備していた** —
verify フェーズを常設の WorkflowTemplate として作り、保守性のために ConfigMap へ切り出し、
その ConfigMap を検査する CI を新設した。**プローブをリファクタする者はいない。**
その間 `design.md` は一行も動いていない。

実験は「問いに答えて装置を捨てる」もので、成果の証はこの文書に事実が増えること。
**出力先を使わない実験は、誰も決めないまま製品になる。**
本節はその出力先を使い直したものであり、同時に再発の記録でもある。
