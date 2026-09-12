# ADR-0011 framework が起こさない run には、framework が答えの置き場を開ける

- **status**: accepted（2026-09-12、人間の承認）
- **根拠**: issue #112。`External` は enum に居るが実装が無く（design.md §4）、その説明は
  「人間レビュー」だった。**P7 の下でコントローラは誰が verdict を埋めたかを知り得ない**ので、
  それは型が保証できないことを型の意味として書いていたことになる。#110 が自分で
  「その External が本当に人間かは保証しない」と書いているのがその自白で、
  `taskhandler_types.go` にも `// A human reviewer is this.` が残っていた

**決定**:

1. **`External` を落とし、`State` を入れる。** 主語を「外の実行者」から**外の状態**へ移す。
   意味は「**この run は framework が起こさない。verdict は宣言された状態に現れる**」だけで、
   人間・外部 CI・別のオペレータの区別は入らない。区別が付かないことが P7 そのもので、
   `External` が「人間 / 外部 CI / 任意の外部システムを 1 つの機構に統合する」と言っていたものは、
   人間という語を一度も使わない形にして初めて本当になる

2. **入口はコントローラが run ごとに作る ConfigMap。** `State` の run を始めるとき、Task と同じ
   namespace に、Task を `ownerReference` に持つ ConfigMap を 1 つ作る。中身は空で、答えの語彙は
   注釈に置く:

   ```yaml
   metadata:
     labels:      {app.kubernetes.io/managed-by: taskflow,
                   flow.tgy.io/task-uid: <uid>, flow.tgy.io/run-id: "3"}
     annotations: {flow.tgy.io/phase: <フェーズ>,
                   flow.tgy.io/choices: "ok more"}  # 宣言から生成した語彙
   data: {}
   ```

   答えは `data.verdict` に宣言されたディレクトリ名そのものを書く。照合は
   `collect.FromPods` と同じ `slices.Contains(declared, value)` で、照合表もパーサも新設しない。
   語彙外の値は termination message が語彙外だったときと同じく直接 `Escalated`。
   `data.reason` があれば termination message の 2 行目と同じ扱い（history / 条件 / Event）。
   キーが 1 本なので、ディレクトリ側で必要だった「非空がちょうど 1 つ」に当たる判定が要らない。

   **`prepare` が `/workspace/{ok,more}/` を敷くのと同じ動作**にあたる — 宣言が語彙を作り、
   答える側は並んでいるものから 1 つ選ぶ。答える側が語彙を知るのに flow を読む必要が無い。
   **handler 側の宣言は `runner: {type: State}` だけで、ConfigMap の名前も場所も書かせない。**

   置き場をコントローラが用意するのは workspace の PVC と同じ筋（[ADR-0002](0002-per-task-workspace-pvc.md)）。
   利用側に固定名の ConfigMap を持たせる案は採らない: handler は複数の Task が共有するので、
   **同じ handler を使う 2 つの Task が同じ run 番号に居るとキーが衝突する**。
   Task ごとに名前を分ける手段は、結局コントローラが名前を決めることに帰着する

3. **先回りは `Create` が弾く。** 既に同じ名前の ConfigMap があれば `AlreadyExists` で、それは
   **run が始まる前に誰かが置き場を作っていた**ということなので受理せず `Failed`（P8）。
   run 番号は数えれば分かるので、「番号を宛先に焼く」だけでは先回りを防げない — 先行者の言葉で
   言えば fencing には**古い / 先走った書き込みを能動的に拒否する側**が要り（下の先行調査 6）、
   ここではその拒否が**作成の一意性**という K8s が元から持っている性質に落ちる。
   「外部の結果が先に出ていた」と「ゲートを先回りで通した」はコントローラから区別が付かず、
   区別が付かないまま通す方が悪い（P7 の帰結）。前もって答えたい側は、run が始まってから書く

   作成の一意性だけでは防げない経路がもう一つある: 箱を `delete` してから、偽装した
   `ownerReference`（書き手が自由に書ける任意メタデータで、apiserver は実在する何かとの
   一致を確かめない）を付けて同じ名前で作り直す、という経路は名前の一意性を素通りする。
   実装はここに 2 段目の防御を足した: この run 自身の `Create` が成功した瞬間の object の
   UID（apiserver が発行するもので、ownerReference と違って書き手が選べない）を status に
   焼き、以後の読みでそれと食い違う object を拒否する。詳細は `RunRef.VerdictBoxUID` の doc

4. **値は uncached の `Get`。watch は要求しない。** 待っている run があるときだけ引き、requeue で
   再訪する。RBAC は `configmaps` の **`get` と `create`** だけで済み、`list` / `watch` が
   要らない = informer のキャッシュにクラスタ中の ConfigMap が乗らない（**値の取得と変更検知を
   別経路にし、後者を任意能力として落とせるようにする**のは先行例がある — 下の先行調査 7）。
   削除は `ownerReference` の GC に任せるので `delete` も要らない。
   **答えられる者を framework は絞らない** — その ConfigMap を書ける権限、つまり素の RBAC が決める。
   名前が生成されるので `resourceNames` で 1 個に絞る道は閉じるが、**namespace を権限の階層にする**
   （design.md §4）が既に選んでいる粒度がそれで、object 単位の切り分けはこの設計に無い粒度になる

5. **コントローラが自分で作る物には、決まったラベルだけを付ける。** 対象は Job・PVC・
   verdict の置き場のように**中身まで framework が決めている物**で、

   - `app.kubernetes.io/managed-by: taskflow` — **framework 産である**ことの唯一の目印
   - `flow.tgy.io/task-uid` — どの Task のものか（既存の contract）
   - `flow.tgy.io/run-id` — どの run のものか

   **ラベルと注釈の分かれ目は「値がラベル値として合法か」**で、これは既に contract が
   採っている分け方（UID と数字はラベル、`調査` のようなフェーズ名は注釈）。
   flow が選んだ名前はラベルに置けないので、`phase` と `choices` は注釈のまま。

   これは**利用側が管理できるようにするため**にある。名前が生成される以上 RBAC の
   `resourceNames` では絞れないが、**admission policy はラベルで絞れる**ので、
   「framework 産の verdict 置き場だけ、この主体が書いてよい」は利用側が書ける
   （決定 4 で失った object 単位の粒度が、ポリシー層で戻る。P2 の線はそのまま）。
   一括で見つける・掃除の対象に入れる / 外すも同じラベルで足りる。

   **pod は対象に入れない。** pod が何を身に着けるかはポリシーが選ぶ面で、それは handler の
   持ち物（design.md §4）。framework が勝手に増やすのは、その面に手を入れることになる

6. **`State` では `timeout` を必須にする**（CEL で create 時に拒否）。期限の無い待ちは沈黙と
   区別が付かず、終端に着かない Task は TTL にも metric にも現れない。超過の扱いは既存のまま
   （`NoAnswer` → `Escalated`）。Job の `activeDeadlineSeconds` に乗せられないので、
   期限はコントローラ側の requeue が持つ（TTL で既に使っている経路）

7. **棚は次の run の prepare が `results/<runID>/<value>/` を空で敷く。**
   `State` の run は Pod を作らないので自分では seal できないが、
   **空の宣言ディレクトリは既に正当な verdict の形**（`ok/` は空）なので、新しい表現は要らない。
   これで ADR-0004 の「1 run = 1 sealed directory、番号はフェーズの通り」が保てる。
   コントローラが PVC に触るのではなく、既に workspace をマウントしている prepare が敷く

**先行調査**（2026-09-12、一次資料。原型ごとに 1 件）:

| # | 原型 | 代表 | 経路 | 期限超過 | 「誰が答えたか」 |
|---|---|---|---|---|---|
| 1 | inbox → 実行中オブジェクトの status | Argo Workflows の suspend/resume | `status.nodes[].phase` を外から `Succeeded` に書く | **`Succeeded`（fail-open）** | ラベルに上書き記録。履歴は残らない |
| 2 | inbox → spec と status の両方 | Argo Rollouts の pause/promote | promote が `status.pauseConditions` と `spec.paused` を両方書く | duration 無しは無期限 | 型に無し |
| 3 | 専用 kind を create + webhook | Tekton の manual-approval-gate | `ApprovalTask.spec.approvers[].input` を本人が書き、webhook が `UserInfo` で照合 | 既定 60 分で **reject**（fail-closed） | **型に入れた**（approvers に name/type） |
| 4 | コントローラが外へ pull | Flagger の confirm-* webhook | HTTP POST、判定は 2xx か否かだけ | fail-closed だが待ちに上限が無い | 外部サーバの中。型に無し |
| 5 | **宣言された状態を読む** | Cluster API の machine deletion hooks | 注釈が在る間は止まる。外の誰かが外すと進む | 無期限 | 注釈の**値に owner を書くが制御には使わない** |
| 6 | 世代を宛先に焼く | Temporal / Zeebe | `workflow_id + run_id` / `jobKey + leaseToken` | timer / lock 期限で再配布 | `identity` / `worker` は**監査専用**、制御には不使用 |
| 7 | **宣言された ConfigMap の値を読む** | Argo Workflows の `synchronization`（semaphore） | 毎回ライブ `Get()`。変更検知だけ別 informer | 値が壊れていれば明示エラー、待ちは Pending | 型に無し |

読み取れたことのうち、この ADR を変えたもの・裏書きしたもの:

- **inbox 型は先行者が 6 年払っている。** Argo Workflows の「コントローラ以外が `status` を書く」は
  親 issue #2942（2020 年 open のまま）で "Only the workflow controller should be able to change a
  workflow" と総括され、2026-09-10 の提案 #16741 が `WorkflowAction` CRD へ是正しようとしている
  — **外は intent を作るだけ、対象の status を書くのはコントローラだけ**。決定の向きはそちらと同じ
- **細粒度 RBAC は先行者も作れていない。** #16741 は Non-goal に
  "Kubernetes RBAC cannot distinguish 'may Resume' from 'may Terminate'" と書く。Rollouts も
  `rollouts` / `rollouts/status` の全か無か。**サブリソースで verdict だけを切り出す案（#112 の当初案）は、
  API が許していたとしても先行例が無い**
- **「答えられる者」を型と webhook で作り込むと、その作り込みが攻撃面になる。** Tekton の
  manual-approval-gate は旧リリースで controller の ClusterRole を `system:authenticated` に
  bind しており、認証済みなら誰でも任意 namespace の承認を通せた（docs が "critical security issue"
  と明記、破壊的変更で修正）。**誰が答えたかを型の意味にしない**方針（P7）は、この面を持たない
- **同じ決定空間を明示的に列挙した先行者が 1 件ある。** Cluster API の proposal は
  Status Field / Spec Field / CRDs / Finalizers を却下理由つきで並べ、**オブジェクトに付いた状態を
  読む**形を選んでいる。却下理由は「spec を他のコントローラが動的に書き換えるのは declarative でない」
  「CR を作ると情報の同期が要る」「status は人が直せない」で、**taskflow が別経路で辿り着いた結論と
  同じ向き**。ただし CAPI は「注釈を書ける者」= 対象オブジェクトを編集できる者、で権限を切っており、
  それは対象が管理者の持ち物だから成立している。Task の spec は投入者のものなので、
  **答えの置き場を Task の注釈にはしない**（決定 2 が別オブジェクトを採る理由）
- **無期限に待てる設計は 4 件中 3 件で issue になっている**（Flagger #1450「WaitingPromotion から
  何時間も進まない」、#705「まだ判断中と失敗を区別できない」、Rollouts は duration 無しで無期限）。
  決定 4（`State` では `timeout` 必須）の裏書き
- **期限超過をどちらへ倒すかは割れている。** Argo Workflows は `Succeeded`（fail-open）、
  Tekton は reject（fail-closed）。P6 を持つ側として後者に寄せる（`NoAnswer` → `Escalated`）
- **世代を宛先に焼くだけでは fencing にならない。** Zeebe の `leaseToken` は
  "a command carrying a stale token is rejected" と明記し、Temporal は closed な run への signal を
  `ErrWorkflowCompleted` で拒否する。**能動的な拒否がある**のが要点で、キー名を分けただけの
  Camunda 7 の `workerId`（自称文字列）は弱い側の例。決定 2 の「開始時に既に在れば `Failed`」は
  この拒否にあたる
- **同じ原型を、最も成熟した先行者が inbox 型より優先して採った記録がある。** Argo Workflows の
  #16731（semaphore の limit 0 を承認ゲートに）は、メンテナがまず built-in の suspend を勧めたのに対し、
  提案者が suspend ベースの承認ゲートを実際に作ったうえで "cumbersome and error-prone"（entrypoint の
  差し替え、入力パラメータの forwarding で edge case が多発）と報告し、**ConfigMap の値を読む側が
  "lightweight and robust" として PR #16805 で実装・マージされている**（2026-08-24）。
  **却下ではなく採用**で、しかも同じリポジトリが inbox 型（suspend/resume）を持っている状態での選択
- **値の取得と変更検知は別経路にできる。** 同じ実装で、limit の値は**毎回ライブ `Get()`**（TTL 既定 0）、
  変更検知は**ペイロードを落としたメタデータのみの informer** で、しかも `list` / `watch` 権限が
  無ければ informer を作らずログを出して**機能は落とさず即応性だけ落とす**。決定 3 が
  「`get` だけを要求する」で成立することの実証にあたる。なお向こうが全 ConfigMap を無差別に watch して
  いるのは「semaphore の ConfigMap は label selector で特定できない」という向こう側の事情で、
  そこは真似る理由が無い
- **読めなかったときに黙って待たない。** 同じ実装は ConfigMap 不在 / キー不在 / 値が数値でない を
  明示エラーにし、一時的なら Pending で requeue、恒久的なら Error / Failed に倒す。
  決定 4 はこれと同じ分け方（置き場が無い = 矛盾、答えがまだ無い = 待ち）
- **古い答えが次の試行を通す穴は、先行者では開いたまま運用されている。** Tekton の ApprovalTask は
  CustomRun と同名で再利用され `RetriesStatus` を読まない。Flagger の参照実装は payload の
  `Checksum`（revision 識別子）を受け取りながら判定に使わず、鍵は `name.namespace` だけ

**覆したもの**:

- **#112 の当初案（Task の `verdict` サブリソースへ patch）。** `CustomResourceSubresources` は
  `status` と `scale` の 2 つだけで、任意名のサブリソースを出せるのは aggregated API server だけ
  （apiextensions v1）。`kubectl patch --subresource=verdict` は apiserver に届かず、
  当初案が最大の利点としていた「**verdict だけ書ける RBAC**」も同時に消える
  （`tasks/status` を配ると phase も history も runID も書ける）
- **Task 自身へ書かせる案**（`status` / `spec` / 注釈のいずれでも）。決定 2 は「コントローラが
  run ごとに私書箱を開ける」形なので inbox ではあるが、**書かれる先が Task ではない**。
  Task の `status` を外から書かせると、Argo Workflows が 2020 年から抱えている two-writer problem
  （#2942、"Only the workflow controller should be able to change a workflow"）をそのまま踏む。
  `spec` は投入者のもので、注釈は `patch tasks` を配ることになり spec も書けてしまう。
  Cluster API は注釈を選んでいるが、それは対象（Machine）がもともと管理者の持ち物だから成立している
- **「計画された人間の判断」を framework の語彙として持つ案**（#109 / #110 の前提）。
  P7 の下で見分けられない。#110 の実行時検査「gate のフェーズの handler が `External` でなければ
  `Failed`」は、見分けられるふりをしていた半分なので落とす。残すのは admission のグラフ検査
  （untrusted な始点から `Success` への全経路が、宣言された gate フェーズを通る）だけ

**なぜポーリング Job ではないか**: 外部 API を叩いて結果を待つ handler は「ただの Job」で表せるが、
表した結果が設計の断ったものになる。

1. **長命 runner を正面から入れることになる。** 数時間〜数日 sleep する Job は、§7 の「終了必須」と
   `Sandbox` を採らなかった判断が拒否しているもの
2. **外部システムの資格情報が全 handler に降りてくる。** ポーラは外へ*出て行って*聞くので、
   その認証情報を Task の namespace の Pod が持つ。状態を読む向きは逆で、外側が中へ書く
3. **同じ配管を handler の作者が毎回書き直す。** retry・backoff・「ポーラ自身が落ちた」と
   「まだ答えが無い」の区別。design.md §4 が同じ理屈で init と sidecar をコントローラ側へ吸っている
   （「handler の作者はその存在を知らなくてよい」）

**ADR-0007 との関係**: あちらの「覆すには」が名指ししていた「Job を作らない runner」がこれにあたるが、
**ハッシュは持たない（ADR-0007 のまま）**。`State` の run は走行中に flow / handler が変われば
語彙がその場で入れ替わるが、P8 が禁じているのは「矛盾したまま進むこと」であって
「上流が正しくロールアウトした定義を受け取ること」ではない。スナップショットすべき実行の実体が
そもそも無いので、問いは「何をスナップショットできるか」ではなく「読む時点で宣言が何か」になる。

**覆すには**: ConfigMap 以外の状態を見たくなったとき — 任意 GVK を unstructured で見にいくと、
外部依存が `batch/v1` + core/v1 だけという性質（§14 の public 化の根拠）が崩れるので、
その時に問うべきは「どの GVK を足すか」ではなく「橋渡しを利用側に置けない理由は何か」。
あるいは verdict の書き手に構造検証を掛けたくなったとき（専用 kind なら schema が持てるが、
書き手の幅は狭くなる）

**実装で決めたこと**（2026-09-12、この ADR が実装時に送っていた 3 つ）:

- **待ち中の再確認間隔は 30 秒**（`verdictPoll`）。答えが書かれてから遷移するまでの遅延はこれで決まる。
  読むのは待っている run ごとに 1 回の `Get` だけで、その run 専用の置き場なので絞り込みも
  キャッシュも要らない。**watch を任意能力として足す道**（先行調査 7）は開いたまま、既定では要求しない
- **置き場の名前は Job 名と同じ規則**（`childName`）: `<task>-<runID>-<フェーズのハッシュ>-<UID のハッシュ>-verdict`、
  63 文字上限、切り詰めるときは Task 名の digest を併記する。253 ではなく 63 に合わせたのは、
  切り詰め規則を 2 つ持たないため。**名前は `status.currentRun.verdictBox` にも出る** —
  答える側は命名規則を知らずに宛先を引ける。UID のハッシュは Job 名には無いセグメントで、
  `WorkspacePVCName` と同じ理由（同名 Task の別世代が残した置き場との衝突を避ける）で足した
  — こちらは衝突が `AlreadyExists` を経て恒久 `Failed` に倒れるので、Job 側の理屈は通らない
- **`maxInfraRetries` は CEL で拒否**（`jobTemplate` / `workspace` も同じ理由で一緒に）。
  読まれないのに受理されるフィールドは「設定は書いてあるが効いていない」の形そのもので、
  無視する側に倒すと後から検出手段が無い

**実装が ADR から外れた 1 点**: 決定 2 の例は `flow.tgy.io/choices: "ok more"` だったが、
実装は Pod の語彙（`FLOW_DIRECTORIES`）と同じ **JSON 配列**で書く。ディレクトリ名は自由な文字列で
空白を含みうるので、区切り文字の形式は一部の名前を禁止しないと成立しない。
照合はこの注釈ではなく宣言そのものに対して行うので、これは表示の選択であって語彙の出所ではない

**未解決**:

- **棚**（決定 7）。次の run の `prepare` が `results/<runID>/<value>/` を空で敷く分はまだ無い。
  `prepare` のマウントは `work/` の下なので、`results/` に触れるには配線が要る
