# ADR-0013 並列フェーズは `join` で宣言し、分岐から合流までを閉じた領域にする

- **status**: accepted（2026-09-25、人間の承認）
- **根拠**: issue #159。互いに依存しないフェーズ（観点ごとのレビュー等）が直列にしか並べられず、1 本ごとに
  Pod の起動と前処理の時間が積み上がる。前提として循環の上限をフェーズごとの実行回数に変えた
  （[ADR-0012](0012-run-limit-per-phase.md)）— 並列の枝が 1 本の予算を奪い合わないようにするため

**決定**:

1. **分岐元の binding に `join` を書くと、それが並列の宣言になる。** フラグと合流先を別々に持たせると
   「フラグはあるが合流先が無い」が書けるので、1 つのフィールドにする。合流先は推測しない（`start` と同じ、P8）

   ```yaml
   観点出し:
     handler: pick-perspectives
     next: {security: security, logic: logic, Escalated: stuck}
     join:
       phase: 仕分け
       always: [tests]
   security: {handler: review-security, next: {仕分け: done, Escalated: stuck}}
   logic:    {handler: review-logic,    next: {仕分け: done, Escalated: stuck}}
   tests:    {handler: review-tests,    next: {仕分け: done, Escalated: stuck}}
   ```

   - 分岐元の run が非空にしたディレクトリの行き先を**全部**起動する。`join` を持たない binding は
     今までどおり「非空がちょうど 1 つ」
   - **起動する枝 = 書かれた行き先 ∪ `always`。** 「テストは必ず見る」はプロンプトに保証させられない
     （design.md §2 の実測: 指示は守るが保証はしない）ので設定に置く。これは flow のトポロジであって
     flow 作者の持ち物
   - **非空が 0 個なら今と同じく `Escalated`（`NoAnswer`）。** `always` があっても黙った run を進ませない（P6）
   - `Escalated` への宣言された辺と、枝の行き先を同時に書いたら、答えが 1 つに定まらないので `NoAnswer`
     （直列の「2 つ書いたら答え無し」と同じ）。`Escalated` の辺だけに書いたときは今と同じく `Declined`
   - 並列の上限と候補は flow に固定され、実行時に決まるのは**どれを選んだか**だけ。表を引くだけで式は無い
     （P9）。#159 の「N は定義に固定」は、N を上限として固定し部分集合を選ぶ形で満たす

2. **分岐から合流までを、入口 1 つ・出口 1 つの閉じた領域にする（一般形）。** admission（webhook）で検査する

   | # | 規則 | 防ぐもの |
   |---|---|---|
   | S1 | 枝の入口 = 分岐元の `next` の行き先（`Escalated` を除く）∪ `always`。`always` と `next` の行き先は重ならない。入口はどれも束縛のあるフェーズ | 選ばれ方が 2 通りある枝、走る者のいない枝 |
   | S2 | 枝の領域 = 入口から `join.phase` を通らずに到達できるフェーズ。枝同士の領域は互いに素 | 2 つの枝が同じフェーズを同時に走らせる |
   | S3 | 領域から出る辺は、自分の枝の領域の中・`join.phase`・`Escalated` だけ | 枝が勝手に終端や他の枝へ行く |
   | S4 | 領域に入る辺は分岐元からだけ。領域から分岐元へ戻る辺も無い | どの合流を待つのか決まらない run |
   | S5 | 枝の中の分岐は、その `join.phase` も同じ枝の領域に置く | 合流の交差 |

3. **v1 の制限。どれも後から外して既存の flow を壊さない。**
   - 枝はフェーズ 1 つ（入口がそのまま `join.phase` と `Escalated` だけへ出る）。鎖・枝の中の循環・入れ子は無い。
     外すときは S2〜S5 の検査を一般形に広げるだけで、1 フェーズの枝はその特殊な場合
   - 枝の handler は `Job` runner だけ。`State` の枝は実行時に `Failed`（TaskFlow の admission は handler を
     見ない — [ADR-0006](0006-taskflow-admission-webhook.md) 決定4）
   - 行き先に `join.phase` 自身を書いて「追加の枝は無し」を表す形は持たない。読みにくく、要るかは使ってから分かる

4. **合流と止め方。**
   - 起動した枝が全部 `join.phase` へ着いたら、合流先の run を 1 つ起動する
   - **どこかの枝が `Escalated` に着いた時点で Task は `Escalated`。** 決定1 の下では残りの枝が何を返しても
     結果は変わらないので、残りの Job はその場で消す（fail-fast）。消した run は history に outcome
     `Cancelled` の 1 行を残す
   - **同じ reconcile で複数の枝が既に決着していることがある。** その時点で決着している（Job が終わっている）
     枝は、枝のフェーズ名の順に全部決着させて history に残す（それぞれ本来の outcome）。まだ走っている枝
     だけを消して `Cancelled` にする。決着した枝のうち 1 つでも `Escalated` に着いていれば Task は
     `Escalated`
   - 回数の上限（ADR-0012）は各フェーズに掛かる。枝は互いに素なフェーズを持つので、判定は枝ごとに閉じて
     終わった順に依存しない。合流先のフェーズは合流 1 回につき 1 回数える
   - **待ち合わせ（起動した枝が全部着くまで待つ）が意味を持つのは、分岐元が起動した枝が走っている間だけ。**
     その間に走っている run は枝だけで、枝から出る辺は S3 で `join.phase` と `Escalated` に縛られるので、
     待ち合わせ中に外から `join.phase` へ着くことは無い。それ以外のときに他の辺から `join.phase` に着くのは
     普通の直列の到達（例: 後段からの rework）で、`inputs` も直列の行（決定6）に従う

5. **番号は枝ごとに、起動した時に払う。** 並列の枝はそれぞれが run で、`results/<runID>/` の棚・
   「subPath 無し = この run」（[ADR-0003](0003-run-view-and-sweep.md)）・prepare / publish はどれも
   今と同じ形で成り立つ。ADR-0003 決定2 が「並列化の日はコントローラのリスト計算だけが変わる」と書いた
   とおり、sweep リストは「生きている run 以外の `work/<id>`」になる（[ADR-0004](0004-run-id-counts-runs-not-attempts.md)
   決定4「sweep リストは変えない（`1..current-1`）」を覆す）
   - 払う順は枝のフェーズ名の順。同じ入力なら同じ番号になる
   - [ADR-0004](0004-run-id-counts-runs-not-attempts.md) 決定1 の「番号 = 決着した run の順序」は
     「**番号 = 起動した run の順序**」に改める。インフラ再試行で番号が動かないのは同じ
   - `Cancelled` の番号の棚は、あるとは限らない。Job を消すと publish は SIGTERM で封印するので、
     handler が書き終えていれば棚に載り、書き終えていなければ載らない。どちらでも history の `Cancelled`
     が正で、棚の中身は遷移に使われない。こうした番号ができるのは Task が `Escalated` に着いたときだけで、
     後に読むのは finally と人間だけ
   - 枝の中の循環（決定3 を外した後）も、次の番号を払うだけで収まる — 棚を「並列全体で 1 つの run」
     （`results/<N>/<枝>/`）にしなかった理由

6. **handler が読む口は `inputs` ビュー。run 番号は handler に見せない。** handler が `flow-workspace` を
   予約 subPath `inputs`（readOnly 必須）でマウントすると、コントローラはそれを「**この run に至った答え**」
   ごとの readOnly マウントに展開する。置き場は `<mountPath>/<その答えを書いたフェーズ名>`、中身は
   `results/<その run>/<書かれたディレクトリ>`

   | この run | `inputs/` に並ぶもの |
   |---|---|
   | 直列の run | `inputs/<前のフェーズ>/` = 前の run が選んだディレクトリ |
   | 選ばれた枝 | `inputs/<分岐元>/` = 分岐元がこの枝を選んだディレクトリ |
   | `always` の枝 | 何も無い（この枝を選んだ答えが無い） |
   | 合流先 | `inputs/<枝のフェーズ>/` が枝の数だけ |
   | `start` の最初の run | 何も無い |
   | finally | `inputs/<終端に着いた run のフェーズ>/`。並列の途中で `Escalated` に着いたときは、`Escalated` に着いた枝の数だけ並ぶ（決着した run が無ければ何も無い） |

   ```sh
   cat /inputs/*/report.md   # 仕分け: 全観点のレポート。番号も前のフェーズ名も知らない
   ```

   - finally が受け取る単一値の env（[ADR-0009](0009-finally-after-the-ending.md) 決定6の
     `FLOW_ENDING_PHASE` / `FLOW_ENDING_OUTCOME`）は「**終端を決めた run** の行」から引く。決定4の
     とおり並列では複数の枝がフェーズ名の順に history へ積まれうるので、`Escalated` に着いたときは
     `Escalated` に着いた枝のうち枝のフェーズ名の順で最初のものが終端を決めた run になる。他の枝は
     `inputs` と history で見える
   - 前のフェーズ名を handler に直書きさせない（ADR-0004 が棚のキーを phase にする案を却下した理由）。
     glob で読めるので、handler は flow を跨いで使い回せる
   - ADR-0004 で「fan-out を実装する日に決める」と先送りした**入力ビュー**をここで決める。symlink は
     使わない — prepare / publish が symlink を拒否する規則とぶつからない
   - キーにするフェーズ名はパス要素として正しくなければならない（`/` `..` を含まない）。
     `contract.CheckDirectoryName` と同じ関数で、**全ての binding のキー**を admission で検査する
   - 棚の `subPath: results`（明示）はそのまま残る。過去の全 run を見たい handler はそちらを使う

7. **status は今動いている run をリストで持つ。** `status.currentRun`（1 つ）を `status.currentRuns`
   （`phase` をキーにしたリスト）に置き換える。直列と finally は要素 1 つ、並列は枝の数。2 つの形を
   持つと「今動いている run はどこか」が 2 か所に分かれる
   - 移行は expand/contract。expand（この版）は両方のフィールドに書き、`currentRuns` を読み、旧
     `currentRun` しか持たない Task（コントローラを戻したときも含め、旧版が書いた Task）は
     `currentRuns` へ引き取る（`taskstate.AdoptLegacyRun`）。旧 `currentRun` は消さず、要素が
     ちょうど 1 つのときはその複製、0 個または並列で 2 個以上のときは `nil` として鏡のように
     書き続ける（`taskstate.SetCurrent`）——旧版はこのフィールドしか読まないので、この版へ上げた
     あとでも旧版へ戻せる。2 つが食い違ったときは旧 `currentRun` を正として `currentRuns` を作り直す
     （並列で要素が 2 個以上のときだけ逆に旧を `nil` へ揃える）——この版は両方を必ず揃えて書くので、
     食い違いが生まれるのは旧フィールドだけを書く誰か（旧版のコントローラ、手作業のパッチ）が後から
     書いたときに限られ、そのときは旧の方が新しい。contract（並列を実際に走らせる版、PR4）で旧
     フィールドを消す。そこがロールバックできなくなる境界であり、この版ではない

**覆したもの**:

- design.md §4「1 フェーズに複数の検査」のスケッチ（束縛に `handlers: [...]` を並べ、答えるのはちょうど 1 つ）。
  並列の枝は、1 フェーズの中の handler ではなく、それぞれがフェーズになる — 別の SA・別のモデル・別の
  回数上限を持てて、history にも 1 行ずつ残る。合成規則「答えるのはちょうど 1 つ」は捨てたのではなく、
  「判断は合流後の直列フェーズがする」に置き換わった
- ADR-0004 の「番号 = 決着した run の順序」（決定1）
- ADR-0004 決定4「sweep リストは変えない（`1..current-1`、自 run は Sweep 自身が拒否したまま）」。
  ADR-0003 決定2 が「並列化の日はコントローラのリスト計算だけが変わる」と予告していた変化がこれに当たる
- ADR-0003 決定1 の「覆すには」に書いた条件（並列 run で「subPath 無し = この run」の一意性が崩れる）は
  **踏まない** — 枝ごとに番号を払うので、各枝の「この run」は 1 つに定まる
- [ADR-0009](0009-finally-after-the-ending.md) 決定6 の「outcome は finally の runID − 1 の行から引く」を
  「終端を決めた run の行から引く」に改める（直列では両者は同じ行を指すので、直列の挙動は変わらない）

**却下した案**:

- **グラフの中で自由に分岐・合流させる**（`next` が複数の行き先を持ち、合流用のフェーズを別に置く）。
  どの合流を待つのかが辺の形から決まらず、`currentRun` が 1 つという前提・訪問の数え方・分岐の途中へ
  戻る rework の意味を作り直すことになる。閉じた領域（決定2）はその制限版で、検査できる範囲に収まる
- **枝の答えを全員一致なら進む / 最も不利な答えが勝つ**。後者は §4 で既に却下（ステータスの優劣は
  framework に分からない）。前者は、枝が行き先を選べると並列のチェックが食い違うたびに `Escalated` になり、
  結局「判断は合流後」に書き直すことになる。枝の行き先は `join.phase` と `Escalated` だけ（S3）
- **並列全体で run 1 つ、棚を `results/<N>/<枝>/` に**。合流先の読み方は glob 1 つで済むが、枝の中の
  循環（2 回目の run の置き場）が表せず、ADR-0003 決定1 のビューの意味も変わる
- **対応表（`{"security": 4}`）を run ディレクトリに置いて handler に番号を引かせる**。handler に jq と
  番号の扱いを要求し、「エージェントは番号を数えない」を実質的に破る。分岐元が枝に渡した中身
  （「ここを見て」）を枝が読む手段も別に要った
- **枝ごとの予算**。ADR-0012 のフェーズごとの回数で要らなくなった

**覆すには**: 枝の数を実行時の値（入力の配列の長さ等）で決めたい場面が出たとき。それは P9 が拒む「実行
しないと正しさが分からない」側で、汎用のワークフローエンジンに任せる境界（design.md §2）を引き直す話になる

**実測（2026-09-25）**: readOnlyRootFilesystem・非 root のコンテナに、同じ RWX の NFS PVC を 3 つの subPath
（うち 1 つは日本語のマウントパス）で、存在しない親ディレクトリの下へ readOnly マウントした。runc と
サンドボックス VM（`runtimeClassName` 指定）のどちらでも、親はランタイムが root 所有・読み取り専用で作り、
中身は読めて、`touch` も親での `mkdir` も `Read-only file system` で拒否された

**未解決**:

- 分岐元の run が書いたディレクトリの中身を、選ばれた枝がどう使うか（観点の指示を `focus.md` で渡す等）は
  利用側の規約で、framework は運ぶだけ。規約が固まるのは利用側の flow（issue #159 の利用側）を回してから
- `Cancelled` の Job の publish が封印に失敗した run の `work/<id>` は残骸になる。次の run が無い
  （Task は `Escalated`）ので sweep されず、TTL まで検死素材として残る（ADR-0003 決定4 と同じ非対称）
