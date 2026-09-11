# ADR-0009 終端の後に 1 回だけ走る `finally`。終端は変えない

- **status**: accepted（2026-09-11、人間の承認）
- **根拠**: 終端の後に何かを走らせる経路が無い。`Escalated` / `Failed` は framework の終端で
  handler を束縛できず、時間切れや NoAnswer で止まった Task の後に片付けも報告も走らない。
  Task の外で作った物（PR のコメント、ブランチ、投稿）を消せるのは task-uid と workspace が
  生きている間だけで、別 Task で追いかける形では workspace に触れない。先行調査 2026-09-11
  （Tekton `finally` / Argo `onExit` / GitHub Actions `always()` / Concourse `ensure` /
  Kubernetes `metadata.finalizers` の一次資料。レポートと実測リストはリポジトリ外の調査記録）

**決定**:

1. **`spec.finally` を持つ。1 つ、条件無し、`next` 無し。** handler と、片付いたことを表す
   ディレクトリ（`done`）を宣言する。`bindings` の外に置くので、到達性や終端の検査には関わらない

   ```yaml
   spec:
     finally:
       handler: cleanup
       done: ok
   ```

   **ディレクトリは 1 つだけ。** 「非空がちょうど 1 つ」の語彙は遷移先を選ぶためのもので、finally には
   遷移が無い。framework が知りたいのは「片付いたと言ったか」の 1 ビットで、書けば done、書かなければ
   失敗。「片付けられなかった」を書いて表明する 2 つ目のディレクトリは、history の reason 1 行を
   買うだけで、片付いていない状態を 2 通りに分けることになる。片付けの失敗理由は finally の Pod の
   ログにある（Job に TTL は付けないので、Task が消えるまで残り、`task-uid` ラベルで引ける）。
   ディレクトリで答えるのは handler が script か LLM かを framework が知らない（P7）からで、
   契約を「finally は exit code」にすると verdict の機構が 2 本になる

2. **終端が確定してから走り、終端を変えない。** `status.phase`、`terminals` の severity、Event、
   metric の severity ラベル、`Ready` の reason は、終端に着いた時点のまま封印される。
   finally の verdict は遷移しない。「仕事の結論」と「片付けの結論」は別の場所に書く
3. **finally は run である。** runID を 1 つ消費し、`history[]` に予約名 `Finally` の 1 行として
   `directory` / `outcome` / `reason` を残す。prepare / publish サイドカーは他の run と同じに乗り、
   workspace の run ビューと棚も同じ規則（[ADR-0003](0003-run-view-and-sweep.md)）。`Finally` は
   `bindings` のキーにも `next` の行き先にも使えない

   `RunRef` の doc は 1 文に不変条件を 2 つ持つ。**両方をここで改める**: (1)「止まった Task は
   currentRun を持たない」— **終端に着いた Task は、finally の run をちょうど 1 つだけ持ちうる**。
   (2)「currentRun は常に task が今いる phase を指す」（`currentRun.Phase == status.Phase`。
   `internal/taskstate/taskstate_test.go` の `TestCurrentRunNamesTheCurrentPhase` が固定）— finally
   走行中は `status.phase` が終端に着いたときのまま封印される一方（決定 2）、`currentRun.phase` は
   予約名 `Finally` を指し、両者は一致しない。finally が走っている Task は「止まっている」のでは
   なく、「終端は確定していて、片付けが走っている」状態である。Reconcile は「finally 中」を
   **`currentRun.phase == Finally` で見分ける**（束縛の有無や `status.phase` では見分けない）。
   束縛の無い phase なのに currentRun が `Finally` 以外を指していれば、従来どおり構造破損として
   `Failed` に落とす。finally の handler は `bindings` からではなく `spec.finally` から解決する

   `taskstate` パッケージ doc が (2) の一致を根拠に挙げる「`transition.Next` の "phase has no
   binding" ガードが本番で到達しない理由」は、finally の run が遷移表を通らない（次の段落）ので、
   finally が走っても引き続き成り立つ

   起動と決着は `bindings` 経由の遷移を通らない、独立した経路を持つ。Task を「止まっている」と
   みなして早期 return する分岐は 2 つある（予約 phase の分岐、束縛の無い phase の分岐）。**どちらも**、
   抜ける前に `currentRun.phase == Finally` を見る。finally の Job は `spec.finally` から組む
   （`bindings` を引かない）。finally の run の決着は **`transition.Next` も `Advance` も通らない** —
   `history[]` への 1 行追記、失敗時の `Ready=False` / Warning Event / finally 専用の metric、
   `expiresAt` の焼き付け（決定 5）だけを行う。verdict は `done` の 1 値で、行き先 phase は存在しない。
   finally の verdict は棚（`results/<runID>/`）に置かない — 読む後続 run が無い
4. **finally の失敗は隠さない。** 片付いたと言わなかった run（NoAnswer / インフラ再試行の
   使い切り / handler が解決できない）は `Ready=False`（reason `FinallyFailed`）、Warning Event、
   finally 専用の metric で声を出し、TTL は `ttl.failed` を取る。仕事の結論を表す値はどれも動かさない
5. **TTL は一度しか焼かない。** `status.expiresAt` は **`Expire` の規則を変えない** — 焼くのは 1 回きりで、
   焼いた日付は動かさない。flow が `finally` を持つ Task では、終端到達時（`Advance` / `Fail` の
   どちらの経路でも）には**焼かず**、finally の run が決着した瞬間に焼く。走っている finally の
   足元から Task を消さない。この機能より前に終端へ着いた Task の backfill は、従来どおり終端到達で
   焼き、finally は走らせない（決定 7 の表と同じ）
6. **finally が受け取るもの**は、終端の意味（`Success` / `Failure` / `Escalated` / `Failed` /
   `Undeclared`）、終端のフェーズ名、終端に着いた run の outcome。他の run と同じ経路（環境変数）で
   値として差し込む。分岐は書かせない（P9）。**run が一度も決着せずに `Failed` に着いた場合**
   （flow の破損が run の開始前・開始不能で判明した場合）、outcome は**空で渡す**。無い値を推測で
   埋めない（P8）
7. **走らない場面を最初から列挙する**（後から 1 つずつ足さない）:

   | 場面 | 扱い |
   |---|---|
   | `Escalated` に着いた（NoAnswer / Declined / BudgetExhausted / インフラ再試行の使い切り、`next` で宣言された辺のどれでも） | **走る**。終端の意味 5 値のうち finally の一番の動機 |
   | flow 宣言終端（`terminals` の `Success` / `Failure` / `Undeclared`）に着いた | **走る**。終端の意味は変えない（決定 2） |
   | Task が削除された（TTL 前の手動削除、走行中の削除） | 走らない。ownerReference で Job も消える。外に残った物は sweep の仕事（§10） |
   | flow が読めずに `Failed` に着いた | 走らない。finally を読む先が無い |
   | 終端に着いた後で flow に `finally` が足された | 走らない。着き済みの Task は flow の編集を受けない（`expiresAt` の backfill と同じ） |
   | finally の handler が解決できない（不在、template が壊れている） | 走れない。決定 4 の失敗として記録。`status.phase` は `Failed` に**しない** |
   | finally の handler の timeout | NoAnswer として決定 4 |
   | flow 自体が壊れて `Failed` に着いた。run は決着していた（`Structural`） | **走る**。flow は読めるので finally は解決できる。outcome は決着した run のもの |
   | flow 自体が壊れて `Failed` に着いた。run は一度も決着していない（`fail()` 経由 — start 未束縛、走行中の run の束縛消失、`ensureJob` / `ensureWorkspacePVC` の brokenFlow） | **走る**。flow は読めるので finally は解決できる。outcome は空で渡す（決定 6） |

8. **削除フックは採らない。** `metadata.finalizers` は「消す前に許可を待つ鍵」で、起動条件が DELETE
   だけ、期限も順序も無く、結果を書く先のオブジェクトごと消える。§10 の「finalizer は best-effort +
   デッドライン」は K8s の外の掃除の話で、この決定とは別。混ぜない。名前を `finally` にするのも
   その衝突を避けるため

**なぜ**:

**終端を封印するのは、先行者全員が「区別できない」ことで困っているから。** Tekton / Argo / GitHub
Actions / Concourse は全員 finally の失敗を全体の失敗に畳む。理由は結果が 1 値（reason / phase /
conclusion）しか無いからで、畳んだ結果、DAG 成功 + finally 失敗と DAG 失敗 + finally 失敗が同じ
`Failed` になり（Tekton）、main が失敗すると finally の結果が phase からも message からも消え
（Argo）、`outcome` と `conclusion` の 2 層を後から分ける羽目になった（GitHub Actions）。
この framework は終端の意味 5 値と `history[]` と Conditions を別々に持っているので、畳む必要が無い。
畳まないが隠しもしない — 決定 4 は「声を出す」側だけ最悪値に寄せる。cleanup の失敗を 1h で消すと
誰も気づかない

**finally に「なぜ走ったか」を最初から渡すのは、渡さなかった先行者が別のフックを足したから。**
Concourse の `ensure` は状態を受け取らず、`on_success` / `on_failure` / `on_error` / `on_abort` の
4 本が別に要った。Tekton は status を渡したが `onError: continue` で `Failed` が曖昧になり
`.reason` を後から足した。決定 6 は終端の意味と outcome の両方を最初から渡す

**走らない場面を列挙するのは、Tekton が「常に走る」を一枚岩で出して例外を 6 本の TEP で
後付けしたから**（cancel 3 値、timeout 3 種、結果欠落、`when`、検証失敗。`timeouts.tasks` の穴は
2 年 10 か月開いていた）。この framework には利用者の停止操作も flow 全体の timeout も無く、
finally は 1 つなので、列挙は表 1 枚で済む

**1 つ・条件無しは P9 の帰結。** 「失敗のときだけ走る finally」は `when` であり、Tekton の TEP-0045 は
それを足すときに「finally の契約を壊す」と自認している。分岐が要るなら flow の `next` で終端を分け、
finally の中で終端の意味を読んで振る舞いを変える（それは handler の話で、framework の話ではない）

**覆したもの**: `api/v1alpha1/task_types.go` の `RunRef` doc が 1 文に持つ不変条件を両方（決定 3）—
「currentRun は常に task が今いる phase を指す」（`internal/taskstate/taskstate_test.go` の
`TestCurrentRunNamesTheCurrentPhase` が固定）と「止まった Task は currentRun を持たない」。
`internal/taskstate` パッケージ doc が後者を根拠に挙げる「`transition.Next` の "phase has no binding"
ガードが本番で到達しない理由」は、finally の run が遷移表を通らない（決定 3）ので引き続き成り立つ。
§5 の「framework が持つ名前は 2 つだけ」は、`status.phase` に現れる名前としては 2 つのまま。
`Finally` は `history[]` と `currentRun` にだけ現れる記録用の予約名

**覆すには**: finally の失敗が仕事の結論を変えるべきだと実測で示されたとき（決定 2）。
あるいは終端ごとに別の finally が要ると分かったとき（決定 1 の「1 つ」）— ただしそれは `next` で
終端を分けて finally の中で読み分ける形で足りないことの実証が先

**やらなかったこと**:

- **`metadata.finalizers` で削除時に走らせる**（決定 8）。Flux / cluster-api / Argo Workflows は
  finalizer に期限と opt-out を自前で足して固着を避けている。ここでは削除時の後始末を持たず、sweep に任せる
- **finally を複数**。順序・finally 間の参照・集約の問題が全部消える
- **finally に `when`**。P9
- **finally 失敗で `status.phase` を `Failed` に**。`Failed` は flow の破損であって片付けの失敗ではない

**未解決**:

- 決定 4 の TTL（`ttl.failed` に倒す）は、severity のラベルを動かさずに `Ready` と TTL だけ動かせると
  実装で確認できることが前提。できなければ Condition と Event だけにして TTL は触らない
