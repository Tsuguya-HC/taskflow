# ADR-0015 終わった Task は S3 互換ストレージへ送ってから消す

- **status**: accepted（2026-10-15、人間の承認）
- **根拠**: issue #221。Task は TTL で消え、spec・status・history と棚（`results/`）を読む道が無くなる。
  検死も監査も、Task が存在している間（succeeded 1h / failed 168h）に終わらせなければならない
- **覆す**: [ADR-0008](0008-no-store-pointers-in-status.md) 決定 1 の「store を status に持たない」を `status.archive` に限って

**決定**:

1. **送るもの。** Task の JSON 1 つ（`managedFields` を除く）。flow に workspace の claim があれば、
   PVC の `results/` 以下も送る。`work/` は送らない。workspace が emptyDir の flow は JSON だけ
2. **時機。** `status.expiresAt` が焼かれた後に、コントローラが Task 所有の送り役 Job を作る。
   焼かれるのは、`finally` の無い flow では終端に着いた時、ある flow では finally が決着した時
   （ADR-0009 決定 5）。`expiresAt` は動かさない。**削除するのは「期限を過ぎた」と「送信が決着した
   （成功でも失敗でも）」の両方がそろったとき。** Task が手動で削除された場合は送らない
3. **場所は配置側の設定で渡す。** endpoint・bucket・prefix・region・Secret 名をコントローラの flag で
   受ける。flow の定義には何も書かせない。キーは `<prefix>/<namespace>/<flow>/<Task UID>/`。
   flow が解決できない Task は flow の位置に `_unresolved` を使う。
   TLS を持たない平文 HTTP の endpoint も選べる。その配置では、送り役が本文を署名せずに PUT する
   （`DisableContentSha256`）ため、本文の秘匿と改ざん検知は経路側に任せることになる
4. **資格情報。** Task と同じ namespace にある、設定された名前の Secret を、送り役 Job の env にだけ
   渡す。コントローラは Secret を読まない。PUT 以外の権限を前提にしない
5. **秘密値の検査。** 組み込みの固定規則で検査する。規則に当たったファイル、symlink や通常ファイル
   以外のもの、読めないファイルは送らない。送らなかった理由は保存先の `manifest.json` に残す。
   symlink を辿らないのは、送り役の env にある鍵を送らないため
   ただし task.json だけは、コントローラが渡すディレクトリの中に収まる symlink を辿る。ConfigMap の
   ボリュームは各キーを symlink で見せるので、辿らなければ毎回送れない。ディレクトリの外を指すものは
   従来どおり送らず、results の中の symlink は指す先を問わず辿らない。そのため results は
   ConfigMap のボリュームの下に重ねず、別のディレクトリに付ける
6. **失敗時。** 終端・`Ready`・TTL の値は変えない。`Archived` condition、Warning Event、metric で知らせる。
   結果は `status.archive` に持つ
7. **設定が無いとき。** 今と同じ動きにする。Job も condition も metric も出さない
8. **保存期間は bucket 側の lifecycle に任せる。** この保存物は Task を跨いで残す層で、`task-uid` の
   sweep の対象外

**なぜ**:

**ADR-0008 を覆すのは `status.archive` だけ。** 0008 が退けたのは、コントローラが知らない store の場所を
status に約束させることだった。ここでは場所を配置側の設定としてコントローラが受け取り、送るのも
コントローラ所有の Job なので、`status.archive` を埋められるのは書く本人（コントローラ）になる。
埋まらないフィールドを API に約束する、という 0008 の問題は起きない。`status.artifacts` と
`history[].ref`、および handler が書いた先を framework が配らないことは 0008 のまま。

**コントローラは store に触れず、Job が触れる。** 資格情報を持つのは送り役 Job の Pod だけで、
コントローラは Secret の名前を渡すだけ。verdict の伝達（design.md §6）は変わらない。

**[ADR-0002](0002-per-task-workspace-pvc.md) 決定 3 とは両立する。** あれが退けたのは flow の挙動の既定を
コントローラの flag で注入すること（見えない注入が flow の意味を変えた）。送り先は flow の挙動に
影響せず、Task の走り方も遷移も変えない。配置側の設定は flow の既定ではない。

**lifecycle を使えるのは、層が違うから。** design.md §11 が退けたのは Task 存続中の成果物の掃除を
2 本の機構で担うこと。ここは Task が消えた後に残す層で、消す対象を「対応する Task が無い」では
決められない（Task は最初から無い）ので、経過日数で消すしかなく、それは bucket の仕事。

**覆したもの**: ADR-0008 決定 1 の範囲（`status.archive` に限る）。design.md §2 の P5・P7・責務表、
§6「2 ホップ」、§10「S3 lifecycle は使わない」、§11 の「S3 の掃除を bucket lifecycle に任せる」の行

**覆すには**: 送り役 Job を持つ負荷が、送る価値を上回ると示されたとき。あるいは flow ごとに送り先が
要ると分かったとき（決定 3）— ただし送り先は flow の持ち物ではなく、配置側の設定の複数化で足りるかの確認が先

**やらなかったこと**:

- **コントローラが直接 PUT する。** コントローラが資格情報を持ち、egress を要る（P2）
- **flow に送り先を書かせる。** 提供側が配置側の持ち物を引き込む。flow の再利用性も落ちる
- **`work/` も送る。** 作業ツリーは大きく、秘密値が混じる確率も高い。残す価値があるのは引き渡しの記録
- **送信の失敗で削除を止め続ける。** 再試行の無限化。失敗は声を出して決着とみなす
