# ADR-0014 Task は開始時に写した定義で最後まで走る

- **status**: accepted（2026-10-02、人間の承認）
- **根拠**: issue #176（承認: https://github.com/Tsuguya-HC/taskflow/issues/176#issuecomment-5950628820）。
  実装は #194 / #197 / #200 / #205 / #210。1 つの Task の run が、その時々の flow と handler の版で
  解釈されていた

**決定**:

1. **Task は、開始時に写した TaskFlow の spec と TaskHandler の spec で最後まで走る。** 新しい定義や
   編集された定義が届くのは、その後に作られた Task だけ。走っている Task に新しい定義を使わせたいなら
   Task を作り直す。走行中の Task の写しを差し替える操作は無い
2. **写すのは** flow の spec、binding が名指すすべての handler、そして **finally の handler（開始の時点で
   在るときだけ）**。後から作られた finally handler は使われず、cleanup の run は「無い」と記録する
   （`FinallyFailed`）。どの binding からも名指されない handler は写さない。status は写さない。
   ConfigMap と Secret から取る値（`configMapKeyRef` / `envFrom` / volume）は固定しない — kubelet が Pod の
   起動ごとに解決する。タグで書いた image も書いたまま写すので、タグは run ごとに引き直される。
   Task の間ずっと同じであるべき値は handler の spec に書く（タグでなく digest）
3. **置き場所は Task ごとに 1 つの ControllerRevision。** Task が owner で、Task と一緒に回収される。
   名前は Task の名前と UID から導き、作った後は誰も書き換えない。status に持つのは condition
   `DefinitionsPinned` だけで、写しへの参照は持たない（[ADR-0008](0008-no-store-pointers-in-status.md)
   と同じ向き）。理由はこの設計の中にある: status の参照は以後の reconcile が信じるしかない指し先で、
   Task から導いた名前は毎回計算し直せて、見つけた物の owner を検査できる。Task の名前の下に、別の UID が
   controller として持つ revision があっても、それは読まない（写しが無いものとして扱う）
4. **開始時の失敗。** 開始フェーズが未束縛、binding が名指す handler が無い、写しが 1 つの object に
   収まらない — このどれでも Task は最初の run の前に `Failed` になる（Ready の reason は `FlowBroken`）。
   収まるかどうかは apiserver の拒否に任せる。コントローラ独自の上限は持たない
5. **開始後。** 止まっていない Task の写しが消えたら `DefinitionsLost` で `Failed` にする。cleanup の run は
   走らせない — 固定した定義が無くなった Task に固定していない定義で片付けを走らせることを、この ADR は
   排するから。写しが作られる前に始まった Task で、止まっていないものは、その時点の定義から 1 度だけ
   写しを作る。**止まった Task** は写しが無くても移行も失敗もさせず、残っている負債（cleanup の run と
   `expiresAt`）のために live の定義を読む。止まったとは、`expiresAt` がある、予約フェーズにいる、
   cleanup の run が走っている、または走行中の run が無く flow が束縛しないフェーズにいる、のどれか
6. **遷移の瞬間の構造検査（[ADR-0007](0007-no-resolved-spec-hashes.md) 決定 5）は残り、写しに対して走る。**
   ハッシュも live の定義との比較も持たない（同決定 1・2 のまま）。ハッシュの比較は正当な更新のたびに
   走行中の Task を落とす。この対価は戻ってこない

**なぜ**:

**主な理由は、§2 の責務表がコントローラに置いた「実行の同一性」。** これまでは 1 つの Task の run が、
framework 自身の宣言の違う版で判定されえた。run に見せる語彙（宣言ディレクトリ、`State` の選択肢）は
dispatch 時の `next` から敷かれ、その run は後の `next` で決着する。`maxRunsPerPhase`、`terminals`、
`finally`、`join` も run の間に変わりえた — run の数え方、終わりの意味、cleanup が走るか、合流が待つ枝が、
それぞれ別の版に従う。遷移が書かれている単位は Task なので、1 つの Task の run は 1 つの定義で解釈する。

**[ADR-0007](0007-no-resolved-spec-hashes.md) の論拠は 1 つの run の中では成り立ち、run をまたぐと成り立たない。**
Job の template が run を固定するという指摘は正しいが、固定されるのは 1 つの run だけ。

**フェーズ間の引き渡しは、P7 の下の一例。** フェーズは [ADR-0005](0005-vocabulary-at-the-mount-root.md) の
ファイルで、handler が選んだ形で受け渡す。2 つの版の handler がその形で合意しているかを framework は
知り得ない。保証できるのは「1 つの Task に 1 つの版」だけで、これが ADR-0007 の未解決を閉じる。

**P5。** 写すのは定義であって、ログ・結果・レポートではない。Task と同数で、その数は TTL が抑える（§10）。
大きさは多くて 1 object で、それを超える写しは開始時に拒否され Task がそこで失敗する。Task と一緒に消える。

**先行例**（公開されたもの）: Argo Workflows の `status.storedWorkflowSpec`、Tekton の TaskRun の
`status.taskSpec`、StatefulSet と DaemonSet の ControllerRevision。

**覆したもの**:

- [ADR-0006](0006-taskflow-admission-webhook.md) 決定 5 の**理由だけ**。「編集済みの flow が走行中の
  タスクの足元に残る」は成り立たなくなった。**決定は残る** — 写しは保存されている flow そのものから
  取られ、それを admission が検査したとは限らないので、実行時の検査は写しに対して走り続ける
- [ADR-0007](0007-no-resolved-spec-hashes.md) 決定 3（編集が効くのは次の run から）: 効くのは作り直した
  Task から。決定 4（削除は run の開始時に検出する）: 検出は Task の開始時に移り、写しの削除が加わる。
  決定 5（flow を毎 reconcile で読み直す）: 構造検査は残るが写しに対して走り、編集で「`next` から
  ディレクトリが消える」場合は起こらなくなる。そして**未解決**（版をまたぐ引き渡し）: この ADR が閉じる。
  決定 1・2 は残る
- [ADR-0009](0009-finally-after-the-ending.md) 決定 6 の「3 値は dispatch 時点の flow から読む」: 写しから
  読む。決定 7 の表の 3 行:
  - 「終端に着いた後で flow に `finally` が足された」: 結果は変わらない。写しを持つ Task には、開始の
    瞬間から当てはまる
  - 「finally の handler が解決できない」: 「無い」の判定が開始時に移る。開始時に無ければ、後で作られても
    無いまま。開始後に消されても写しから走る
  - 「flow が壊れて `Failed` に着いた。run は一度も決着していない」: 写しを持つ Task では、走行中の run の
    束縛が消えることは起こらなくなる。開始時の失敗と移行の失敗がこの行に加わり、cleanup は走る — 写しが
    無いので live の定義から
  - 表への追加: **写しが消えて `DefinitionsLost` で `Failed` になった Task は、cleanup が走らない**
- [ADR-0011](0011-verdict-from-declared-state.md)「ADR-0007 との関係」: `State` の run の語彙は走行中に入れ
  替わらない。写しから読む。「覆すには」の外部依存の列挙（`batch/v1` + core/v1 だけ）: ControllerRevision は
  組み込みの API で、あの列挙が守ろうとしていた性質 — 組み込みの API だけを使い、サードパーティに依存しない —
  は変わらない。列挙は元から網羅していなかった（leader election、metrics の保護、webhook の設定）ので、
  補完はしない

**覆すには**: Task を作り直すことで失うもの（workspace の中身、history）が実害になり、走行中の Task に
新しい定義を取らせるしかないと分かったとき。あるいは 1 つの object に定義が収まらないとき。どちらも先に
問うのは「何を差し替えたいのか」で、差し替えを足す前に、定義のどの部分が run をまたいで変わってよいかを
言えるかどうか

**やらなかったこと**:

- **写しを Task の status に持つ**: status は遷移のたびに書き直され、P5 の対象になる。写しは作ったら
  書かないものなので、書き直される場所に置く理由が無い
- **ハッシュ名で Task 間に写しを共有する（StatefulSet 式）**: 共有された object には 1 つの owner が
  無い。回収に ownerReference の更新とそのための RBAC が要る。Task ごとの写しは、作るだけで、唯一の
  owner が回収する
- **走行中の Task の写しを差し替える操作**: この ADR が消した曖昧さ（この run はどの版で判定されるのか）が
  そのまま戻る
- **ConfigMap と Secret の値を固定する**: Pod の spec の意味を解き、Secret を読むことをコントローラに
  求めることになる。§2 と P2 が外に置いたもの
