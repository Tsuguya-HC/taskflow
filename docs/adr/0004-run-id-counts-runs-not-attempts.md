# ADR-0004 runID は「決着した run」を数える。インフラ再試行では動かない

- **status**: accepted（2026-09-03、人間の承認）
- **根拠**: issue #91。「前フェーズの結果を読む手段が無い」の調査から、原因が参照モデルではなく
  番号のセマンティクスにあると分かった

**決定**:

1. **`RetryInfra` は `RunID` を進めない**。runID はタスクが決着させた run の番号であって、
   その run を起動するのに何回かかったかではない。何も決まっていない試行に番号を払うと、
   `results/` の棚に**穴**が空く — そして穴は、そこに穴があると既に知っている者にしか読めない
2. **Job 名は attempt で分ける**。番号だけでは同じ run の 2 回目を区別できなくなり、
   失敗した attempt の Job は残ったまま（コントローラは Job を消さない。Task 自身の削除が
   回収する）なので、`<task>-<runID>-<phasehash>` に、attempt が 0 でないときだけ
   `-r<attempt>` を挟む。初回の綴りは従来どおり
3. **再試行の残骸は `MakeRun` が消す**。番号を再利用すると再試行は同じディレクトリに戻るので、
   prepare は自分の run ディレクトリを作る前に消す。消せなければエラー（fail-closed）で、
   これは Sweep が他の run に対して結んでいるのと同じ取引。ただし**実ディレクトリ以外は消さず
   拒否する** — symlink を `RemoveAll` は黙って外し、そのあと作り直した実ディレクトリで
   `checkRealDir` が通ってしまうので、防御が消える
4. sweep リストは変えない（`1..current-1`、自 run は Sweep 自身が拒否したまま）。
   再試行の残骸はもう sweep リストの仕事ではない
5. **prepare が run に自分の Pod を刻み、publish は照合してから棚へ動かす**。prepare は
   `out/.prepared-by` に自分の Pod UID を書き（`O_EXCL`、既にあれば失敗）、publish は SIGTERM を
   受けたあと**封印より前に**同じファイルを読んで自分の UID と比べる。一致しなければ封印も Move も
   せず失敗を報告する（verdict 無し、非 0 exit）。UID は downward API で注入 sidecar の
   2 コンテナだけに `FLOW_POD_UID` として渡し、handler のコンテナには渡さない。`.prepared-by` は
   予約名で、flow はこの名前を宣言できない（`checkName` が拒否する）

**なぜ**:

`RetryInfra` の元のコメントは番号を消費する理由をこう書いていた:

> It costs a runID, so the retry gets fresh directories rather than whatever the last attempt
> left behind

この「前の attempt が残したもの」は**通常は存在しない**。再試行の条件は
`failure != "" && !collect.Ran(pods)` で、`Ran` は init container の終了も見る。prepare が走って
`work/<runID>` を作っていたなら `Ran` は true になり、その run は再試行されず
「handler の沈黙」として Escalated へ行く（`collect.Ran` の doc がそう言っている）。
番号を消費する根拠として書かれているものが、ほぼ空振りしていた。

残る到達経路は **Pod オブジェクトごと消えた場合**（eviction / node drain）だけで、そこでだけ
`work/<runID>` に残骸がありうる。決定3 はその 1 ケースのためにある。

**得られるもの**は番号が飛ばないこと自体ではなく、棚が読めるようになること。`results/` は
フェーズごとに 1 つずつ、番号どおりに並ぶ。序数と番号が一致するので、`status.history` と棚が
番号で対応し、A → B → C で C が A を読む用途が `results/1` と書けるようになる。
ADR-0002 決定5 の「最大番号 = 直前の封印済み run」は「番号 = 決着した run の順序」に強くなる。

**ADR-0003 のゴーストの評価が反転する**。あちらは NFS の sillyrename を*危険*として書いた
（「消せなければエラー」）。番号を据え置くと、これは run ディレクトリの**排他機構**になる —
ゴーストが出る = 生きている書き手がいる = その runID で再開してはいけない、が fail-closed で
成立する。旧来の番号付けではゾンビが `work/3` を掴んだまま再試行が `work/4` で平然と始まり、
**ゾンビが生きていること自体が誰にも見えなかった**。ADR-0003 決定3 の
「ボリュームが係争中ということで、その上で新しい仕事を始めない」が、ここで初めて完全になる。

ただし排他として働くのは**同じノード**のケースだけ。別ノードのゾンビならサーバが素直に unlink して
削除は成功し、ゾンビは次の I/O で ESTALE を食って死ぬ。どちらでも新しい attempt のディレクトリは
汚れないが、「必ず Escalated で見える」とは言えない。

**決定5 が要るのは、番号を据え置くと生まれる 1 つの穴のため**。Pod オブジェクトは消えたが
プロセスは生きている publish（kubelet との断絶、force delete）は、いずれ自分の SIGTERM を受けて
`work/<runID>` を封印し `results/<runID>` へ rename する。番号が動かない以上、そのとき同じパスで
**次の attempt が作業中**でありうる — ゾンビが持ち去るのは自分の run ではなく、生きている attempt の
書きかけになる。これは要件（番号を消費しない）を取り下げる理由ではなく、要件を守ったまま提供側の
内側で閉じる問題で、prepare / publish の 2 つの間だけで閉じている: 利用側の契約（マウント、
ディレクトリ規則、`results/<runID>` の読み方）はどこも変わらない。

- **同一性は Pod UID で、attempt 番号ではない**。Job コントローラは terminating な Pod を
  置き換えるので、同じ attempt の Pod が 2 つ並ぶことがある（`collect.FromPods` も Job あたり
  複数 Pod を前提にしている）。attempt 番号では同じ attempt の 2 つの Pod を区別できない
- **マークは `out/` の中**。Prepare 後 0555、所有は agent とは別の sidecar uid なので、agent の
  `rm -rf` でも消せない。棚へ運ばれると `results/<runID>/out/.prepared-by` として残り、
  どの Pod がその run を作ったかの記録になる
- **照合は封印より前**。他人の run を封印すると、その run の答えが自分の termination log に
  出てしまう。拒否を報告する側の出口は起動時検証と同じ `reportFailure` で、コントローラは
  「publish が走ったが verdict が無い」= handler の沈黙として扱う
- **照合と rename の間の窓は許容**。ゾンビの照合が通った直後に次の attempt の `MakeRun` が
  ディレクトリを消して作り直し、そこへゾンビの rename が走る幅は残る。その場合は生きている
  attempt の作りかけが棚へ載り、その attempt の publish が自分の照合でマークを見つけられず失敗を
  報告して Escalated になる — 嘘の verdict は出ない。現実的な競合はこれではなく「ゾンビが
  grace period のあいだ SIGTERM を待つ数十秒〜数分のうちに次の attempt が始まる」で、
  そちらは照合で確実に止まる。窓を閉じるには照合と rename を 1 つの原子操作にする必要があり、
  POSIX にその道具は無い

**孤児の窓（ADR-0002 決定5）とは衝突しない**。封印して rename は済んだが termination message を
書く前に publish が死んだ run は、publish が終了しているので `Ran` が true、つまり再試行されない。
`results/<runID>` が既にある状態で同じ番号の再試行が来ることは、Pod ごと消えた場合を除いて
起こらない。その 1 ケースでは `Move` が既存の棚を上書きせず拒否し、run は Escalated で人間に回る
— fail-closed で、嘘はつかない。

**却下した案**:

- **棚のキーを runID から phase にする**（`results/<phase>/<runID>/`）。読む側が前フェーズの名前を
  直書きすることになり、handler が flow に貼り付いて使い回せなくなる。直列の常用ケースが
  rework という稀なケースの分を払う。先行調査でも Argo / Tekton は参照をノード名で持ちつつ
  **物理レイアウトは試行キーのまま**にしており（Argo の artifact repository は例が pod 名を含む）、
  対応表を持つ側があれば物理レイアウトを変える必要は無い
- **コントローラが入力ビューを組む**（`/inputs/` に前フェーズの結果を並べる）。fan-out が
  型に存在しない今は要らない。fan-in のファイル受け渡しは Argo が #934（2018 起票 →
  2021 未解決クローズ）→ #6805（2021 起票、今も open）で 8 年払って払えていない領域で、
  fan-out を実装する日にその設計の一部として決める

**覆したもの**: `RetryInfra` が `RunID` を消費していた旧挙動と、その根拠だったコメント（前 attempt
の残骸を避けるため番号を消費する）——「なぜ」節で見た通りほぼ空振りだった。ADR-0003 が NFS
sillyrename のゴーストを*危険*として書いた評価も反転し、同一ノードでは同一 runID の排他機構として
働く（ADR-0003 決定3 自体は覆さず、むしろ完全になる）。ADR-0002 決定5「最大番号 = 直前の封印済み
run」は「番号 = 決着した run の順序」へ強まる。

**覆すには**: 1 フェーズに複数 handler（design.md §4）が入って、1 つの run に複数の答えが
出るようになったとき。そのとき棚の 1 run 1 ディレクトリが崩れるので、番号の意味も一緒に決め直す。

**未解決**: ゾンビが `work/<runID>` を掴んだ状態で `MakeRun` の削除が ENOTEMPTY で落ち、
infra retry を経て Escalated まで届くか（ADR-0003 の未解決と同じ実測。番号を据え置いたことで
逃げ道が消え、掃除の成否がそのまま再試行の成否になったので、優先度が上がった）。
削除が成功した場合に、同名で作り直したディレクトリへゾンビの書き込みが現れないこと
（kubelet の subPath bind mount が inode に固定されているはず）も、まだ推論であって実測ではない。
決定5 の照合が実際のゾンビ（`kubectl delete pod --force` した publish）で発火し、生きている
attempt の run が棚へ運ばれないことも、単体テストで再現しただけでクラスタでは未実測。
