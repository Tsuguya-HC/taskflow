# ADR-0005 語彙はマウント直下。`out/` という中間階層は置かない

- **status**: accepted（2026-09-05、人間の承認）
- **根拠**: issue #92。棚を読む側が `results/<runID>/out/ok/report.md` の `out/` を落として
  Escalated を踏んだ（ADR-0002/0003 を読んだ上で書いてなお）。実測 2026-09-05（下記）

**利用側から見た形**（これが先。機構はここから導出した）:

```yaml
volumeMounts:
  - {name: flow-workspace, mountPath: /workspace}                              # この run: /workspace/ok, /workspace/escalate
  - {name: flow-workspace, mountPath: /shelf, subPath: results, readOnly: true} # 棚: /shelf/<runID>/ok/report.md
```

プロンプトに書く 1 行は「`ls /workspace` に出るディレクトリのどれか 1 つに report.md を書く」。
handler が自分で emptyDir を持ち込む経路でも**同じ形**で、書き方は経路で分岐しない。

**決定**:

1. **run ディレクトリそのものが語彙の root**。prepare は `work/<runID>/` を作り、その直下に
   宣言ディレクトリを敷いて `<runID>/` 自体を 0555 に閉じる。`out/` は無い。
   `.prepared-by`（ADR-0004）も run 直下に置く（予約名のまま。`ls` では見えず `ls -a` で見える）
2. **template 持ち込みの volume（emptyDir）も同じレイアウト**。prepare は volume を `subPath: work` で
   マウントして `<runID>/` を自分で作り、handler のマウントは flow-workspace と同じ規則で
   `work/<runID>` に pin される（subPath 無し = この run、readOnly でも）。これで handler から見える
   root は経路によらず「prepare が作って閉じた run ディレクトリ」になる。
   publish は root を readOnly でマウントして `work/<runID>` を封じるだけ（棚が無いので rename も無い）
3. **書き込み可マウントに handler が自分で SubPath / SubPathExpr を書くのは、template volume でも拒否**。
   pin が両経路に及ぶので、拒否の理由（黙って上書きしない）も両経路に及ぶ
4. sidecar のフラグは `--out` = run ディレクトリに一本化。`--run-dir` と `--seal-from` は `--out` と
   常に同じ値だったので消す。残るのは prepare の `--sweep` と publish の `--seal-to` だけ。
   Pod UID の刻印と照合は両経路で行う（emptyDir では照合が偽になる経路は無いが、分岐を持たない
   方が両端の一致を検証しやすい）
5. **閉じた run を棚へ動かすとき、publish は一度 0755 に開けて rename し、棚の上で 0555 に閉じ直す**。
   Linux はディレクトリを別の親へ rename するときそのディレクトリ自身への書き込み権限を要求する
   （`..` を書き換えるため。ユニットテストで EACCES を踏んで判明）。旧レイアウトは 0777 の run 直下に
   0555 の `out/` があったので踏まなかった。0755 は owner（publish = prepare の uid）だけに書けるので
   agent には開かない。開ける・rename・閉じ直すのどれが失敗しても verdict 無しの非 0 exit（fail-closed）。
   閉じ直しだけが失敗したときは棚に開いたまま残さず work/ へ rename で戻して閉じる — 同じ runID が
   次の attempt でも同じ番号を使う（ADR-0004）ので、戻さないと results/ に開いたままの run が居座り、
   次の Move が「既にある」で永久に拒否され続ける

**なぜ落とせるか**: `out/` が要った理由は「prepare が chmod する対象は prepare 自身が作ったもの
でなければならない（kubelet が作った subPath ディレクトリは root 所有で EPERM）」だった。
ADR-0003 で prepare は run ディレクトリを自分で作るようになった（`MakeRun`）ので、閉じる対象は
run ディレクトリ自体でよい。emptyDir 経路も同じ手が使える — init container は main より先に走り、
kubelet は既に存在する subPath ディレクトリを作り直さない。

**実測（2026-09-05、claude-code namespace、runc と kata の両方）**: prepare 役（uid 65532、
`subPath: work`）が `work/1/ok` を作って `work/1` を 0555 に閉じ、handler 役（uid 65533、
`subPath: work/1`）から見ると root が `dr-xr-xr-x 65532`。`mkdir /workspace/evil` は EACCES、
`chmod 777 /workspace` は EPERM、`ok/` への書き込みは成功。kubelet が作った `work` 自体への
chmod は EPERM（従来どおり、prepare はそこを触らない）

**失うもの**: handler が workspace の run 直下を作業領域として使うこと（0555 になる）。作業
領域は `/tmp`（readOnlyRootFilesystem 下で emptyDir を持ち込む必要があり、既存 handler は全部
そうしている）。Escalated の残骸（ADR-0003 決定4）は work/<runID>/ に語彙とマークがそのまま残る

**覆したもの**: ADR-0001 の「`out/` を敷く」、ADR-0002 決定5 の `work/<runID>/out/...`、
ADR-0003 決定2の実装（`MakeRun` の旧 doc コメント）が前提にしていた「run ディレクトリは agent の
scratch 領域で語彙の外」、ADR-0004 決定5 の `out/.prepared-by`。いずれも `out/` を run 直下に
読み替えれば残りはそのまま

**覆すには**: workspace に語彙以外のものを置く必然が出たとき（例: フェーズ間で語彙と別に渡す
大きな成果物）。それは棚の中身の話で、`out/` を戻すより `results/<runID>/` の下に語彙と並ぶ
別名を宣言する方向

**利用側の追従**（home-cluster、同時に切り替える）: handler の `/workspace/out/` → `/workspace/`、
棚の `/shelf/<n>/out/ok/` → `/shelf/<n>/ok/`、verdict-protocol プロンプトの `ls /workspace/out` →
`ls /workspace`。controller / sidecar のイメージと同じ commit に揃えて上げる（kustomize の
手動ゲート）
