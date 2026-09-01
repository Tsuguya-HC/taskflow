# ADR-0002 フェーズ間の引き渡しは Task ごとの PVC で、claim は flow が書き、コントローラは設定を持たない

- **status**: accepted（2026-09-01、人間の承認）
- **根拠**: 2026-08-30 の実測（subPathExpr 下で prepare の語彙強制が Kata + NFS でも成立、
  手書き Job）/ 先行調査 2026-09-01（Tekton / Argo Workflows / StatefulSet / CNPG の一次資料。
  レポートと実測リストは `.claude/works/prior-art-20260901-workspace-pvc/report.md`）

**決定**:

1. **store は S3 ではなく PVC。** フェーズ間引き渡しの消費者は同じ Task の後続 Pod だけで、
   認証情報も egress も増やさずに済み、封印のパーミッションモデル（uid 分離・0555）が
   ファイルシステムのまま乗る。S3 は「Task を跨いで残す・人間が見る」層（`artifacts`）の話で別物
2. **`TaskFlow.spec.workspace.volumeClaimTemplate`**（`corev1.PersistentVolumeClaimSpec` 標準型を
   そのまま。**spec のみ** — metadata 込みの丸ごと型は name 事故 / merge key / シリアライズの実害
   3 件が先行者に記録されている）。フェーズ横断で共有するものはフェーズ集合を定義する flow の
   持ち物。Task は起票側が最小 spec で量産するので置かない。handler はフェーズ単位なので置けない。
   **vct の変更は新しい Task からだけ効き、既存 PVC へは伝播しない**（STS が 8 年解けていない
   KEP-4650 の領域。定義/インスタンス分離が最初からくれる性質で、非目標と明記して負わない）
3. **中身の既定は CRD スキーマで焼く**（省略時 `{RWX, 1Gi}` の struct default。admission 時に
   保存オブジェクトへ実体化するので、効いている値が `kubectl get` に見える）。
   `storageClassName` は既定に含めない — クラスタ固有名を CRD に書かない。省略はクラスタの
   default StorageClass へ。**コントローラは CM / flag の既定を持たない**（workflowDefaults が
   YAML に見えない注入で CNP timeout を生んだ実測 2026-08-30 を踏まえる）
4. **コントローラが Task ごとに PVC を 1 つ作る**。名前は **Task UID 由来の決定論**（Task 名だけだと
   作り直し・terminating 残骸と再バインドレースになる — CNPG #10985、Tekton は同じ理由で UID ハッシュ）。
   AlreadyExists は ownerRef の UID 照合で自分のものだけ採用し、他人の同名 PVC は拒否（STS が TODO の
   まま残した未定義動作を最初から定義する）。ownerRef は Task（TTL 削除に同乗。Task 自体が消える
   設計なので、先行者が後付けした「完了時明示削除」を持たない — TTL 期間ぶん storage を握るのは
   受け入れるコストで、調整は flow の ttl で行う）。`task-uid` ラベル。handler は予約 volume 名
   **`flow-workspace`** を `spec.workspace.volume` に書いて乗る。template が同名 volume を
   自前定義したら、注入コンテナ名と同じく拒否。**利用側の前提**: ownerRef の UID は API サーバーが
   検証しない値なので、この AlreadyExists 照合は認可ではなく GC ヒントでしかない。Task を起票する
   SA に PersistentVolumeClaim の create 権限を与えてはならない — 与えれば決定論的な名前を
   先取りし、本物の Task の UID を騙った ownerRef を付けて squatting できる
5. **レイアウトは per-run、走行中は work/、封印後は publish が rename で results/ に移す**:
   `work/<runID>/out/...` が run 中の書き込み先（prepare がここに `out/` を敷く。handler が
   flow-workspace を書き込み可でマウントするコンテナも、生成時に literal `subPath: work/<runID>`
   を焼かれてこの下に閉じる）。publish は seal で verdict を確定させた後、同一ボリューム内の
   `rename(work/<runID> → results/<runID>)` で棚に移す — 別ボリュームを跨がないので原子的。
   **`results/` には封印済みの run だけが並ぶ**、というのが読み側に効く意味論。handler が
   work/ マウントに自分で SubPath / SubPathExpr を書いたら拒否（黙って上書きしない — 他の
   予約フィールドと同じ理由付け）。**publish は flow-workspace モードでは唯一 volume の root を
   書き込み可でマウントする**（rename が work/ と results/ の二つの棚を跨ぐため、片方に pin する
   subPath では届かない）— agent の書き込みは work/<runID> の subPath に閉じたままで、results/
   を書けるのは publish だけ。rename が失敗したら **termination message を書かずに非 0 で exit**
   （verdict 無し → 既存の infra retry 経路に落ちる fail-closed。移動できていないのに verdict だけ
   通ると下流が読めないディレクトリを指すことになる）。**孤児の窓**: rename 成功の直後、
   termination message を書く前に publish が死ぬと、History には記録されない完了 run が
   results/ の棚に残り得る — 「最大番号 = 直前の完了 run」だけを読む下流には実害が薄いので許容し、
   埋めない。過去の run を読むフェーズは同じ volume を readOnly でマウントし、**推奨形は
   `subPath: results`**（リテラル定数。root のまま無指定でマウントすると、マウント先を
   `results` のような名前にした場合に `.../results/results/1/...` と二重に入れ子になる）—
   これでマウント直下の `<runID>/` を自分で開けば、封印済みの run だけが並ぶ。checkWorkspace が
   拒否するのは work/ を指す書き込み可マウントの SubPath / SubPathExpr だけで、readOnly 側に
   コントローラは何も焼かない（何とも衝突しないので拒否する理由が無い）ので、この literal
   subPath は handler が自由に書ける。今まさに走っている run のディレクトリは work/ にいるので
   この読み取り専用ビューには見えない（マウントで「過去だけ」を切り出す手段が要らなくなった、
   rename 方式の副産物）。env / subPathExpr は使わない（run 番号は annotation の配管止まりで、
   エージェントの env には出ない。design.md「配管だけが読む」）。マウント点の 1 階層上に当てる
   （直接当てると kubelet 所有になり prepare の chmod が EPERM — 実測）
6. **RWO も成立する**。run は Task 内で直列なので RWX は必須でない。node ローカル storage では
   volume affinity で後続フェーズが同じノードに固定される — キャッシュ引き継ぎではむしろ利点

**覆したもの**: なし（ADR-0001 が予約した拡張点「注入側にも subPath が要る PVC backend」を
`spec.workspace` の拡張で埋める、その予告どおりの形）。design.md §4 の annotation/env の一般論
（handler が自分の作るコンテナに run 番号を引き込みたい場合の話）はそのまま残るが、flow workspace
の per-run パスにはそもそも適用されない — コントローラが生成時に決定論的に焼くので、利用側が選ぶ
余地は無い。`volumeClaimTemplate` を書かない flow は現状のまま（emptyDir、単一フェーズ）

**覆すには**: Task を跨ぐ引き渡し（flow 連鎖）が必要になったとき。それは PVC の寿命（= Task）を
超えるので `artifacts` / TaskSequence（design.md v2 候補）の層で、この決定の外

**未解決**: 注入経路の literal subPath 下で prepare の語彙強制の再実測（8/30 実測は
subPathExpr + 手書き Job で行ったもので、コントローラが焼く literal subPath 経由ではまだ確認して
いない）/ qnap-nfs 上での rename 封印の実測（注入経路。work/ → results/ の rename が NFS でも
原子的に振る舞うか、まだ手書き Job・注入経路のどちらでも確認していない）/ cnp-check への 報告
フェーズ追加（home-cluster 側）/ `local-storage`（no-provisioner）を使う flow が出たときの PV 整備
（クラスは既にある。動的プロビジョニングが無いので PV を手で切る必要がある）
