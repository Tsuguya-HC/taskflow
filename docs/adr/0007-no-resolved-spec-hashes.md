# ADR-0007 解決済み spec のハッシュを持たない。走行中の run は Job の immutability が守る

- **status**: accepted（2026-09-05、人間の承認）
- **根拠**: issue #101。PR #100 のレビューで、design.md が 2 箇所で参照している
  `handlerHash` / `flowHash` に実装が 1 行も無いことが分かり、実装するか記述を落とすかを決めた

**決定**:

1. **`status.currentRun` に解決済み spec のハッシュを置かない。** §4 の `flowHash` と
   §5 の `handlerHash` の記述を落とす。実行中の `TaskHandler` が**編集**されたことは検出しない
2. **走行中の run は既に守られている。** `ensureJob` は既存の Job があればそのまま返し、handler を
   読むのは Job を新規作成する時だけ。Job の `spec.template` は作成後 immutable なので、
   スナップショットは Kubernetes 側が取っている。ハッシュが守るはずだったものは、もう守られている
3. **編集が効くのは次の run から。それは変わってよい。** ハッシュが実際に変える挙動は、次フェーズや
   差し戻しが新しい handler で走るのを `Failed` に倒すこと。それは GitOps でロールアウトした定義が
   次から使われるという普通の挙動で、止める理由がない
4. **削除は引き続き検出する**（`handlerFor` → `brokenFlow` → `Failed`）。実在しない handler では
   配管が定義できないので、これは提供側の関心
5. **flow の矛盾は遷移の瞬間に構造として捕まえる。** flow は毎 reconcile で読み直し、
   `transition.Next` が「1 つのディレクトリが 2 ステータスを指す」「`Failed` を行き先に宣言している」を
   その場で `Failed` にする。ハッシュより安く、編集された flow に対して正しく倒れる。handler が
   書いたディレクトリが新しい `next` に無い場合は `NoAnswer` → `Escalated` で人に渡る（fail-closed）

**覆したもの**: design.md §5「handler の変更検知は `status.currentRun.handlerHash` に解決済み spec の
ハッシュを置く。不一致なら即 `Failed`。実行中のタスクの足元で定義が変わったなら、それはもう別のタスク」
と、§4 の `flowHash`。筋は通っていたが、**対価が実測で見合わない**。このクラスタでは Renovate が
agent イメージの digest を上げ、Kyverno の mutateDigest が焼き直すのが日常なので、ハッシュを持つと
その都度走行中のタスクが `Failed` になる。P8 が禁じているのは「矛盾したまま進むこと」であって、
「上流が正しくロールアウトした定義を受け取ること」ではない。

先行例も揃って逆側だった — Argo Workflows は `workflowTemplateRef` を `status.storedWorkflowSpec` に
スナップショットして走り続け、Tekton の TaskRun は `status.taskSpec` に固め、CronJob は Job 作成時に
jobTemplate をコピーする。**ハッシュを持って不一致で落とす**設計は見当たらない。

**覆すには**: Job を作らない runner（`External`）が入り、実行の実体が Kubernetes のスナップショットに
守られなくなったとき。その場合も先に問うべきは「ハッシュか」ではなく「その runner が何をスナップショット
できるか」

**未解決**: 1 つの Task が複数フェーズにまたがる間に handler が更新されると、フェーズごとに違う版で
走ることになる。今は「それでよい」としているが、フェーズ間の引き継ぎ形式（ADR-0005 の
`/workspace/<dir>/report.md`）が版をまたいで変わった場合に何が起きるかは実測していない
