# ADR-0008 成果物の置き場所を status に持たない

- **status**: accepted（2026-09-05、人間の承認）
- **根拠**: issue #21。`status.artifacts` と `status.history[].ref` が API に宣言されたまま、
  誰も書かないフィールドとして 10 日以上本番の CRD に載っていた

**決定**:

1. **`status.artifacts`（`ArtifactsRef`）を消す。** 使用例の status にあった
   `artifacts.store: "s3://..."` は API から落とす
2. **`status.history[].ref` を消す。** 併せて `taskstate.Advance` の `ref` 引数も消す
   （唯一の呼び出し元が `""` を渡していた）
3. **history に残るのは「何を決めたか」だけ** — `phase` / `runID` / `directory` /
   `outcome` / `reason` / `finishedAt`。どこに何が置かれたかは framework の知識ではない

**なぜ**:

**コントローラは store を知らないし、知る道もない。** §9 が「store の実装は利用側の pod spec 側で、
コントローラの機能ではない」と決めており、§6「2 ホップ」でコントローラは workspace にも store にも
触れない。これは §2 の責務表がそう配置した結果であって、配置側が egress をどう
絞っているかには依存しない — 依存させると、決定の向きが配置側の設定次第で決まってしまう。
S3 の URL を status に書くには、コントローラが store の設定を受け取る必要があり、それは §2 の責務表が
利用側に置いたものを提供側へ引き込むことになる（この取り違えは既に 5 回繰り返している）。

**status はコントローラが書くもの**なので、利用側しか埋められない値をそこに置くのは、埋まらないことが
最初から決まっているフィールドを API に約束させることに等しい。実際 10 日間、本番の 2 タスクとも
`artifacts: null` / `ref: ""` だった。

**代わりにどう辿るか**: 成果物の場所は利用側が決めた規則で決まる（task の uid、runID、
handler が書いた先）。辿りたい側は `metadata.uid` と `status.history[].runID` から自分の規則で
組み立てられる。framework が知らないものを framework 経由で配らない。

**覆したもの**: 使用例の status（`artifacts.store` と `history[].ref` の 2 行）。
どちらも実装されないまま例だけが残っていた

**覆すには**: コントローラが store を知るべき理由が出たとき。ただしそれは §2 の責務表を
書き換える話で、このフィールドを戻すだけでは済まない

**やらなかったこと**: `status` に「利用側が書くための空き地」を用意する案は採らない。status の
subresource は書き手を分けられず、利用側が書けるようにすると controller の更新と競合する。
利用側が持ちたい情報は利用側のオブジェクト（Task の annotation、あるいは別の記録先）に置けばよい
