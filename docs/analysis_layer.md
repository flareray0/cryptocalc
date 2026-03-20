# Analysis Layer

このドキュメントは、`H:\cryptocalc` の既存税務計算ロジックに対して **additive / rollback trivial** で追加した分析レイヤーの定義をまとめたものです。

## 方針
- 既存の日本向け税務計算ロジックは変更しない
- 既存 API / UI / export は削除しない
- 追加したのは `app/analysis` と、その保存・表示・export のみ

## 指標定義

### total_equity_jpy
`cash_jpy + Σ(quantity × jpy_price)`

### total_equity_usd
`total_equity_jpy / USDJPY`

### realized_pnl
分析レイヤー上の実現損益です。  
`fees` は別のコスト項目として切り出しています。

### unrealized_pnl
残存在庫の時価評価額から、分析レイヤー上の簿価を引いた値です。

### inventory_revaluation
snapshot では current unrealized と同値、期間別 attribution ではその差分を表示します。

### benchmark_total_equity
年初の保有資産をそのまま hold し、入出金だけ同じように反映した仮想ポートフォリオです。

### edge_vs_benchmark
`actual_total_equity - benchmark_total_equity`

### trading_edge
`edge_vs_benchmark - reward_income`

reward / staking の寄与をざっくり分離して、売買の上乗せ分を見やすくする補助指標です。

## 既知の制約
- `spread_cost`
- `slippage`
- `funding`

これらは元データが十分でない場合 `0` です。

- USD 換算は manual rates または USD-like price が無いと未評価になります。
- benchmark は説明用の passive hold 比較であり、税務・投資判断の確定値ではありません。
