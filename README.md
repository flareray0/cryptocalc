# Crypto Tax App

日本居住者向けの暗号資産損益計算補助ソフトです。  
`H:\cryptocalc` を固定作業ルートにした、**ローカル専用の Web UI + API** として実装しています。

## 重要な注意
- 本ソフトは **日本の暗号資産損益計算の補助** を目的としています。
- 本ソフトは **税務申告そのものを自動保証しません**。
- 暗号資産の売却又は使用により生ずる利益は、事業所得等に付随する場合を除き、原則として **雑所得** を前提に整理します。
- **最終的な申告区分・税務判断は利用者または税理士等が行ってください。**
- 不明取引、JPY 未評価取引、対応付け不能な入出庫は **要確認** として残します。

国税庁の参考ページ:
- 暗号資産等に関する税務上の取扱い及び計算書について  
  https://www.nta.go.jp/publication/pamph/shotoku/kakuteishinkokukankei/kasoutuka/index.htm
- 所得の種類と課税方法  
  https://www.nta.go.jp/taxes/shiraberu/shinkoku/tebiki/2025/01/1_03.htm

Binance Spot API の公式ドキュメント:
- Account Endpoints  
  https://developers.binance.com/docs/binance-spot-api-docs/rest-api/account-endpoints

## このソフトでできること
- Binance Japan の CSV / XLSX / 年間取引報告書の read-only import
- 補助 CSV の取込
  - 期首残高
  - 手動補正
  - JPY 補完レート
- 総平均法 / 移動平均法での計算
- 単年計算に加えて、年跨ぎ / 全期間の合算集計
- 分析レイヤーによる総資産 / benchmark / edge の可視化
- 単年分析に加えて、年跨ぎ / 全期間の分析
- Binance 現在残高との照合と、取引履歴ベース再構築残高との差分確認
- 年次サマリ、銘柄別サマリ、監査明細、要確認一覧の表示
- 国税庁転記補助 CSV / Excel の生成
- JSON API によるローカル連携
- Binance 互換 read-only API での接続テスト / 履歴同期
- 保存済み API key / secret の再利用

## このソフトでまだ自動化しないこと
- 最終税務判断の断定
- 外部時価 API の必須化
- 不明取引の自動穴埋め
- 元 CSV の上書き
- 外部公開前提の認証設計

## ディレクトリ構成
```text
H:\cryptocalc\
  ├─ app\
  │  ├─ api\
  │  ├─ calc\
  │  ├─ domain\
  │  ├─ integrations\
  │  ├─ parsers\
  │  ├─ reports\
  │  ├─ services\
  │  ├─ storage\
  │  └─ ui_web\
  ├─ docs\
  ├─ tests\
  ├─ logs\
  ├─ samples\
  ├─ data\
  ├─ exports\
  ├─ README.md
  ├─ requirements.txt
  └─ .gitignore
```

## 対応入力
### 1. Binance Japan CSV / XLSX
現在の parser は、ローカルで確認した Binance Japan エクスポート例の次の列に合わせています。

- `Date(UTC)`
- `Pair`
- `Base Asset`
- `Quote Asset`
- `Type`
- `Price`
- `Amount`
- `Total`
- `Fee`
- `Fee Coin`

加えて、日本語の Binance Japan 履歴 CSV として次も read-only import できます。

- `資産履歴 - 取引履歴`
  - `ユーザーID, 時間, アカウント, 操作, コイン, 変更, 備考`
- `現物 - 取引履歴`
  - `時間, ペア, サイド, 価格, 実行済み, 金額, 手数料`
- `入庫履歴`
  - `時間, コイン, ネットワーク, 金額, 住所, トランザクションID, ステータス`
- `出庫履歴`
  - `時間, コイン, ネットワーク, 金額, 手数料, 住所, トランザクションID, ステータス`
- `法定通貨による入金履歴`
  - `時間, 方法, 入金額, 受取金額, 手数料, ステータス, 取引ID`
- `法定通貨による出金履歴`
  - `時間, 方法, 出金額, 受取金額, 手数料, ステータス, 取引ID`
- `法定通貨による交換履歴`
  - `方法, 金額, 価格, 最終金額, 作成日時, ステータス, 取引ID`

`入庫履歴 / 出庫履歴` が `条件に一致するデータがありません。` の場合は、0 件として安全に読み飛ばします。
`法定通貨による交換履歴` は空ファイルなら 0 件として読み飛ばし、非空の場合は要確認取引として残します。

ローカル検証では、実ファイル `取引履歴のエクスポート-2026-01-13 19_51_25.xlsx` を parser に通して
- `624` 件読込
- `unknown_column_names = []`
- `unknown_tx_types = []`
を確認しています。

### 2. 手動補正 CSV
最低限、次のような列を受け付けます。
- `timestamp_utc`
- `tx_type`
- `asset`
- `quantity`
- `price_per_unit_jpy`
- `gross_amount_jpy`
- `side`
- `note`

### 3. JPY 補完レート CSV
- `timestamp_utc`
- `asset`
- `jpy_rate`
- `source`

### 4. `data` フォルダ一括取込
- `H:\cryptocalc\data\` 配下を再帰走査して、`csv / xlsx / xlsm` をまとめて取り込めます
- おすすめ構成:
  - `data/exchange/` に Binance 履歴
  - `data/manual_adjustments/` に手動補正 CSV
  - `data/manual_rates/` に JPY 補完レート CSV
- 判定ルール:
  - パスやファイル名に `manual_adjustments` / `adjustments` / `補正` を含むものは手動補正
  - パスやファイル名に `manual_rates` / `rates` / `レート` を含むものは JPY 補完レート
  - それ以外は通常の取引履歴として扱います
- `data` 配下の実データは `.gitignore` 対象で、git へは含めません
- 元ファイルは read-only import のままで、上書きしません

## 対応取引種別
- 購入
- 売却
- 暗号資産同士の交換
- 手数料
- 入庫
- 出庫
- 報酬 / ステーキング等
- 調整仕訳
- 期首残高
- 不明種別

不明種別は無視せず、`review_required` として残します。

## 計算方式
### 総平均法
- Binance Japan の年間取引報告書導線では、総平均法を既定推奨にしています。
- 年度単位で、期首残高と当年取得を合算した平均取得単価で売却原価を出します。

### 移動平均法
- 取引時系列順に平均取得単価を更新します。
- 売却時点の平均取得単価で原価を計算します。

### JPY 評価ポリシー
1. ファイル内の JPY 列を優先
2. 手動レート CSV があれば補完
3. それでも不足なら要確認

### 入出庫の扱い
- `TRANSFER_IN / TRANSFER_OUT` は、分析だけでなく税務計算側の在庫数量にも反映します
- ただし、**入庫時の取得原価が無い場合** は `原価不明在庫` として別管理し、要確認警告を出します
- 原価不明在庫を含むまま売却・交換した場合、実現損益は参考値になりやすいので、`期首残高 CSV` や `手動補正 CSV` で補完してください

## UI 画面一覧
- ダッシュボード
- ファイル取込
- API 連携
- 取引一覧
- 計算結果
- 分析
- 要確認
- エクスポート
- 設定

## API エンドポイント一覧
### Import
- `POST /api/v1/import/csv`
- `POST /api/v1/import/manual-adjustments`
- `POST /api/v1/import/manual-rates`
- `POST /api/v1/import/data-folder`
- `GET /api/v1/transactions`
- `GET /api/v1/transactions/review-required`

### Calculation
- `POST /api/v1/calc/run`
- `POST /api/v1/calc/run-window`
- `GET /api/v1/calc/window-latest`

### Reports
- `GET /api/v1/reports/yearly`
- `GET /api/v1/reports/assets`
- `GET /api/v1/reports/audit`
- `GET /api/v1/reports/inventory-timeline`
- `GET /api/v1/reports/nta-export`
- `GET /api/v1/reports/download`

### Analysis
- `POST /api/v1/analysis/run`
- `POST /api/v1/analysis/run-window`
- `GET /api/v1/analysis/latest`
- `GET /api/v1/analysis/window-latest`
- `GET /api/v1/analysis/portfolio-history`
- `GET /api/v1/analysis/asset-quantities`
- `GET /api/v1/analysis/benchmark`
- `GET /api/v1/analysis/pnl-breakdown`
- `GET /api/v1/analysis/edge-report`
- `GET /api/v1/analysis/export`
- `GET /api/v1/analysis/window-export`
- `POST /api/v1/analysis/exchange-balance/refresh`
- `GET /api/v1/analysis/exchange-balance/latest`

### API Integration
- `POST /api/v1/integrations/binance-japan/connect`
- `POST /api/v1/integrations/binance-japan/sync`
- `GET /api/v1/integrations/status`
- `POST /api/v1/integrations/binance-japan/disconnect`

### Settings
- `GET /api/v1/settings`
- `POST /api/v1/settings`

## CSV-only モードと API 連携モード
### CSV-only モード
- 推奨の第一級手段です
- Binance Japan の CSV / XLSX があれば主要機能をオフラインで使えます
- API 制約の影響を受けません

### API 連携モード
- read-only 前提です
- まず接続テストを行い、その後ユーザー指定の symbol ごとに `myTrades` を同期します
- 取れない項目や JPY 評価が不足する項目は CSV / 手動補完を併用します
- API key / secret は Windows DPAPI で暗号化保存し、次回は空欄でも保存済み設定を再利用できます

## 年跨ぎ / 全期間集計
- 計算結果画面から、`開始年` と `終了年` を指定して期間合算ができます
- 両方空欄なら、読込済みデータの最初の年から最後の年までを使います
- 既存の単年計算とは分離した追加集計で、申告用の既存結果は変えません
- 期間集計では、年別サマリ・期間銘柄別サマリ・期間内取引件数・読込済み総取引件数を表示します

## 分析レイヤーについて
分析レイヤーは **既存の税務計算ロジックとは分離** していて、申告用の損益計算結果を上書きしません。

### 追加した主な分析指標
- `total_equity_jpy`
  - `cash_jpy + Σ(quantity × jpy_price)` で計算した JPY 建て総資産
- `total_equity_usd`
  - `total_equity_jpy / USDJPY` で換算した USD 建て総資産
- `external_inflows_jpy / usd`
  - 外部から口座へ入ってきた入金・入庫の累計
- `external_outflows_jpy / usd`
  - 口座から外へ出た出金・出庫の累計
- `net_external_flow_jpy / usd`
  - `累計入金 - 累計出金`
- `equity_if_no_withdrawals_jpy / usd`
  - `総資産 + 累計出金`
  - 「生活費などで引き出していなかったら、今どれくらい残っていたか」を見る補助指標
- `equity_minus_net_contributions_jpy / usd`
  - `総資産 - 純入金`
  - 自分で後から入れたお金を差し引いた、ざっくりした増減感を見る補助指標
- `asset_quantity_total_by_symbol`
  - 各時点での BTC / ETH / XRP / SOL などの保有総量
- `realized_pnl_jpy / usd`
  - 分析レイヤー上の実現損益。**fees は別表示** にしています
- `unrealized_pnl_jpy / usd`
  - 残存在庫の時価評価額から、分析レイヤー上の簿価を引いた含み損益
- `inventory_revaluation_jpy / usd`
  - snapshot では current unrealized と同値、期間集計ではその増減差分
- `fees_jpy / usd`
  - 取得できた手数料コスト
- `spread_cost_jpy / usd`
  - 取得不能時は `0`
- `slippage_jpy / usd`
  - 取得不能時は `0`
- `funding_jpy / usd`
  - 対象データが無い場合は `0`
- `benchmark_total_equity_jpy / usd`
  - 年初保有と同じ外部入出金だけを反映した passive hold 比較
- `edge_vs_benchmark_jpy / usd`
  - `actual_total_equity - benchmark_total_equity`
- `trading_edge_jpy / usd`
  - `edge_vs_benchmark - reward_income`
  - 相場全体の上昇や reward/staking を除いた、売買寄与をざっくり見る補助指標

### 分析の前提
- 分析は **税務申告値の置換ではなく補助表示** です
- 分析画面では単年だけでなく、開始年〜終了年を指定した **期間分析 / 全期間分析** もできます
- 分析画面では、`累計入金 / 累計出金 / 純入金 / 出金してなかったら / 純入金差引後` を見て、
  「残高は増えていないが、生活側の出し入れを除くとどうだったか」を確認できます
- 分析画面の `現在残高照合` では、Binance API の現在残高を公開 JPY 価格で評価して、履歴再構築の数量差・評価額差を確認できます
- USD 建て分析は `USD` / `USDT` / `USDC` 等の内部レート、または手動レート CSV に依存します
- `ETH/BTC` のような暗号資産同士交換は、JPY 時価が無ければ手動補完を優先します
- benchmark は passive hold の比較軸であり、将来の最適戦略を保証するものではありません

## API 接続設定の保存方式
- Windows DPAPI を使ったローカル暗号化ファイルです
- 保存場所: `app/storage/secrets/`
- 平文の API key / secret をログへ出しません
- ブラウザの localStorage には保存しません
- 保存後は `API連携` 画面で、マスク済みキー表示と既定 symbol / 期間の再利用ができます

## セキュリティ上の注意
- 既定で `127.0.0.1` バインドです
- 既定で外部公開しません
- CORS は localhost のみ許可します
- 元 CSV は read-only import で、保存時はコピーを内部管理します
- ログには API secret を出しません
- export に秘密情報を含めません

## 既知の制約
- Binance Japan API については、口座 / 権限 / 提供差分により使えない項目がありえます
- API 同期は Binance Spot 互換 read-only endpoint を前提にした実装です
- `myTrades` は symbol 単位での取得を前提としており、公式 Spot API の制約上、`startTime` と `endTime` の窓が長すぎると分割取得が必要です
- 暗号資産同士交換で JPY 評価が無いものは、手動レート CSV が無い限り要確認です
- 外部ウォレット間の厳密な紐付けは未自動化です
- fee を暗号資産で支払ったケースは、JPY 換算が不足すると要確認に残ります
- 国税庁提出用の最終書類そのものを自動保証するものではありません
- 分析レイヤーの `spread_cost / slippage / funding` は、元データに十分な列が無い場合 `0` のままです
- USD 建て総資産は `USDJPY` または USD-like レートが無いと未評価になります

## 起動方法
### 1. venv 作成
```powershell
cd H:\cryptocalc
python -m venv venv
```

### 2. venv を有効化
PowerShell の場合:
```powershell
cd H:\cryptocalc
.\venv\Scripts\Activate.ps1
```

コマンドプロンプト (`cmd.exe`) の場合:
```bat
cd /d H:\cryptocalc
venv\Scripts\activate.bat
```

### 3. 依存インストール
```powershell
cd H:\cryptocalc
.\venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
```

### 4. ローカル Web UI + API 起動
```powershell
cd H:\cryptocalc
.\venv\Scripts\Activate.ps1
python -m uvicorn app.api.main:app --host 127.0.0.1 --port 8017
```

### 5. ブラウザで開く
- UI: `http://127.0.0.1:8017/dashboard`
- API Docs: `http://127.0.0.1:8017/docs`

PowerShell で `Activate.ps1` が実行できない場合は、次のように一時的に実行ポリシーを緩めるか、`cmd.exe` 側の `activate.bat` を使ってね。
```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.\venv\Scripts\Activate.ps1
```

## 使い方
1. `ファイル取込` 画面で Binance Japan CSV / XLSX を読み込む
2. まとめて入れたい時は `H:\cryptocalc\data\` 配下へファイルを置いて、`data フォルダを一括取込` を押す
3. 必要なら `手動補正 CSV` と `JPY 補完レート CSV` を個別に読み込む
4. `計算結果` 画面で年度と方式を選んで計算する
5. `分析` 画面で年度と参照方式を選び、総資産 / benchmark / edge を確認する
6. `分析` 画面で `累計入金 / 累計出金 / 純入金 / 出金してなかったら / 純入金差引後` を見て、生活側の出し入れを考慮した感覚と突き合わせる
7. 必要なら `現在残高照合` を実行して、Binance 現在残高ベース総資産と履歴再構築残高の差を確認する
8. `要確認` 画面で未知 / 未評価 / 重複疑いを確認する
9. `エクスポート` 画面で年次 CSV / 国税庁補助 CSV / Excel / 分析 CSV / JSON を生成する

## サンプルファイル
- `samples/binance_japan_sample.csv`
- `samples/manual_adjustments_sample.csv`
- `samples/manual_rates_sample.csv`

## テスト
```powershell
cd H:\cryptocalc
python -m pytest -q
```

現在のローカル確認:
- `31 passed`
- 実 Binance Japan XLSX sample の parser 読込確認済み

## ロールバック
### 直近の変更だけ戻す
```powershell
cd H:\cryptocalc
git status
git diff
```

### すべて初期コミットへ戻す
```powershell
cd H:\cryptocalc
git log --oneline
git restore .
```

### データだけ掃除したい場合
次を削除すれば、import / logs / exports / secrets を掃除できます。
- `app/storage/app_data/`
- `logs/`
- `exports/`
- `app/storage/secrets/`

## 免責の再掲
- このソフトは日本の暗号資産損益計算の補助を目的とします
- 最終的な税務判断は利用者責任です
- 不明取引や評価不能取引は要確認として残します
- 税務署提出用の最終書類を自動保証しません
