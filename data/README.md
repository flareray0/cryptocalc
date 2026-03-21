# data フォルダ

このフォルダ配下に、ローカルで取り込みたい CSV / XLSX / XLSM を置いて使うでよ。

おすすめ構成:

```text
data/
  ├─ exchange/
  │  ├─ Binance-取引履歴-....csv
  │  ├─ Binance-現物取引履歴-....csv
  │  ├─ Binance-法定通貨による入金履歴-....csv
  │  └─ Binance-法定通貨による出金履歴-....csv
  ├─ manual_adjustments/
  │  └─ my_adjustments.csv
  └─ manual_rates/
     └─ jpy_rates.csv
```

判定ルール:
- `manual_adjustments` / `adjustments` を含む名前は手動補正として読むでよ。
- `manual_rates` / `rates` / `レート` を含む名前は JPY 補完レートとして読むでよ。
- それ以外は通常の取引履歴として読むでよ。

注意:
- このフォルダの実データは `.gitignore` で git へ乗らないようにしてあるでよ。
- 元ファイルは read-only import で、上書きしないでよ。
