from __future__ import annotations

from pathlib import Path

from app.parsers.binance_japan_parser import BinanceJapanParser


def test_binance_japan_parser_reads_sample():
    parser = BinanceJapanParser()
    sample = Path(r"H:\cryptocalc\samples\binance_japan_sample.csv")
    batch = parser.parse(sample)

    assert batch.transaction_count == 4
    assert batch.transactions[0].base_asset == "BTC"
    assert batch.transactions[0].gross_amount_jpy is not None
    assert batch.transactions[2].tx_type.value == "crypto_swap"
    assert batch.transactions[3].review_flag is True


def test_binance_japan_parser_reads_japanese_balance_history(tmp_path):
    sample = tmp_path / "binance_jp_balance_history.csv"
    sample.write_text(
        "\n".join(
            [
                "ユーザーID,時間,アカウント,操作,コイン,変更,備考",
                "1,25-07-17 09:18:07,Spot,Deposit,JPY,15000,",
                "1,25-07-17 09:55:49,Spot,Binance Convert,JPY,-15000,",
                "1,25-07-17 09:55:49,Spot,Binance Convert,ETH,0.02929314,",
                "1,25-07-17 10:37:15,Spot,Transfer Between Main and Funding Wallet,BTC,-0.0002,",
                "1,25-07-17 10:37:15,Funding,Transfer Between Main and Funding Wallet,BTC,0.0002,",
                "1,25-07-17 11:06:11,Spot,Transaction Fee,ETH,-0.0000035,",
                "1,25-07-17 11:06:11,Spot,Transaction Buy,ETH,0.0035,",
                "1,25-07-17 11:06:11,Spot,Transaction Spend,BTC,-0.00009908,",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    parser = BinanceJapanParser()
    batch = parser.parse(sample)

    assert batch.transaction_count == 3
    assert batch.unknown_column_names == []
    assert batch.unknown_tx_types == []
    assert batch.transactions[0].tx_type.value == "transfer_in"
    assert batch.transactions[0].base_asset == "JPY"
    assert batch.transactions[1].tx_type.value == "buy"
    assert batch.transactions[1].base_asset == "ETH"
    assert batch.transactions[1].gross_amount_jpy == 15000
    assert batch.transactions[2].tx_type.value == "crypto_swap"
    assert batch.transactions[2].base_asset == "ETH"
    assert batch.transactions[2].quote_asset == "BTC"
