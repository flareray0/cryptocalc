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
