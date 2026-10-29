import json
import re
from decimal import Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase

from aioresponses import aioresponses

from hummingbot.connector.exchange.kraken import kraken_constants as CONSTANTS
from hummingbot.core.rate_oracle.sources.kraken_rate_source import KrakenRateSource


class KrakenRateSourceTest(IsolatedAsyncioWrapperTestCase):
    @staticmethod
    def ticker(ask: str, bid: str):
        return {"a": [ask, "1", "1.000"], "b": [bid, "1", "1.000"], "c": [bid, "0.1"]}

    def setup_kraken_responses(self, mock_api):
        asset_pairs = {
            # Legacy pair: Kraken's pair name differs from the altname the connector maps by
            "XXBTZUSD": {"altname": "XBTUSD", "wsname": "XBT/USD", "base": "XXBT", "quote": "ZUSD"},
            # Newer pair: pair name and altname are the same
            "SOLUSD": {"altname": "SOLUSD", "wsname": "SOL/USD", "base": "SOL", "quote": "ZUSD"},
            "ETHUSDT": {"altname": "ETHUSDT", "wsname": "ETH/USDT", "base": "XETH", "quote": "USDT"},
        }
        mock_api.get(
            url=re.compile(rf"^{CONSTANTS.BASE_URL}/*{CONSTANTS.ASSET_PAIRS_PATH_URL}"),
            body=json.dumps({"error": [], "result": asset_pairs}),
            repeat=True,
        )
        tickers = {
            "XXBTZUSD": self.ticker("83010.0", "83000.0"),
            "SOLUSD": self.ticker("106.6", "106.4"),
            "ETHUSDT": self.ticker("2452.0", "2450.0"),
            # Not in AssetPairs: ignored
            "SOMEPAIR": self.ticker("1.0", "1.0"),
        }
        mock_api.get(
            url=re.compile(rf"^{CONSTANTS.BASE_URL}/*{CONSTANTS.TICKER_PATH_URL}"),
            body=json.dumps({"error": [], "result": tickers}),
            repeat=True,
        )

    @aioresponses()
    async def test_get_prices(self, mock_api):
        self.setup_kraken_responses(mock_api=mock_api)

        prices = await KrakenRateSource().get_prices()

        self.assertEqual(Decimal("83005.0"), prices["BTC-USD"])
        self.assertEqual(Decimal("106.5"), prices["SOL-USD"])
        self.assertEqual(Decimal("2451.0"), prices["ETH-USDT"])
        self.assertEqual(3, len(prices))

    @aioresponses()
    async def test_get_prices_filters_by_quote_token(self, mock_api):
        self.setup_kraken_responses(mock_api=mock_api)

        prices = await KrakenRateSource().get_prices(quote_token="USDT")

        self.assertEqual({"ETH-USDT": Decimal("2451.0")}, prices)
