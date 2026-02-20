import json
from decimal import Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase

from aioresponses import aioresponses

from hummingbot.connector.exchange.kraken import kraken_constants as CONSTANTS
from hummingbot.connector.utils import combine_to_hb_trading_pair
from hummingbot.core.rate_oracle.sources.kraken_rate_source import KrakenRateSource


class KrakenRateSourceTest(IsolatedAsyncioWrapperTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.target_token = "ETH"
        cls.global_token = "USDT"
        cls.trading_pair = combine_to_hb_trading_pair(base=cls.target_token, quote=cls.global_token)
        cls.ignored_trading_pair = combine_to_hb_trading_pair(base="SOME", quote="PAIR")

    def setup_kraken_responses(self, mock_api, expected_rate: Decimal):
        symbols_url = f"{CONSTANTS.BASE_URL}/{CONSTANTS.ASSET_PAIRS_PATH_URL}"
        symbols_response = [
            {
                "altname": f"{self.target_token}{self.global_token}",
                "wsname": f"{self.target_token}/{self.global_token}",
                "aclass_base": "currency",
                "base": f"{self.target_token}",
                "aclass_quote": "currency",
                "quote": f"{self.global_token}",
                "lot": "unit",
                "pair_decimals": 2,
                "lot_decimals": 8,
                "lot_multiplier": 1,
                "leverage_buy": [
                    2,
                    3,
                ],
                "leverage_sell": [
                    2,
                    3,
                ],
                "fees": [
                    [
                        0,
                        0.26
                    ],
                    [
                        50000,
                        0.24
                    ],
                ],
                "fees_maker": [
                    [
                        0,
                        0.16
                    ],
                    [
                        50000,
                        0.14
                    ],
                ],
                "fee_volume_currency": "ZUSD",
                "margin_call": 80,
                "margin_stop": 40,
                "ordermin": "0.005"
            }
        ]
        mock_api.get(url=symbols_url, body=json.dumps(symbols_response))
        prices_url = f"{CONSTANTS.BASE_URL}/{CONSTANTS.TICKER_PATH_URL}"
        prices_response = [
            {
                "error": [],
                "result": {
                    f"{self.target_token}/{self.global_token}": {
                        "a": [
                            "1983.160000",
                            "2",
                            "2.000"
                        ],
                        "b": [
                            "1982.560000",
                            "1",
                            "1.000"
                        ],
                        "c": [
                            "1982.830000",
                            "0.01776628"
                        ],
                        "v": [
                            "425.83616054",
                            "2481.42735297"
                        ],
                        "p": [
                            "1963.187741",
                            "1970.754739"
                        ],
                        "t": [
                            672,
                            3539
                        ],
                        "l": [
                            "1950.480000",
                            "1922.260000"
                        ],
                        "h": [
                            "1985.930000",
                            "2036.970000"
                        ],
                        "o": "1956.000000"
                    }
                }
            }
        ]
        mock_api.get(url=prices_url, body=json.dumps(prices_response))

    @aioresponses()
    async def test_get_prices(self, mock_api):
        expected_rate = Decimal("10")
        self.setup_kraken_responses(mock_api=mock_api, expected_rate=expected_rate)

        rate_source = KrakenRateSource()
        prices = await rate_source.get_prices()
        print("PRICES: " + str(prices))

        self.assertIn(self.trading_pair, prices)
        self.assertEqual(expected_rate, prices[self.trading_pair])
        self.assertNotIn(self.ignored_trading_pair, prices)
