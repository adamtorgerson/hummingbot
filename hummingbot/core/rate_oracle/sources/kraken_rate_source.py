from decimal import Decimal
from typing import TYPE_CHECKING, Dict, Optional

from hummingbot.connector.exchange.kraken import kraken_constants as CONSTANTS
from hummingbot.connector.utils import split_hb_trading_pair
from hummingbot.core.rate_oracle.sources.rate_source_base import RateSourceBase
from hummingbot.core.utils import async_ttl_cache

if TYPE_CHECKING:
    from hummingbot.connector.exchange.kraken.kraken_exchange import KrakenExchange


class KrakenRateSource(RateSourceBase):
    def __init__(self):
        super().__init__()
        self._exchange: Optional[KrakenExchange] = None  # delayed because of circular reference

    @property
    def name(self) -> str:
        return "kraken"

    @async_ttl_cache(ttl=30, maxsize=1)
    async def get_prices(self, quote_token: Optional[str] = None) -> Dict[str, Decimal]:
        self._ensure_exchange()
        results = {}
        try:
            tickers = await self._exchange._api_get(
                path_url=CONSTANTS.TICKER_PATH_URL,
                is_auth_required=False
            )
            for name, values in tickers.items():
                try:
                    pair = await self._exchange.trading_pair_associated_to_exchange_symbol(name)
                except KeyError:
                    # Ignore results for which their symbols is not tracked by the connector
                    continue

                if quote_token is not None:
                    _, quote = split_hb_trading_pair(trading_pair=pair)
                    if quote != quote_token:
                        continue

                if len(values["a"][0]) == 0 or len(values["b"][0]) == 0:
                    # Ignore results for which the order book is empty
                    continue

                if Decimal(values["a"][0]) > 0 and Decimal(values["b"][0]) > 0:
                    results[pair] = (Decimal(values["a"][0]) +
                                     Decimal(values["b"][0])) / Decimal("2")
        except Exception:
            self.logger().exception(
                msg="Unexpected error while retrieving rates from Kraken. Check the log file for more info.",
            )
        return results

    def _ensure_exchange(self):
        if self._exchange is None:
            self._exchange = self._build_kraken_connector_without_private_keys()

    @staticmethod
    def _build_kraken_connector_without_private_keys() -> 'KrakenExchange':
        from hummingbot.connector.exchange.kraken.kraken_exchange import KrakenExchange

        return KrakenExchange(
            kraken_api_key="",
            kraken_secret_key="",
            trading_pairs=[],
            trading_required=False,
        )
