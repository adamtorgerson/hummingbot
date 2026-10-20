import asyncio
import json
import re
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from typing import Awaitable, Dict, List, Optional
from unittest.mock import AsyncMock, MagicMock, patch

from aioresponses import aioresponses
from bidict import bidict

from hummingbot.connector.exchange.kraken import kraken_constants as CONSTANTS
from hummingbot.connector.exchange.kraken.kraken_api_user_stream_data_source import KrakenAPIUserStreamDataSource
from hummingbot.connector.exchange.kraken.kraken_auth import KrakenAuth
from hummingbot.connector.exchange.kraken.kraken_constants import KrakenAPITier
from hummingbot.connector.exchange.kraken.kraken_exchange import KrakenExchange
from hummingbot.connector.exchange.kraken.kraken_utils import build_rate_limits_by_tier
from hummingbot.connector.test_support.network_mocking_assistant import NetworkMockingAssistant
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler


class KrakenAPIUserStreamDataSourceTest(IsolatedAsyncioWrapperTestCase):
    level = 0

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.base_asset = "COINALPHA"
        cls.quote_asset = "HBOT"
        cls.trading_pair = f"{cls.base_asset}-{cls.quote_asset}"
        cls.ex_trading_pair = f"{cls.base_asset}{cls.quote_asset}"
        cls.ws_ex_trading_pair = cls.base_asset + "/" + cls.quote_asset
        cls.api_tier = KrakenAPITier.STARTER

    def setUp(self) -> None:
        super().setUp()
        self.log_records = []
        self.listening_task: Optional[asyncio.Task] = None

        self.mock_time_provider = MagicMock()

    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        self.mocking_assistant = NetworkMockingAssistant()
        self.throttler = AsyncThrottler(build_rate_limits_by_tier(self.api_tier))

        self.connector = KrakenExchange(
            kraken_api_key="",
            kraken_secret_key="",
            trading_pairs=[self.trading_pair],
            trading_required=False)

        not_a_real_secret = "kQH5HW/8p1uGOVjbgWA7FunAmGO8lsSUXNsu3eow76sz84Q18fWxnyRzBHCd3pd5nE9qa99HAZtuZuj6F1huXg=="
        self.auth = KrakenAuth(api_key="someKey", secret_key=not_a_real_secret, time_provider=self.mock_time_provider)

        self.connector._web_assistants_factory._auth = self.auth
        self.data_source = KrakenAPIUserStreamDataSource(self.connector,
                                                         api_factory=self.connector._web_assistants_factory,
                                                         )

        self.data_source.logger().setLevel(1)
        self.data_source.logger().addHandler(self)

        self.resume_test_event = asyncio.Event()

        self.connector._set_trading_pair_symbol_map(bidict({self.ex_trading_pair: self.trading_pair}))

    def tearDown(self) -> None:
        self.listening_task and self.listening_task.cancel()
        super().tearDown()

    def handle(self, record):
        self.log_records.append(record)

    def _is_logged(self, log_level: str, message: str) -> bool:
        return any(record.levelname == log_level and record.getMessage() == message
                   for record in self.log_records)

    def async_run_with_timeout(self, coroutine: Awaitable, timeout: float = 1):
        ret = self.ev_loop.run_until_complete(asyncio.wait_for(coroutine, timeout))
        return ret

    @staticmethod
    def get_auth_response_mock() -> Dict:
        auth_resp = {
            "error": [],
            "result": {
                "token": "1Dwc4lzSwNWOAwkMdqhssNNFhs1ed606d1WcF3XfEMw",
                "expires": 900
            }
        }
        return auth_resp

    @staticmethod
    def get_open_orders_mock() -> List:
        open_orders = [
            [
                {
                    "OGTT3Y-C6I3P-XRI6HX": {
                        "status": "closed"
                    }
                },
                {
                    "OGTT3Y-C6I3P-XRI6HX": {
                        "status": "closed"
                    }
                }
            ],
            "openOrders",
            {
                "sequence": 59342
            }
        ]
        return open_orders

    @staticmethod
    def get_own_trades_mock() -> List:
        own_trades = [
            [
                {
                    "TDLH43-DVQXD-2KHVYY": {
                        "cost": "1000000.00000",
                        "fee": "1600.00000",
                        "margin": "0.00000",
                        "ordertxid": "TDLH43-DVQXD-2KHVYY",
                        "ordertype": "limit",
                        "pair": "XBT/EUR",
                        "postxid": "OGTT3Y-C6I3P-XRI6HX",
                        "price": "100000.00000",
                        "time": "1560516023.070651",
                        "type": "sell",
                        "vol": "1000000000.00000000"
                    }
                },
            ],
            "ownTrades",
            {
                "sequence": 2948
            }
        ]
        return own_trades

    @aioresponses()
    async def test_get_auth_token(self, mocked_api):
        url = f"{CONSTANTS.BASE_URL}{CONSTANTS.GET_TOKEN_PATH_URL}"
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))
        resp = self.get_auth_response_mock()
        mocked_api.post(regex_url, body=json.dumps(resp))

        ret = await (self.data_source.get_auth_token())

        self.assertEqual(ret, resp["result"]["token"])

    @aioresponses()
    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    async def test_listen_for_user_stream(self, mocked_api, ws_connect_mock):
        url = f"{CONSTANTS.BASE_URL}{CONSTANTS.GET_TOKEN_PATH_URL}"
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))
        resp = self.get_auth_response_mock()
        mocked_api.post(regex_url, body=json.dumps(resp))
        ws_connect_mock.return_value = self.mocking_assistant.create_websocket_mock()
        output_queue = asyncio.Queue()
        asyncio.create_task(self.data_source.listen_for_user_stream(output_queue))

        resp = self.get_open_orders_mock()
        self.mocking_assistant.add_websocket_aiohttp_message(
            websocket_mock=ws_connect_mock.return_value, message=json.dumps(resp)
        )
        ret = await output_queue.get()

        self.assertEqual(ret, resp)

        resp = self.get_own_trades_mock()
        self.mocking_assistant.add_websocket_aiohttp_message(
            websocket_mock=ws_connect_mock.return_value, message=json.dumps(resp)
        )
        ret = await (output_queue.get())

        self.assertEqual(ret, resp)

    @aioresponses()
    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    async def test_reconnect_after_invalid_session_uses_fresh_token(self, mocked_api, ws_connect_mock):
        self.data_source._sleep = AsyncMock()
        url = f"{CONSTANTS.BASE_URL}{CONSTANTS.GET_TOKEN_PATH_URL}"
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))
        expired_token_resp = self.get_auth_response_mock()
        expired_token_resp["result"]["token"] = "expiredToken"
        fresh_token_resp = self.get_auth_response_mock()
        fresh_token_resp["result"]["token"] = "freshToken"
        mocked_api.post(regex_url, body=json.dumps(expired_token_resp))
        mocked_api.post(regex_url, body=json.dumps(fresh_token_resp))

        first_ws = self.mocking_assistant.create_websocket_mock()
        second_ws = self.mocking_assistant.create_websocket_mock()
        ws_connect_mock.side_effect = [first_ws, second_ws]
        self.mocking_assistant.add_websocket_aiohttp_message(
            websocket_mock=first_ws,
            message=json.dumps({"errorMessage": "ESession:Invalid session", "event": "subscriptionStatus",
                                "status": "error", "subscription": {"name": "openOrders"}}))
        self.mocking_assistant.add_websocket_aiohttp_message(
            websocket_mock=second_ws, message=json.dumps(self.get_open_orders_mock()))

        output_queue = asyncio.Queue()
        self.listening_task = asyncio.create_task(self.data_source.listen_for_user_stream(output_queue))
        ret = await asyncio.wait_for(output_queue.get(), timeout=5)

        self.assertEqual(ret, self.get_open_orders_mock())
        first_tokens = [msg["subscription"]["token"]
                        for msg in self.mocking_assistant.json_messages_sent_through_websocket(first_ws)]
        second_tokens = [msg["subscription"]["token"]
                         for msg in self.mocking_assistant.json_messages_sent_through_websocket(second_ws)]
        self.assertEqual(["expiredToken", "expiredToken"], first_tokens)
        self.assertEqual(["freshToken", "freshToken"], second_tokens)

    async def test_reconnect_delay_backs_off_and_resets_after_data(self):
        delays = []
        for _ in range(8):
            self.data_source._failed_connection_attempts += 1
            delays.append(self.data_source._reconnect_delay())

        self.assertEqual([2.0, 4.0, 8.0, 16.0, 32.0, 60.0, 60.0, 60.0], delays)

        await self.data_source._process_event_message(self.get_open_orders_mock(), asyncio.Queue())

        self.assertEqual(0.0, self.data_source._reconnect_delay())
