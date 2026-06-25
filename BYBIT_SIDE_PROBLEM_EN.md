**Problem**

There is a critical issue in the bot's Bybit P2P side mapping logic.

Observed behavior:
- the signal may show a Bybit link to one tab (`SELL` or `BUY`),
- but the quoted price and merchant are actually taken from the opposite side of the book,
- as a result, the user opens the provided link and cannot find the merchant or the quoted rate there,
- or there are better valid offers on the correct side, but the bot does not use them.

**What needs to be checked**

Please verify and align the full Bybit side contract across the code path:

1. Side encoding in API requests:
   - function `bybit_api_side_code(...)`
   - payload sent to `/v5/p2p/item/online`

2. Side decoding from Bybit responses:
   - function `canonical_side_from_bybit_response(...)`
   - validation of `raw_side` returned by Bybit

3. All related usage points:
   - public P2P market scanning
   - parsing Bybit ads into `P2POrder`
   - decoding active ads / snapshots in the order manager
   - generation of the correct Bybit web link/tab

**Expected behavior**

- If the bot performs step `SELL USDT`, it must:
  - search only the correct Bybit side for `sell`,
  - take both the price and merchant strictly from that same side,
  - generate a Telegram link that opens the same side used for selection.

- If the bot performs step `BUY USDT BACK`, it must:
  - search only the correct Bybit side for `buy`,
  - take both the price and merchant strictly from that same side,
  - generate a Telegram link that opens the same side used for selection.

**Important**

- Please fix only the Bybit side logic.
- Do not change Binance or BingX side logic.
- Do not modify `.env`, API keys, payment filters, or FX logic unless it is strictly required for the Bybit side mapping fix.

**Files included in the packet**

- `p2p_bot/utils/canonical_side.py`
- `p2p_bot/utils/bybit_p2p.py`
- `p2p_bot/modules/order_manager.py`
- `tests/test_canonical_side.py`
- `tests/test_bybit_p2p_payloads.py`

This is the minimal file set directly related to Bybit side mapping and propagation through the bot.
