import ccxt.async_support as ccxt
import asyncio

# async def get_async_exchanges():
#     async_exchanges = []
#     for exchange_id in ccxt.exchanges:
#         exchange_class = getattr(ccxt, exchange_id)
#         if hasattr(exchange_class, 'fetch_ticker'):
#             async_exchanges.append(exchange_id)
#     return async_exchanges

# async def main():
#     exchanges = await get_async_exchanges()
#     print("Exchanges that support async price fetching:")
#     for exchange in exchanges:
#         print(exchange)

# if __name__ == "__main__":
#     import asyncio
#     asyncio.run(main())

async def test_fetch_ticker(exchange_id, symbol):
    exchange = getattr(ccxt, exchange_id)()
    ticker = await exchange.fetch_ticker(symbol)
    print(f"{exchange_id} {symbol}: {ticker}")

async def main():
    await test_fetch_ticker('okex', 'BTC/USDT')
    await test_fetch_ticker('coinbase', 'BTC/USDT')

if __name__ == "__main__":
    asyncio.run(main())