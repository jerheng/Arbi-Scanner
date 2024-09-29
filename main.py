import ccxt.async_support as ccxt
import asyncio
from itertools import combinations
from tabulate import tabulate
import colorama
import logging
from datetime import datetime

colorama.init()

# Set up logging
logging.basicConfig(filename='arbitrage_opportunities.log', level=logging.INFO, 
                    format='%(asctime)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

# Estimated fees for each exchange (these are example values, please update with accurate fees)
FEES = {
    'binance': 0.001,  # 0.1%
    'kraken': 0.004,    # 0.40%
    'bitfinex': 0.002,  # 0.20%
}

async def fetch_ticker(exchange, symbol):
    try:
        ticker = await exchange.fetch_ticker(symbol)
        return {
            'exchange': exchange.id,
            'ask': ticker['ask'],
            'bid': ticker['bid'],
            'volume': ticker['baseVolume']
        }
    except Exception as e:
        logging.error(f"Error fetching {symbol} from {exchange.id}: {str(e)}")
        return None

async def scan_symbol(exchanges, symbol):
    tasks = [fetch_ticker(exchange, symbol) for exchange in exchanges]
    results = await asyncio.gather(*tasks)
    # Filter out exchanges with volume == 0
    return [r for r in results if r is not None and r['volume'] > 0]

async def scan_arbitrage(exchanges, symbols):
    symbol_tasks = [scan_symbol(exchanges, symbol) for symbol in symbols]
    all_results = await asyncio.gather(*symbol_tasks)
    
    arbitrage_opportunities = []
    arbitrage_before_fees = {}
    
    for symbol, results in zip(symbols, all_results):
        if not results:
            arbitrage_before_fees[symbol] = None
            continue
        
        highest_bid = max(result['bid'] for result in results)
        lowest_ask = min(result['ask'] for result in results)
        max_diff = highest_bid - lowest_ask
        arbitrage_before_fees[symbol] = max_diff if max_diff > 0 else None
        
        for (exchange1, exchange2) in combinations(results, 2):
            if exchange1['bid'] > exchange2['ask']:
                buy_price = exchange2['ask']
                sell_price = exchange1['bid']
                buy_fee = buy_price * FEES[exchange2['exchange']]
                sell_fee = sell_price * FEES[exchange1['exchange']]
                
                # Calculate the amount of crypto you can buy with 1 USDT after fees
                crypto_amount = (1 - FEES[exchange2['exchange']]) / buy_price
                
                # Calculate how much USDT you get after selling that crypto amount and paying fees
                sell_usdt = (crypto_amount * sell_price) * (1 - FEES[exchange1['exchange']])
                
                net_profit = sell_usdt - 1  # Subtract the initial 1 USDT investment
                profit_percentage = (net_profit / 1) * 100  # Calculate percentage based on 1 USDT investment
                
                arbitrage_opportunities.append({
                    'symbol': symbol,
                    'buy_at': exchange2['exchange'],
                    'sell_at': exchange1['exchange'],
                    'buy_price': buy_price,
                    'sell_price': sell_price,
                    'fees': buy_fee + sell_fee,
                    'net_profit': net_profit,
                    'profit_percentage': profit_percentage,
                    'volume': min(exchange1['volume'], exchange2['volume'])
                })
    
    # Sort opportunities by profit percentage in descending order
    arbitrage_opportunities.sort(key=lambda x: x['profit_percentage'], reverse=True)
    
    return arbitrage_opportunities, all_results, arbitrage_before_fees

def create_table(exchanges, symbols, opportunities, all_results, arbitrage_before_fees):
    headers = ['Symbol', 'Arbitrage Before Fees'] + \
              [f"{e.id}\nBid/Ask/Volume" for e in exchanges] + \
              ['Fees', 'Net Profit', 'Profit %', 'Min Volume']
    table = []
    
    for symbol, results in zip(symbols, all_results):
        row = [symbol]
        
        # Arbitrage Before Fees
        arb_before = arbitrage_before_fees.get(symbol)
        if arb_before is not None:
            arb_before_str = f"\033[92m{arb_before:.2f}\033[0m" if arb_before > 0 else f"\033[91m{arb_before:.2f}\033[0m"
        else:
            arb_before_str = "N/A"
        row.append(arb_before_str)
        
        # Exchange data
        for exchange in exchanges:
            exchange_data = next((r for r in results if r['exchange'] == exchange.id and r['volume'] > 0), None)
            if exchange_data:
                cell = f"{exchange_data['bid']:.2f}/{exchange_data['ask']:.2f}/{exchange_data['volume']:.2f}"
            else:
                cell = "N/A"
            row.append(cell)
        
        # Fees, Net Profit, Profit %, Min Volume
        op = next((o for o in opportunities if o['symbol'] == symbol), None)
        if op:
            fees_str = f"\033[93m{op['fees']:.4f}\033[0m"  # Yellow for fees
            net_profit_str = f"\033[92m{op['net_profit']:.4f}\033[0m" if op['net_profit'] > 0 else f"\033[91m{op['net_profit']:.4f}\033[0m"
            profit_pct_str = f"\033[92m{op['profit_percentage']:.2f}%\033[0m" if op['profit_percentage'] > 0 else f"\033[91m{op['profit_percentage']:.2f}%\033[0m"
            row.append(fees_str)
            row.append(net_profit_str)
            row.append(profit_pct_str)
            row.append(f"{op['volume']:.2f}")
        else:
            row.extend(["N/A", "N/A", "N/A", "N/A"])
        
        table.append(row)
    
    return tabulate(table, headers=headers, tablefmt="grid")

async def continuous_arbitrage_scan():
    exchange_ids = ['binance', 'kraken', 'bitfinex']
    exchanges = [getattr(ccxt, exchange_id)() for exchange_id in exchange_ids]
    
    # Load markets for all exchanges
    await asyncio.gather(*[exchange.load_markets() for exchange in exchanges])
    
    # Find common symbols across all exchanges
    common_symbols = set.intersection(*[set(exchange.symbols) for exchange in exchanges])
    
    # Filter for USDT pairs
    symbols = [symbol for symbol in common_symbols if symbol.endswith('/USDT')]

    try:
        while True:
            opportunities, all_results, arbitrage_before_fees = await scan_arbitrage(exchanges, symbols)
            table = create_table(exchanges, symbols, opportunities, all_results, arbitrage_before_fees)
            print("\033[2J\033[H")  # Clear screen
            print(table)
            
            if opportunities:
                for op in opportunities:
                    color = "\033[92m" if op['net_profit'] > 0 else "\033[91m"
                    reset = "\033[0m"
                    log_message = (f"{op['symbol']}: Buy at {op['buy_at']} for {op['buy_price']:.2f}, "
                                   f"Sell at {op['sell_at']} for {op['sell_price']:.2f}, "
                                   f"Fees: {op['fees']:.4f}, Net Profit: {op['net_profit']:.4f}, "
                                   f"Profit: {op['profit_percentage']:.2f}%, Volume: {op['volume']:.2f}")
                    logging.info(f"{color}{log_message}{reset}")
            
            await asyncio.sleep(5)
    finally:
        await asyncio.gather(*[exchange.close() for exchange in exchanges])

if __name__ == "__main__":
    asyncio.run(continuous_arbitrage_scan())