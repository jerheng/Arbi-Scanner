import ccxt.async_support as ccxt
import os
import asyncio
from itertools import combinations
from tabulate import tabulate
import colorama
import logging
from datetime import datetime
from multiprocessing import Pool

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
        if ticker['ask'] == 0 or ticker['bid'] == 0:
            logging.warning(f"Zero price detected for {symbol} on {exchange.id}: Ask: {ticker['ask']}, Bid: {ticker['bid']}")
        return {
            'symbol': symbol,
            'exchange': exchange.id,
            'ask': ticker['ask'],
            'bid': ticker['bid'],
            'volume': ticker['baseVolume']
        }
    except Exception as e:
        logging.error(f"Error fetching {symbol} from {exchange.id}: {str(e)}")
        return None

async def fetch_all_tickers(exchanges, symbols):
    tasks = [fetch_ticker(exchange, symbol) for exchange in exchanges for symbol in symbols]
    return await asyncio.gather(*tasks)

async def scan_arbitrage(exchanges, symbols):
    all_results = await fetch_all_tickers(exchanges, symbols)
    
    # Group results by symbol
    grouped_results = {}
    for result in all_results:
        if result is not None:
            symbol = result['symbol']
            if symbol not in grouped_results:
                grouped_results[symbol] = []
            grouped_results[symbol].append(result)
    
    arbitrage_opportunities = []
    arbitrage_before_fees = {}
    
    for symbol, results in grouped_results.items():
        if not results:
            arbitrage_before_fees[symbol] = None
            continue
        
        base_currency = symbol.split('/')[-1]
        
        highest_bid = max(result['bid'] for result in results)
        lowest_ask = min(result['ask'] for result in results)
        max_diff = highest_bid - lowest_ask
        arbitrage_before_fees[symbol] = max_diff if max_diff > 0 else None
        
        for (exchange1, exchange2) in combinations(results, 2):
            if exchange1['bid'] > exchange2['ask'] and exchange2['ask'] > 0:
                buy_price = exchange2['ask']
                sell_price = exchange1['bid']
                buy_fee = buy_price * FEES[exchange2['exchange']]
                sell_fee = sell_price * FEES[exchange1['exchange']]
                
                crypto_amount = (1 - FEES[exchange2['exchange']]) / buy_price
                sell_amount = (crypto_amount * sell_price) * (1 - FEES[exchange1['exchange']])
                
                net_profit = sell_amount - 1
                profit_percentage = (net_profit / 1) * 100
                
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
    
    arbitrage_opportunities.sort(key=lambda x: x['profit_percentage'], reverse=True)
    
    return arbitrage_opportunities, grouped_results, arbitrage_before_fees

def create_table(exchanges, symbols, opportunities, all_results, arbitrage_before_fees):
    if not symbols:
        return "No data available for this base currency."

    headers = ['Symbol', 'Arbitrage Before Fees'] + \
              [f"{e.id}\nBid/Ask/Volume" for e in exchanges] + \
              ['Fees', 'Net Profit', 'Profit %', 'Min Volume']
    table = []
    
    for symbol in symbols:
        row = [symbol]
        
        arb_before = arbitrage_before_fees.get(symbol)
        if arb_before is not None:
            arb_before_str = f"\033[92m{arb_before:.2f}\033[0m" if arb_before > 0 else f"\033[91m{arb_before:.2f}\033[0m"
        else:
            arb_before_str = "N/A"
        row.append(arb_before_str)
        
        results = all_results.get(symbol, [])
        for exchange in exchanges:
            exchange_data = next((r for r in results if r['exchange'] == exchange.id), None)
            if exchange_data:
                cell = f"{exchange_data['bid']:.2f}/{exchange_data['ask']:.2f}/{exchange_data['volume']:.2f}"
            else:
                cell = "N/A"
            row.append(cell)
        
        op = next((o for o in opportunities if o['symbol'] == symbol), None)
        if op:
            fees_str = f"\033[93m{op['fees']:.4f}\033[0m"
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
def get_valid_pairs(common_symbols):
    base_currencies = ['USDT', 'BTC', 'ETH']
    valid_pairs = []
    for symbol in common_symbols:
        base = symbol.split('/')[-1]
        if base in base_currencies:
            valid_pairs.append(symbol)
    return valid_pairs

def group_symbols_by_base(symbols):
    grouped = {'USDT': [], 'BTC': [], 'ETH': []}
    for symbol in symbols:
        base = symbol.split('/')[-1]
        if base in grouped:
            grouped[base].append(symbol)
    return grouped

async def continuous_arbitrage_scan():
    exchange_ids = ['binance', 'kraken', 'bitfinex']
    exchanges = [getattr(ccxt, exchange_id)() for exchange_id in exchange_ids]
    
    await asyncio.gather(*[exchange.load_markets() for exchange in exchanges])
    
    common_symbols = set.intersection(*[set(exchange.symbols) for exchange in exchanges])
    symbols = get_valid_pairs(common_symbols)
    grouped_symbols = group_symbols_by_base(symbols)

    try:
        while True:
            opportunities, all_results, arbitrage_before_fees = await scan_arbitrage(exchanges, symbols)
            
            os.system('cls' if os.name == 'nt' else 'clear')
            
            for base_currency in ['USDT', 'BTC', 'ETH']:
                base_symbols = grouped_symbols[base_currency]
                base_opportunities = [op for op in opportunities if op['symbol'] in base_symbols]
                base_results = {symbol: results for symbol, results in all_results.items() if symbol in base_symbols}
                base_arbitrage_before_fees = {k: v for k, v in arbitrage_before_fees.items() if k in base_symbols}
                
                table = create_table(exchanges, base_symbols, base_opportunities, base_results, base_arbitrage_before_fees)
                print(f"\n{base_currency} Pairs:")
                print(table)
            
            if opportunities:
                for op in opportunities:
                    color = "\033[92m" if op['net_profit'] > 0 else "\033[91m"
                    reset = "\033[0m"
                    log_message = (f"{op['symbol']}: Buy at {op['buy_at']} for {op['buy_price']:.8f}, "
                                   f"Sell at {op['sell_at']} for {op['sell_price']:.8f}, "
                                   f"Fees: {op['fees']:.8f}, Net Profit: {op['net_profit']:.8f}, "
                                   f"Profit: {op['profit_percentage']:.2f}%, Volume: {op['volume']:.8f}")
                    logging.info(f"{color}{log_message}{reset}")
            
            await asyncio.sleep(5)
    finally:
        await asyncio.gather(*[exchange.close() for exchange in exchanges])

if __name__ == "__main__":
    asyncio.run(continuous_arbitrage_scan())