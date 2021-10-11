import json
import logging
import os
import time
from typing import Any, Dict, List, Optional

import requests

logging.basicConfig(level=logging.INFO)


def merge_data(prev: List[Dict[str, Any]], curr: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    '''Merge curr into prev, the element in list must have a date field.'''
    for c in curr:
        x: Optional[Dict[str, Any]] = None
        for p in prev:
            if p['date'] == c['date']:
                x = p
                break
        if x is None:
            prev.append(c)
        else:
            x.update(c)
    return sorted(prev, key=lambda d: d['date'])


def http_get(url: str, field_name: str) -> List[Dict[str, Any]]:
    resp = requests.get(url=url)
    if resp.status_code == 200:
        data = resp.json()
        return data[field_name]
    else:
        logging.warning(f'{url} {resp.status_code} {resp.text}')
        if resp.status_code == 429:
            exit(0)
        return []


def get_coins() -> List[str]:
    url = 'https://www.bitstamp.net/api/v2/trading-pairs-info/'
    resp = requests.get(url=url)
    if resp.status_code == 200:
        symbols = resp.json()
        coins = [s['name'].split('/')[0].lower() for s in symbols]
        coins = sorted(list(set(coins)))
        return coins
    else:
        logging.warning(f'{url} {resp.status_code} {resp.text}')
        if resp.status_code == 429:
            exit(0)
        return []


def get_price(coin: str) -> None:
    url = f'https://www.bitstamp.net/api-internal/stats/v1/{coin}/financial/price'
    curr = http_get(url=url, field_name='price')
    file_path = f'./data/price-{coin}.json'
    if os.path.exists(file_path):
        with open(file_path, 'rt') as f_in:
            prev = json.loads(f_in.read())
    else:
        prev = []
    prev = merge_data(prev, curr)
    with open(file_path, 'wt') as f_out:
        json.dump(prev, f_out, indent=2)


def get_transactions(coin: str) -> None:
    url = f'https://www.bitstamp.net/api-internal/stats/v1/{coin}/network/transactions'
    curr = http_get(url=url, field_name='txsStats')
    file_path = f'./data/transactions-{coin}.json'
    if os.path.exists(file_path):
        with open(file_path, 'rt') as f_in:
            prev = json.loads(f_in.read())
    else:
        prev = []
    prev = merge_data(prev, curr)
    with open(file_path, 'wt') as f_out:
        json.dump(prev, f_out, indent=2)


def get_addresses(coin: str) -> None:
    url = f'https://www.bitstamp.net/api-internal/stats/v1/{coin}/network/addresses'
    curr = http_get(url=url, field_name='addressesStats')
    file_path = f'./data/addresses-{coin}.json'
    if os.path.exists(file_path):
        with open(file_path, 'rt') as f_in:
            prev = json.loads(f_in.read())
    else:
        prev = []
    prev = merge_data(prev, curr)
    with open(file_path, 'wt') as f_out:
        json.dump(prev, f_out, indent=2)


def get_large_transactions(coin: str) -> None:
    url = f'https://www.bitstamp.net/api-internal/stats/v1/{coin}/financial/large_transactions'
    file_path = f'./data/large_transactions-{coin}.json'
    curr = http_get(url=url, field_name='largeTxs')
    if os.path.exists(file_path):
        with open(file_path, 'rt') as f_in:
            prev = json.loads(f_in.read())
    else:
        prev = []
    prev = merge_data(prev, curr)
    with open(file_path, 'wt') as f_out:
        json.dump(prev, f_out, indent=2)


if __name__ == "__main__":
    # // 8000 requests per 10 minutes, see `REQUEST LIMITS` at https://www.bitstamp.net/api/
    cooldown_time = 0.075
    coins = get_coins()
    for coin in coins:
        logging.info(coin)
        get_price(coin)
        time.sleep(cooldown_time)
        get_transactions(coin)
        time.sleep(cooldown_time)
        get_addresses(coin)
        time.sleep(cooldown_time)
        get_large_transactions(coin)
        time.sleep(cooldown_time)
