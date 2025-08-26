import argparse
import json
import os
import time
from collections import defaultdict
from datetime import datetime
from multiprocessing import Value, Process
from pathlib import Path
from random import randint

import requests

DEFAULT_BASE_URL = 'http://127.0.0.1:8000'
DEFAULT_WAREHOUSES = 10
DEFAULT_DURATION = 5
DEFAULT_OUTPUT_DIR = 'results/'

import logging

# Configure the logger
logging.basicConfig(
    filename="benchmark.log",
    format="%(asctime)s %(levelname)s %(message)s",
    level=logging.INFO,
    filemode='w',
)
logger = logging.getLogger(__name__)


def virtual_user(pid: int, count_orders: Value, run: Value, args):
    BASE_URL = args.base_url
    WAREHOUSES = args.warehouses
    OUTPUT_DIR = args.output_dir

    transaction_log = defaultdict(list)
    while run.value:
        choice = randint(1, 100)
        if choice <= 45:
            # new_order
            transaction = {
                'method': 'POST',
                'url': BASE_URL + '/orders',
                'json': {'w_id': randint(1, WAREHOUSES), 'c_id': randint(1, WAREHOUSES * 10)}
            }
            transaction_name = 'order'
            with count_orders.get_lock():
                count_orders.value += 1
        elif choice <= 88:
            # payment
            transaction = {
                'method': 'POST',
                'url': BASE_URL + '/payment',
                'json': {'w_id': randint(1, WAREHOUSES), 'c_id': randint(1, WAREHOUSES * 10)},
            }
            transaction_name = 'payment'
        elif choice <= 92:
            # order_status
            transaction = {
                'method': 'GET',
                'url': BASE_URL + f'/customers/{randint(1, WAREHOUSES * 10)}/orders',
            }
            transaction_name = 'order_status'
        elif choice <= 96:
            # delivery
            transaction = {
                'method': 'POST',
                'url': BASE_URL + f'/warehouses/{randint(1, WAREHOUSES)}/deliveries',
            }
            transaction_name = 'delivery'
        else:
            # stock_level
            transaction = {
                'method': 'GET',
                'url': BASE_URL + f'/warehouses/{randint(1, WAREHOUSES)}/stock',
            }
            transaction_name = 'stock_level'

        start_time = time.time()
        response = requests.request(**transaction)
        transaction_log[transaction_name].append(time.time() - start_time)

        logger.info(transaction['url'])

        if not response.ok:
            logger.error(response.json())

    filename = os.path.join(OUTPUT_DIR, f'latency_{pid}_{datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}.json')
    with open(filename, 'w') as writer:
        json.dump(transaction_log, writer)


def init(base_url: str):
    return requests.post(base_url + '/init_db', json={'warehouses': DEFAULT_WAREHOUSES})


def stopper(run: Value, start: Value, duration: int):
    while run.value:
        if time.time() - start.value >= duration:
            run.value = False
        time.sleep(1)


def write_results(count_orders: int, duration: int, output_dir: str):
    filename = os.path.join(output_dir, f'benchmark_{datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}.json')
    with open(filename, 'w') as writer:
        json.dump({
            'duration': duration,
            'count_orders': count_orders,
        }, writer)


def main(args):
    BASE_URL = args.base_url
    DURATION = args.duration
    OUTPUT_DIR = args.output_dir

    logger.info('Benchmark starting...')

    res = init(BASE_URL)
    if res.ok:
        logger.info('Initialized successfully')
    else:
        logger.error('Initialization failed: {}', res.json())
        exit(1)

    count_orders = Value('i', 0)
    start = Value('d', 0.0)
    run = Value('b', True)

    start.value = time.time()
    processes = []
    for i in range(2):
        process = Process(target=virtual_user, args=(i, count_orders, run, args))
        process.start()
        processes.append(process)

    process = Process(target=stopper, args=(run, start, DURATION))
    process.start()
    processes.append(process)

    for process in processes:
        process.join()

    write_results(count_orders.value, DURATION, OUTPUT_DIR)


if __name__ == '__main__':
    # Initialize argument parser
    parser = argparse.ArgumentParser()

    # Add arguments with default values and type conversion
    parser.add_argument(
        "-u", "--base_url",
        default=DEFAULT_BASE_URL,
        help="Base URL for the application (default: %(default)s)",
    )
    parser.add_argument(
        "-w", "--warehouses",
        type=int,
        default=DEFAULT_WAREHOUSES,
        help="Number of warehouses to simulate (default: %(default)s)",
    )
    parser.add_argument(
        "-d", "--duration",
        type=int,
        default=DEFAULT_DURATION,
        help="Duration of the simulation in seconds (default: %(default)s)",
    )
    parser.add_argument(
        "-o", "--output_dir",
        type=str,
        default=DEFAULT_OUTPUT_DIR
    )

    # Parse arguments from command line
    args = parser.parse_args()

    Path(args.output_dir).mkdir(parents=True, exist_ok=True)

    main(args)
