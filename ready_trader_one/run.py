import concurrent.futures
import functools
import time
import traceback
import sys

import ready_trader_one.exchange
import ready_trader_one.trader


def __on_task_completed(future: concurrent.futures.Future, name: str, executor: concurrent.futures.Executor) -> None:
    """Consume the result of a task."""
    try:
        future.result()
    except Exception as e:
        print("'%s' threw an exception: %s" % (name, e), file=sys.stderr)
        # traceback.print_exc(file=sys.stderr)
        executor.shutdown(False)


def main():
    """Run a match."""
    # To add another auto-trader add its python module name to this list and
    # add it to the 'Traders' section of the exchange.json file.
    trader_names = ["autotrader", "example1", "example2"]

    with concurrent.futures.ProcessPoolExecutor(max_workers=len(trader_names) + 1) as executor:
        exchange = executor.submit(ready_trader_one.exchange.main)
        exchange.add_done_callback(functools.partial(__on_task_completed, name="exchange", executor=executor))

        # Give the exchange time to start up.
        time.sleep(0.5)
        if exchange.done():
            return

        traders = [executor.submit(ready_trader_one.trader.main, name) for name in trader_names]
        for name, task in zip(trader_names, traders):
            task.add_done_callback(functools.partial(__on_task_completed, name=name, executor=executor))

        concurrent.futures.wait(traders + [exchange])


if __name__ == "__main__":
    main()
