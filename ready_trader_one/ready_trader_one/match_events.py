import asyncio
import csv
import logging
import queue
import threading

from typing import Optional, TextIO

from .account import CompetitorAccount
from .order_book import Order
from .types import ITaskListener, Side


class MatchEvent(tuple):
    __slots__ = ()

    def __new__(cls, time, competitor, operation, order_id, side, volume, price, lifespan, fee, future_price,
                etf_price, account_balance, future_position, etf_position, profit_loss, total_fees, max_drawdown,
                buy_count, sell_count):
        return tuple.__new__(cls, (time, competitor, operation, order_id, side, volume, price, lifespan, fee,
                                   future_price, etf_price, account_balance, future_position, etf_position,
                                   profit_loss, total_fees, max_drawdown, buy_count, sell_count))

    def __iter__(self):
        return iter(("%.6f" % self[0],  # time
                     self[1],  # competitor
                     self[2],  # operation
                     self[3],  # order_id
                     "SB"[self[4]] if self[4] is not None else None,  # side
                     self[5],  # volume
                     "%.2f" % (self[6] / 100.0) if self[6] is not None else None,  # price
                     ("FAK", "GFD")[self[7]] if self[7] is not None else None,  # lifespan
                     "%.2f" % (self[8] / 100.0) if self[8] is not None else None,  # fee
                     "%.2f" % (self[9] / 100.0) if self[9] is not None else None,  # future price
                     "%.2f" % (self[10] / 100.0) if self[10] is not None else None,  # etf price
                     "%.2f" % (self[11] / 100.0),  # account_balance
                     self[12],  # future position
                     self[13],  # etf position
                     "%.2f" % (self[14] / 100.0),  # profit_loss
                     "%.2f" % (self[15] / 100.0),  # total_fees
                     "%.2f" % (self[16] / 100.0),  # max drawdown
                     self[17],  # buy count
                     self[18]))  # sell count


class MatchEvents(object):
    """A processor of match events that it writes to a file."""

    def __init__(self, filename: str, loop: asyncio.AbstractEventLoop, listener: ITaskListener):
        """Initialise a new instance of the MatchEvents class."""
        self.event_loop: asyncio.AbstractEventLoop = loop
        self.filename: str = filename
        self.finished: bool = False
        self.listener: ITaskListener = listener
        self.logger = logging.getLogger("MATCH_EVENTS")
        self.queue: queue.Queue = queue.Queue()
        self.writer_task: Optional[threading.Thread] = None

    def __del__(self):
        """Destroy an instance of the MatchEvents class."""
        if not self.finished:
            self.queue.put(None)
        self.writer_task.join()

    def amend(self, now: float, name: str, account: CompetitorAccount, order: Order, diff: int, future_price: int,
              etf_price: int) -> None:
        """Create a new amend event."""
        self.queue.put(MatchEvent(now, name, "Amend", order.client_order_id, order.side, diff, order.price,
                                  order.lifespan, 0.0, future_price, etf_price, account.account_balance,
                                  account.future_position, account.etf_position, account.profit_or_loss,
                                  account.total_fees, account.max_drawdown, account.buy_volume, account.sell_volume))

    def breach(self, now: float, name: str, account: CompetitorAccount, future_price: int, etf_price: int) -> None:
        """Create a new disconnect event."""
        self.queue.put(MatchEvent(now, name, "Breach", None, None, None, None, None, 0.0, future_price, etf_price,
                                  account.account_balance, account.future_position, account.etf_position,
                                  account.profit_or_loss, account.total_fees, account.max_drawdown, account.buy_volume,
                                  account.sell_volume))

    def cancel(self, now: float, name: str, account: CompetitorAccount, order: Order, diff: int, future_price: int,
               etf_price) -> None:
        """Create a new cancel event."""
        self.queue.put(MatchEvent(now, name, "Cancel", order.client_order_id, order.side, diff, order.price,
                                  order.lifespan, 0.0, future_price, etf_price, account.account_balance,
                                  account.future_position, account.etf_position, account.profit_or_loss,
                                  account.total_fees, account.max_drawdown, account.buy_volume, account.sell_volume))

    def disconnect(self, now: float, name: str, account: CompetitorAccount, future_price: int, etf_price: int) -> None:
        """Create a new disconnect event."""
        if not self.finished:
            self.queue.put(MatchEvent(now, name, "Disconnect", None, None, None, None, None, 0.0, future_price,
                                      etf_price, account.account_balance, account.future_position, account.etf_position,
                                      account.profit_or_loss, account.total_fees, account.max_drawdown,
                                      account.buy_volume, account.sell_volume))

    def fill(self, now: float, name: str, account: CompetitorAccount, order: Order, price: int, diff: int, fee: int,
             future_price: int) -> None:
        """Create a new fill event."""
        self.queue.put(MatchEvent(now, name, "Fill", order.client_order_id, order.side, diff, price, order.lifespan,
                                  fee, future_price, price, account.account_balance, account.future_position,
                                  account.etf_position, account.profit_or_loss, account.total_fees,
                                  account.max_drawdown, account.buy_volume, account.sell_volume))

    def finish(self) -> None:
        """Indicate the the series of events is complete."""
        self.queue.put(None)
        self.finished = True

    def hedge(self, now: float, name: str, account: CompetitorAccount, side: Side, price: int, diff: int,
              future_price: int, etf_price: int) -> None:
        """Create a new fill event."""
        self.queue.put(MatchEvent(now, name, "Hedge", None, side, diff, price, None, 0.0, future_price, etf_price,
                                  account.account_balance, account.future_position, account.etf_position,
                                  account.profit_or_loss, account.total_fees, account.max_drawdown, account.buy_volume,
                                  account.sell_volume))

    def insert(self, now: float, name: str, account: CompetitorAccount, order: Order, future_price: int,
               etf_price: int) -> None:
        """Create a new insert event."""
        self.queue.put(MatchEvent(now, name, "Insert", order.client_order_id, order.side, order.remaining_volume,
                                  order.price, order.lifespan, 0.0, future_price, etf_price, account.account_balance,
                                  account.future_position, account.etf_position, account.profit_or_loss,
                                  account.total_fees, account.max_drawdown, account.buy_volume, account.sell_volume))

    def on_writer_done(self, num_events: int) -> None:
        """Called when the match event writer thread is done."""
        self.listener.on_task_complete(self)
        self.logger.info("writer thread complete after processing %d match events", num_events)

    def start(self):
        """Start the match events writer thread"""
        try:
            match_events = open(self.filename, "w", newline="")
        except IOError as e:
            self.logger.error("failed to open match events file: filename=%s", self.filename, exc_info=e)
            raise
        else:
            self.writer_task = threading.Thread(target=self.writer, args=(match_events,), daemon=False, name="writer")
            self.writer_task.start()

    def tick(self, now: float, name: str, account: CompetitorAccount, future_price: int, etf_price: int) -> None:
        """Create a new tick event"""
        self.queue.put(MatchEvent(now, name, "Tick", None, None, None, None, None, 0.0, future_price, etf_price,
                                  account.account_balance, account.future_position, account.etf_position,
                                  account.profit_or_loss, account.total_fees, account.max_drawdown, account.buy_volume,
                                  account.sell_volume))

    def writer(self, match_events: TextIO) -> None:
        """Fetch match events from a queue and write them to a file"""
        count = 0
        fifo = self.queue

        try:
            with match_events:
                csv_writer = csv.writer(match_events)
                csv_writer.writerow(("Time", "Competitor", "Operation", "OrderId", "Side", "Volume", "Price",
                                     "Lifespan", "Fee", "FuturePrice", "EtfPrice", "AccountBalance", "FuturePosition",
                                     "EtfPosition", "ProfitLoss", "TotalFees", "MaxDrawdown", "BuyVolume",
                                     "SellVolume"))

                evt = fifo.get()
                while evt is not None:
                    count += 1
                    csv_writer.writerow(evt)
                    evt = fifo.get()
        finally:
            if not self.event_loop.is_closed():
                self.event_loop.call_soon_threadsafe(self.on_writer_done, count)
