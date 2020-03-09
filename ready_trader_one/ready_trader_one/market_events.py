import asyncio
import csv
import enum
import logging
import queue
import threading

from typing import Dict, Optional, TextIO

from .order_book import IOrderListener, Order, OrderBook
from .types import IController, ITaskListener, Instrument, Lifespan, Side

MARKET_EVENT_QUEUE_SIZE = 1024


class MarketEventOperation(enum.IntEnum):
    AMEND = 0
    CANCEL = 1
    INSERT = 2


class MarketEvent(object):
    """A market event."""
    __slots__ = ("time", "instrument", "operation", "order_id", "side", "volume", "price", "lifespan")

    def __init__(self, time: float, instrument: int, operation: MarketEventOperation, order_id: int, side: Side,
                 volume: int, price: int, lifespan: Optional[Lifespan]):
        """Initialise a new instance of the MarketEvent class."""
        self.time: float = time
        self.instrument: int = instrument
        self.operation: MarketEventOperation = operation
        self.order_id: int = order_id
        self.side: Optional[Side] = side
        self.volume: int = volume
        self.price: int = price
        self.lifespan: Optional[Lifespan] = lifespan


class MarketEvents(IOrderListener):
    """A processor of market events read from a file."""

    def __init__(self, filename: str, loop: asyncio.AbstractEventLoop, controller: IController, future_book: OrderBook,
                 etf_book: OrderBook, listener: ITaskListener):
        """Initialise a new instance of the MarketEvents class.
        """
        self.controller: IController = controller
        self.controller: IController = controller
        self.etf_book: OrderBook = etf_book
        self.etf_orders: Dict[int, Order] = dict()
        self.event_loop: asyncio.AbstractEventLoop = loop
        self.filename: str = filename
        self.future_book: OrderBook = future_book
        self.future_orders: Dict[int, Order] = dict()
        self.listener: ITaskListener = listener
        self.logger: logging.Logger = logging.getLogger("MARKET_EVENTS")
        self.queue: queue.Queue = queue.Queue(MARKET_EVENT_QUEUE_SIZE)
        self.reader_task: Optional[threading.Thread] = None

        # Prime the event pump with a no-op event
        self.next_event: Optional[MarketEvent] = MarketEvent(0.0, Instrument.FUTURE, MarketEventOperation.CANCEL, 0,
                                                             Side.BUY, 0, 0, Lifespan.FILL_AND_KILL)

    # IOrderListener callbacks

    def on_order_amended(self, now: float, order: Order, volume_removed: int) -> None:
        """Called when the order is amended."""
        if order.remaining_volume == 0:
            if order.instrument == Instrument.FUTURE:
                del self.future_orders[order.client_order_id]
            elif order.instrument == Instrument.ETF:
                del self.etf_orders[order.client_order_id]

    def on_order_cancelled(self, now: float, order: Order, volume_removed: int) -> None:
        """Called when the order is cancelled."""
        if order.instrument == Instrument.FUTURE:
            del self.future_orders[order.client_order_id]
        elif order.instrument == Instrument.ETF:
            del self.etf_orders[order.client_order_id]

    def on_order_placed(self, now: float, order: Order) -> None:
        """Called when a good-for-day order is placed in the order book."""
        if order.instrument == Instrument.FUTURE:
            self.future_orders[order.client_order_id] = order
        elif order.instrument == Instrument.ETF:
            self.etf_orders[order.client_order_id] = order

    def on_order_filled(self, now: float, order: Order, price: int, volume: int, fee: int) -> None:
        """Called when the order is partially or completely filled."""
        if order.remaining_volume == 0:
            if order.instrument == Instrument.FUTURE and order.client_order_id in self.future_orders:
                del self.future_orders[order.client_order_id]
            elif order.instrument == Instrument.ETF and order.client_order_id in self.etf_orders:
                del self.etf_orders[order.client_order_id]

    def on_reader_done(self, num_events: int) -> None:
        """Called when the market data reader thread is done."""
        self.listener.on_task_complete(self)
        self.logger.info("reader thread complete after processing %d market events", num_events)

    def process_market_events(self, elapsed_time: float) -> None:
        """Process market events from the queue."""
        evt: MarketEvent = self.next_event

        while evt and evt.time < elapsed_time:
            if evt.instrument == Instrument.FUTURE:
                orders = self.future_orders
                book = self.future_book
            else:
                orders = self.etf_orders
                book = self.etf_book

            if evt.operation == MarketEventOperation.INSERT:
                order = Order(evt.order_id, Instrument(evt.instrument), evt.lifespan, evt.side, evt.price, evt.volume,
                              self)
                book.insert(evt.time, order)
            elif evt.order_id in orders:
                if evt.operation == MarketEventOperation.CANCEL:
                    book.cancel(evt.time, orders[evt.order_id])
                elif evt.volume < 0:
                    # evt.operation must be MarketEventOperation.AMEND
                    order = orders[evt.order_id]
                    book.amend(evt.time, order, order.volume + evt.volume)

            evt = self.queue.get()

        self.next_event = evt
        if evt is None:
            self.controller.market_events_complete()

    def reader(self, market_data: TextIO) -> None:
        """Read the market data file and place order events in the queue."""
        fifo = self.queue

        operations = dict(zip(("Amend", "Cancel", "Insert"), tuple(MarketEventOperation)))
        lifespans = {"FAK": Lifespan.FILL_AND_KILL, "GFD": Lifespan.GOOD_FOR_DAY}
        sides = {"A": Side.SELL, "B": Side.BUY}

        with market_data:
            csv_reader = csv.reader(market_data)
            next(csv_reader)  # Skip header row
            for row in csv_reader:
                fifo.put(MarketEvent(float(row[0]), int(row[1]), operations[row[2]], int(row[3]), sides.get(row[4]),
                                     int(float(row[5])) if row[5] else 0, int(float(row[6]) * 100) if row[6] else 0,
                                     lifespans.get(row[7])))
            fifo.put(None)

        self.event_loop.call_soon_threadsafe(self.on_reader_done, csv_reader.line_num - 1)

    def start(self):
        """Start the market events reader thread"""
        try:
            market_data = open(self.filename)
        except OSError as e:
            self.logger.error("failed to open market data file: filename='%s'" % self.filename, exc_info=e)
            raise
        else:
            self.reader_task = threading.Thread(target=self.reader, args=(market_data,), daemon=True, name="reader")
            self.reader_task.start()
