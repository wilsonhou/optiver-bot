import bisect
import logging

from typing import Dict, List

from .account import CompetitorAccount
from .match_events import MatchEvents
from .order_book import IOrderListener, Order, OrderBook
from .types import ICompetitor, IController, IExecutionChannel, Instrument, Lifespan, Side


class Competitor(ICompetitor, IOrderListener):
    """A competitor in the Ready Trader One competition."""

    def __init__(self, name: str, controller: IController, exec_channel: IExecutionChannel, future_book: OrderBook,
                 etf_book: OrderBook, account: CompetitorAccount, match_events: MatchEvents, position_limit: int,
                 order_count_limit: int, active_volume_limit: int, tick_size: float):
        """Initialise a new instance of the Competitor class."""
        self.account: CompetitorAccount = account
        self.active_volume: int = 0
        self.active_volume_limit: int = active_volume_limit
        self.etf_book: OrderBook = etf_book
        self.future_book: OrderBook = future_book
        self.buy_prices: List[int] = list()
        self.controller: IController = controller
        self.exec_channel: IExecutionChannel = exec_channel
        self.last_client_order_id: int = -1
        self.logger: logging.Logger = logging.getLogger("COMPETITOR")
        self.match_events: MatchEvents = match_events
        self.order_count_limit: int = order_count_limit
        self.name: str = name
        self.orders: Dict[int, Order] = dict()
        self.position_limit: int = position_limit
        self.sell_prices: List[int] = list()
        self.tick_size: int = int(tick_size * 100.0)

    def disconnect(self) -> None:
        """Disconnect this competitor."""
        if self.exec_channel is not None:
            self.exec_channel.close()

    def hard_breach(self, now: float, client_order_id: int, message: bytes) -> None:
        """Handle a hard breach by this competitor."""
        self.send_error_and_close(now, client_order_id, message)
        self.match_events.breach(now, self.name, self.account, self.future_book.last_traded_price(),
                                 self.etf_book.last_traded_price())

    def on_connection_lost(self, now: float) -> None:
        """Called when the connection to the matching engine is lost."""
        self.exec_channel = None
        self.match_events.disconnect(now, self.name, self.account, self.future_book.last_traded_price(),
                                     self.etf_book.last_traded_price())
        for o in tuple(self.orders.values()):
            self.etf_book.cancel(now, o)

    # IOrderListener callbacks
    def on_order_amended(self, now: float, order: Order, volume_removed: int) -> None:
        """Called when an order is amended."""
        if self.exec_channel is not None:
            self.exec_channel.send_order_status(order.client_order_id, order.volume - order.remaining_volume,
                                                order.remaining_volume, order.total_fees)
        self.match_events.amend(now, self.name, self.account, order, -volume_removed,
                                self.future_book.last_traded_price(), self.etf_book.last_traded_price())

        self.active_volume -= volume_removed

        if order.remaining_volume == 0:
            del self.orders[order.client_order_id]
            if order.side == Side.BUY:
                self.buy_prices.pop(bisect.bisect(self.buy_prices, order.price) - 1)
            else:
                self.sell_prices.pop(bisect.bisect(self.sell_prices, -order.price) - 1)

    def on_order_cancelled(self, now: float, order: Order, volume_removed: int) -> None:
        """Called when an order is cancelled."""
        if self.exec_channel is not None:
            self.exec_channel.send_order_status(order.client_order_id, order.volume - volume_removed,
                                                order.remaining_volume, order.total_fees)
        self.match_events.cancel(now, self.name, self.account, order, -volume_removed,
                                 self.future_book.last_traded_price(), self.etf_book.last_traded_price())

        self.active_volume -= volume_removed

        del self.orders[order.client_order_id]
        if order.side == Side.BUY:
            self.buy_prices.pop(bisect.bisect(self.buy_prices, order.price) - 1)
        else:
            self.sell_prices.pop(bisect.bisect(self.sell_prices, -order.price) - 1)

    def on_order_placed(self, now: float, order: Order) -> None:
        """Called when a good-for-day order is placed in the order book."""
        # Only send an order status if the order has not partially filled
        if order.volume == order.remaining_volume and self.exec_channel is not None:
            self.exec_channel.send_order_status(order.client_order_id, 0, order.remaining_volume, order.total_fees)

    def on_order_filled(self, now: float, order: Order, price: int, volume: int, fee: int) -> None:
        """Called when an order is partially or completely filled."""
        self.active_volume -= volume

        if order.remaining_volume == 0:
            del self.orders[order.client_order_id]
            if order.side == Side.BUY:
                self.buy_prices.pop()
            else:
                self.sell_prices.pop()

        last_traded = self.future_book.last_traded_price()
        self.account.transact(Instrument.ETF, order.side, price, volume, fee)
        self.account.mark_to_market(last_traded, price)
        self.match_events.fill(now, self.name, self.account, order, price, -volume, fee, last_traded)

        midpoint = self.future_book.midpoint_price()
        side = Side.BUY if order.side == Side.SELL else Side.SELL
        self.account.transact(Instrument.FUTURE, side, midpoint, volume, 0)
        self.account.mark_to_market(last_traded, price)
        self.match_events.hedge(now, self.name, self.account, side, midpoint, volume, last_traded, price)

        if self.exec_channel is not None:
            self.exec_channel.send_order_status(order.client_order_id, order.volume - order.remaining_volume,
                                                order.remaining_volume, order.total_fees)
            self.exec_channel.send_position_change(self.account.future_position, self.account.etf_position)
            if abs(self.account.etf_position) > self.position_limit:
                self.hard_breach(now, order.client_order_id, b"position limit breached")

    # Message callbacks
    def on_amend_message(self, now: float, client_order_id: int, volume: int) -> None:
        """Called when an amend order request is received from the competitor."""
        if client_order_id > self.last_client_order_id:
            self.send_error(now, client_order_id, b"out-of-order client_order_id in amend message")
            return

        if client_order_id in self.orders:
            order = self.orders[client_order_id]
            if volume > order.volume:
                self.send_error(now, client_order_id, b"amend operation would increase order volume")
            else:
                self.etf_book.amend(now, order, volume)

    def on_cancel_message(self, now: float, client_order_id: int) -> None:
        """Called when a cancel order request is received from the competitor."""
        if client_order_id > self.last_client_order_id:
            self.send_error(now, client_order_id, b"out-of-order client_order_id in cancel message")
            return

        if client_order_id in self.orders:
            self.etf_book.cancel(now, self.orders[client_order_id])

    def on_insert_message(self, now: float, client_order_id: int, side: int, price: int, volume: int,
                          lifespan: int) -> None:
        """Called when an insert order request is received from the competitor."""
        if client_order_id <= self.last_client_order_id:
            self.send_error(now, client_order_id, b"duplicate or out-of-order client_order_id")
            return

        self.last_client_order_id = client_order_id

        if side != Side.BUY and side != Side.SELL:
            self.send_error(now, client_order_id, b"%d is not a valid side" % side)
            return

        if lifespan != Lifespan.FILL_AND_KILL and lifespan != Lifespan.GOOD_FOR_DAY:
            self.send_error(now, client_order_id, b"%d is not a valid lifespan" % lifespan)
            return

        if price % self.tick_size != 0:
            self.send_error(now, client_order_id, b"price is not a multiple of tick size")
            return

        if len(self.orders) == self.order_count_limit:
            self.send_error(now, client_order_id, b"order rejected: active order count limit breached")
            return

        if volume < 1:
            self.send_error(now, client_order_id, b"order rejected: invalid volume")
            return

        if self.active_volume + volume > self.active_volume_limit:
            self.send_error(now, client_order_id, b"order rejected: active order volume limit breached")
            return

        if now == 0.0:
            self.send_error(now, client_order_id, b"order rejected: market not yet open")
            return

        if ((side == Side.BUY and self.sell_prices and price >= -self.sell_prices[-1])
                or (side == Side.SELL and self.buy_prices and price <= self.buy_prices[-1])):
            self.send_error(now, client_order_id, b"order rejected: in cross with an existing order")
            return

        order = self.orders[client_order_id] = Order(client_order_id, Instrument.ETF, Lifespan(lifespan), Side(side),
                                                     price, volume, self)
        if side == Side.BUY:
            bisect.insort(self.buy_prices, price)
        else:
            bisect.insort(self.sell_prices, -price)
        self.match_events.insert(now, self.name, self.account, order, self.future_book.last_traded_price(),
                                 self.etf_book.last_traded_price())
        self.active_volume += volume
        self.etf_book.insert(now, order)

    def on_timer_tick(self, now: float, future_price: int, etf_price: int) -> None:
        """Called on each timer tick to update the auto-trader."""
        self.account.mark_to_market(future_price or 0, etf_price or 0)
        self.match_events.tick(now, self.name, self.account, future_price, etf_price)

    def send_error(self, now: float, client_order_id: int, message: bytes) -> None:
        """Send an error message to the auto-trader and shut down the match."""
        self.exec_channel.send_error(client_order_id, message)
        self.logger.info("'%s' sent error message: time=%.6f client_order_id=%s message='%s'", self.name, now,
                         client_order_id, message.decode())

    def send_error_and_close(self, now: float, client_order_id: int, message: bytes) -> None:
        """Send an error message to the auto-trader and shut down the match."""
        self.send_error(now, client_order_id, message)
        self.logger.info("'%s' closing execution channel at time=%.6f", self.name, now)
        self.exec_channel.close()

    def set_start_time(self, start_time: float) -> None:
        """Set the start time of the match."""
        self.exec_channel.set_start_time(start_time)
