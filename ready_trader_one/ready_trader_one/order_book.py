from bisect import bisect, insort_left
import collections

from typing import Deque, Dict, List, Optional

from .types import Instrument, Lifespan, Side


MINIMUM_BID = 0
MAXIMUM_ASK = 2 ** 32 - 1
TOP_LEVEL_COUNT = 5


class IOrderListener(object):
    def on_order_amended(self, now: float, order, volume_removed: int) -> None:
        """Called when the order is amended."""
        pass

    def on_order_cancelled(self, now: float, order, volume_removed: int) -> None:
        """Called when the order is cancelled."""
        pass

    def on_order_placed(self, now: float, order) -> None:
        """Called when a good-for-day order is placed in the order book."""
        pass

    def on_order_filled(self, now: float, order, price: int, volume: int, fee: int) -> None:
        """Called when the order is partially or completely filled."""
        pass


class ITradeListener(object):
    def on_trade(self, instrument: Instrument, price: int, volume: int) -> None:
        """Called when a trade occurs.

        Where the aggressor order matches multiple passive orders, on_trade
        will be called only once for each different price level.
        """
        pass


class Order(object):
    """A request to buy or sell at a given price."""
    __slots__ = ("client_order_id", "instrument", "lifespan", "listener", "price", "remaining_volume", "side",
                 "total_fees", "volume")

    def __init__(self, client_order_id: int, instrument: Instrument, lifespan: Lifespan, side: Side, price: int,
                 volume: int, listener: Optional[IOrderListener] = None):
        """Initialise a new instance of the Order class."""
        self.client_order_id: int = client_order_id
        self.instrument: Instrument = instrument
        self.lifespan: Lifespan = lifespan
        self.side: Side = side
        self.price: int = price
        self.remaining_volume: int = volume
        self.total_fees: int = 0
        self.volume: int = volume
        self.listener: IOrderListener = listener

    def __str__(self):
        """Return a string containing a description of this order object."""
        args = (self.client_order_id, self.instrument, self.lifespan.name, self.side.name, self.price, self.volume,
                self.remaining_volume, self.total_fees)
        s = "{client_order_id=%d, instrument=%s, lifespan=%s, side=%s, price=%d, volume=%d, remaining=%d, "\
            "total_fees=%d}"
        return s % args


class Level(object):
    """A collection of orders with the same price arranged in the order they were inserted."""
    __slots__ = ("order_queue", "total_volume")

    def __init__(self):
        """Initialise a new instance of the Level class."""
        self.order_queue: Deque[Order] = collections.deque()
        self.total_volume: int = 0

    def __str__(self):
        """Return a string containing a description of this level object."""
        return "{order_count=%d, total_volume=%d}" % (len(self.order_queue), self.total_volume)


class TopLevels(object):
    """The top prices and their respective volumes from an order book."""
    __slots__ = ("ask_prices", "ask_volumes", "bid_prices", "bid_volumes")

    def __init__(self):
        """Initialise a new instance of the TopLevels class."""
        self.ask_prices: List[int] = [0] * TOP_LEVEL_COUNT
        self.ask_volumes: List[int] = [0] * TOP_LEVEL_COUNT
        self.bid_prices: List[int] = [0] * TOP_LEVEL_COUNT
        self.bid_volumes: List[int] = [0] * TOP_LEVEL_COUNT

    def __str__(self):
        """Return a string containing a description of this top-levels object."""
        args = (self.ask_prices, self.ask_volumes, self.bid_prices, self.bid_volumes)
        return "{ask_prices=%s, ask_volumes=%s, bid_prices=%s, bid_volumes=%s}" % args


class OrderBook(object):
    """A collection of orders arranged by the price-time priority principle."""

    def __init__(self, instrument: Instrument, listener: Optional[ITradeListener], maker_fee: float, taker_fee: float):
        """Initialise a new instance of the OrderBook class."""
        self.__ask_prices: List[int] = [-MAXIMUM_ASK]
        self.__bid_prices: List[int] = [MINIMUM_BID]
        self.__instrument: Instrument = instrument
        self.__last_traded_price: Optional[int] = None
        self.__levels: Dict[int, Level] = {MINIMUM_BID: Level(), MAXIMUM_ASK: Level()}
        self.__listener: Optional[ITradeListener] = listener
        self.__maker_fee: float = maker_fee
        self.__taker_fee: float = taker_fee

    def amend(self, now: float, order: Order, new_volume: int) -> None:
        """Amend an order in this order book by decreasing its volume."""
        if order.remaining_volume > 0:
            fill_volume = order.volume - order.remaining_volume
            diff = order.volume - (fill_volume if new_volume < fill_volume else new_volume)
            self.remove_volume_from_level(order.price, diff, order.side)
            order.volume -= diff
            order.remaining_volume -= diff
            if order.listener:
                order.listener.on_order_amended(now, order, diff)

    def best_ask(self) -> int:
        """Return the current best ask price."""
        return -self.__ask_prices[-1]

    def best_bid(self) -> int:
        """Return the current best bid price."""
        return self.__bid_prices[-1]

    def cancel(self, now: float, order: Order) -> None:
        """Cancel an order in this order book."""
        if order.remaining_volume > 0:
            self.remove_volume_from_level(order.price, order.remaining_volume, order.side)
            remaining = order.remaining_volume
            order.remaining_volume = 0
            if order.listener:
                order.listener.on_order_cancelled(now, order, remaining)

    def insert(self, now: float, order: Order) -> None:
        """Insert a new order into this order book."""
        if order.side == Side.SELL and order.price <= self.__bid_prices[-1]:
            self.trade_ask(now, order)
        elif order.side == Side.BUY and order.price >= self.__ask_prices[-1]:
            self.trade_bid(now, order)

        if order.remaining_volume > 0:
            if order.lifespan == Lifespan.FILL_AND_KILL:
                remaining = order.remaining_volume
                order.remaining_volume = 0
                if order.listener:
                    order.listener.on_order_cancelled(now, order, remaining)
            else:
                self.place(now, order)

    def last_traded_price(self) -> Optional[int]:
        """Return the last traded price."""
        return self.__last_traded_price

    def midpoint_price(self) -> int:
        """Return the midpoint price."""
        return round((self.__bid_prices[-1] - self.__ask_prices[-1]) / 2.0)

    def place(self, now: float, order: Order) -> None:
        """Place an order that does not match any existing order in this order book."""
        price = order.price

        if price not in self.__levels:
            level = self.__levels[price] = Level()
            if order.side == Side.SELL:
                insort_left(self.__ask_prices, -price)
            else:
                insort_left(self.__bid_prices, price)
        else:
            level = self.__levels[price]

        level.order_queue.append(order)
        level.total_volume += order.remaining_volume

        if order.listener:
            order.listener.on_order_placed(now, order)

    def remove_volume_from_level(self, price: int, volume: int, side: Side) -> None:
        level = self.__levels[price]

        if level.total_volume == volume:
            del self.__levels[price]
            if side == Side.SELL and price < MAXIMUM_ASK:
                self.__ask_prices.pop(bisect(self.__ask_prices, -price) - 1)
            elif side == Side.BUY and price > MINIMUM_BID:
                self.__bid_prices.pop(bisect(self.__bid_prices, price) - 1)
        else:
            level.total_volume -= volume

    def top_levels(self):
        """Return an instance of TopLevels for this order book."""
        result = TopLevels()

        i = 0
        j = len(self.__ask_prices) - 1
        while i < TOP_LEVEL_COUNT and j > 0:
            result.ask_prices[i] = -self.__ask_prices[j]
            result.ask_volumes[i] = self.__levels[result.ask_prices[i]].total_volume
            i += 1
            j -= 1

        i = 0
        j = len(self.__bid_prices) - 1
        while i < TOP_LEVEL_COUNT and j > 0:
            result.bid_prices[i] = self.__bid_prices[j]
            result.bid_volumes[i] = self.__levels[result.bid_prices[i]].total_volume
            i += 1
            j -= 1

        return result

    def trade_ask(self, now: float, order: Order) -> None:
        """Check to see if any existing bid orders match the specified ask order."""
        best_bid = self.__bid_prices[-1]
        level = self.__levels[best_bid]
        while order.remaining_volume > 0 and best_bid >= order.price and level.total_volume > 0:
            self.trade_level(now, order, level, best_bid)
            if level.total_volume == 0 and best_bid > MINIMUM_BID:
                del self.__levels[best_bid]
                self.__bid_prices.pop()
                best_bid = self.__bid_prices[-1]
                level = self.__levels[best_bid]

    def trade_bid(self, now: float, order: Order) -> None:
        """Check to see if any existing ask orders match the specified bid order."""
        best_ask = -self.__ask_prices[-1]
        level = self.__levels[best_ask]
        while order.remaining_volume > 0 and best_ask <= order.price and level.total_volume > 0:
            self.trade_level(now, order, level, best_ask)
            if level.total_volume == 0 and best_ask < MAXIMUM_ASK:
                del self.__levels[best_ask]
                self.__ask_prices.pop()
                best_ask = -self.__ask_prices[-1]
                level = self.__levels[best_ask]

    def trade_level(self, now: float, order: Order, level: Level, best_price: int) -> None:
        """Match the specified order with existing orders at the given level."""
        remaining: int = order.remaining_volume
        order_queue: Deque[Order] = level.order_queue
        total_volume: int = level.total_volume

        while remaining > 0 and total_volume > 0:
            while order_queue[0].remaining_volume == 0:
                order_queue.popleft()
            passive: Order = order_queue[0]
            volume: int = remaining if remaining < passive.remaining_volume else passive.remaining_volume
            fee: int = round(best_price * volume * self.__maker_fee)
            total_volume -= volume
            remaining -= volume
            passive.remaining_volume -= volume
            passive.total_fees += fee
            if passive.listener:
                passive.listener.on_order_filled(now, passive, best_price, volume, fee)

        level.total_volume = total_volume
        traded_volume_at_this_level: int = order.remaining_volume - remaining

        fee: int = round(best_price * traded_volume_at_this_level * self.__taker_fee)
        order.remaining_volume = remaining
        order.total_fees += fee
        if order.listener:
            order.listener.on_order_filled(now, order, best_price, traded_volume_at_this_level, fee)

        self.__last_traded_price = best_price
        if self.__listener:
            self.__listener.on_trade(self.__instrument, best_price, traded_volume_at_this_level)
