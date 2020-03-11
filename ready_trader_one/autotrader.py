import asyncio
import itertools
import numpy

from typing import List, Tuple

from ready_trader_one import BaseAutoTrader, Instrument, Lifespan, Side

"""
OPTIONS FOR INSTRUMENTS: (in ascending order of count)
    Instrument: use .FUTURE or .ETF
    Lifespan: use .FILL_AND_KILL or .GOOD_FOR_DAY
    Side: use .SELL or .BUY

METHODS WE CAN AND SHOULD USE:
    self.

    send_amend_order(self, client_order_id: int, volume: int)

    send_cancel_order(self, client_order_id: int)

    send_insert_order(self, client_order_id: int, side: Side, price: int, volume: int, lifespan: Lifespan)

    """

# TODO: GET RID OF RETARDED COMMENTS - INCLUDING THIS ONE!!!
# TODO: FINISH METHODS: on_order_book_update_message, on_order_status_message, on_position_change_message, on_trade_ticks_message


class AutoTrader(BaseAutoTrader):
    def __init__(self, loop: asyncio.AbstractEventLoop):
        """Initialise a new instance of the AutoTrader class."""
        super(AutoTrader, self).__init__(loop)
        # initialise some more variables, such as an internal id counter
        self.order_ids = itertools.count(1)
        self.sequence_count = -1
        self.pending_bid = None
        self.pending_bid_id = 0
        self.pending_ask = None
        self.pending_ask_id = 0

        # Initialising variables
        # Don't track future position since its just negative ETF position
        # TODO: REFACTOR ASK ID AND BID ID OUT
        self.ask_id = self.ask_price = self.bid_id = self.bid_price = self.position = self.weighted_price = 0

    def on_error_message(self, client_order_id: int, error_message: bytes) -> None:
        """Called when the exchange detects an error.

        If the error pertains to a particular order, then the client_order_id
        will identify that order, otherwise the client_order_id will be zero.
        """
        # just log some shit lol
        self.logger.warning("error with order %d: %s",
                            client_order_id, error_message.decode())
        self.on_order_status_message(client_order_id, 0, 0, 0)

    def on_order_book_update_message(self, instrument: int, sequence_number: int, ask_prices: List[int],
                                     ask_volumes: List[int], bid_prices: List[int], bid_volumes: List[int]) -> None:
        """Called periodically to report the status of an order book.

        The sequence number can be used to detect missed or out-of-order
        messages. The five best available ask (i.e. sell) and bid (i.e. buy)
        prices are reported along with the volume available at each of those
        price levels.
        """

        if instrument == Instrument.ETF and sequence_number > self.sequence_count:
            # make sure this is in right order
            sequence_number = self.sequence_count

            # find the current middle market price through a weighted average (TODO: REFACTOR THIS INTO MICRO PRICING)
            # * MAKE SURE THIS IS NON ZERO!!!
            # formulas linked in notes.md
            if (bid_volumes[0] + ask_volumes[0]) == 0:
                return
            imbalance = bid_volumes[0] / (bid_volumes[0] + ask_volumes[0])
            self.weighted_price = imbalance * \
                ask_prices[0] + (1 - imbalance) * bid_prices[0]

            # calculate the spread that we need (TODO: REFACTOR THIS INTO AN ACTUAL ALGORITHM, NOT HARDCODED)
            # right now this is slightly bigger than the taker's fee, 0.015 on both sides. default: 0.03 / 2
            ask_spread = bid_spread = 0.01 / 2  # TODO: LIMIT THIS TO 1 TICK MINIMUM

            # TODO: adjust the spread based on current and target position (multiply by a percentage)

            # TODO: calculate ask volume based on percentage
            ask_volume = bid_volume = 1

            # calculate prices
            bid_price = round(self.weighted_price * (
                1 - bid_spread)) // 100 * 100
            ask_price = round(self.weighted_price * (
                1 + ask_spread)) // 100 * 100

            # aggregate orders and put on pending
            self.pending_bid = (Side.BUY,
                                bid_price, bid_volume, Lifespan.GOOD_FOR_DAY)
            self.pending_ask = (Side.SELL,
                                ask_price, bid_volume, Lifespan.GOOD_FOR_DAY)

        else:
            # WHAT IF ITS A FUTURE???
            pass

        pass

    def on_order_status_message(self, client_order_id: int, fill_volume: int, remaining_volume: int, fees: int) -> None:
        """Called when the status of one of your orders changes.

        The fill_volume is the number of lots already traded, remaining_volume
        is the number of lots yet to be traded and fees is the total fees for
        this order. Remember that you pay fees for being a market taker, but
        you receive fees for being a market maker, so fees can be negative.

        If an order is cancelled its remaining volume will be zero.
        """
        # update the current orders

        # check if the order is a cancel order
        if (client_order_id == self.ask_id or client_order_id == self.bid_id):
            # update ask and bid ids
            self.ask_id = self.pending_ask_id
            self.bid_id = self.pending_bid_id

    def on_position_change_message(self, future_position: int, etf_position: int) -> None:
        """Called when your position changes.

        Since every trade in the ETF is automatically hedged in the future,
        future_position and etf_position will always be the inverse of each
        other (i.e. future_position == -1 * etf_position).

        Goal position is 0.
        """
        # update position
        self.position = etf_position

        # TODO: calculate skewness of spread

    def on_trade_ticks_message(self, instrument: int, trade_ticks: List[Tuple[int, int]]) -> None:
        """Called periodically to report trading activity on the market.

        Each trade tick is a pair containing a price and the number of lots
        traded at that price since the last trade ticks message.
        """
        # TODO: CHECK LOGIC
        self.logger.info(self.pending_ask)

        # short circuit if None type
        if self.pending_ask is None or self.pending_bid is None:
            return

        # short circuit if no new pending id
        # if self.pending_ask_id == self.ask_id or self.pending_bid_id == self.bid_id:
        #     return
        # check current orders

        # place orders

        self.logger.info(self.pending_ask)

        # cancel orders
        self.send_cancel_order(self.ask_id)
        self.send_cancel_order(self.bid_id)

        self.send_insert_order(next(self.order_ids), *self.pending_ask)
        self.send_insert_order(next(self.order_ids), *self.pending_bid)
