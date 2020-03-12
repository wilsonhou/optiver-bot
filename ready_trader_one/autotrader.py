import asyncio
import itertools

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

        # Initialising variables
        # Don't track future position since its just negative ETF position
        # TODO: REFACTOR ASK ID AND BID ID OUT
        self.ask_id = self.ask_price = self.ask_spread = self.bid_id = self.bid_price = self.bid_spread = self.position = self.true_price = 0

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
            if (bid_volumes[0] + ask_volumes[0]) == 0:
                return
            imbalance = bid_volumes[0] / (bid_volumes[0] + ask_volumes[0])
            self.true_price = imbalance * \
                ask_prices[0] + (1 - imbalance) * bid_prices[0]

            # calculate the spread that we need (TODO: REFACTOR THIS INTO AN ACTUAL ALGORITHM, NOT HARDCODED)

            # make the spread the max of bid
            ask_spread = self.ask_spread
            bid_spread = self.bid_spread

            # TODO: calculate volume based on percentage
            ask_volume = bid_volume = 1

            # calculate prices
            bid_price = round(self.true_price -
                              bid_spread) // 100 * 100 - self.position * 100
            ask_price = round(self.true_price +
                              ask_spread) // 100 * 100 - self.position * 100

            # check that price is different to current price
            if self.bid_id != 0 and bid_price not in (self.bid_price, 0):
                self.send_cancel_order(self.bid_id)
                self.bid_id = 0
            if self.ask_id != 0 and ask_price not in (self.ask_price, 0):
                self.send_cancel_order(self.ask_id)
                self.ask_id = 0

            # aggregate orders and send
            if self.bid_id == 0 and bid_price != 0 and self.position < 100:
                # * rename into new_bid_price
                self.bid_id = next(self.order_ids)
                self.bid_price = bid_price
                self.send_insert_order(self.bid_id, Side.BUY,
                                       bid_price, bid_volume, Lifespan.GOOD_FOR_DAY)

            if self.ask_id == 0 and ask_price != 0 and self.position > -100:
                self.ask_id = next(self.order_ids)
                self.ask_price = ask_price
                self.send_insert_order(self.ask_id, Side.SELL,
                                       ask_price, bid_volume, Lifespan.GOOD_FOR_DAY)

        elif instrument == Instrument.FUTURE:
            # Find the optimal spread based on the future!!!

            # ! THIS STRATEGY IS JUST SPECULATED, IMPROVE THIS IN NEXT VERSION PLEASE
            if 0 not in bid_prices and 0 not in ask_prices and self.true_price != 0:

                # our spread is the minimum of the future spreads
                self.bid_spread = self.ask_spread = max(
                    self.true_price - bid_prices[0], ask_prices[0] - self.true_price)
                self.logger.info(f"UPDATED SPREAD: {self.bid_spread}")
        else:
            self.logger.warning(f"OUT OF ORDER: {sequence_number}")

    def on_order_status_message(self, client_order_id: int, fill_volume: int, remaining_volume: int, fees: int) -> None:
        """Called when the status of one of your orders changes.

        The fill_volume is the number of lots already traded, remaining_volume
        is the number of lots yet to be traded and fees is the total fees for
        this order. Remember that you pay fees for being a market taker, but
        you receive fees for being a market maker, so fees can be negative.

        If an order is cancelled its remaining volume will be zero.
        """
        # update the current orders
        # TODO: MAKE THIS REFILL ORDERS!!!

        # check if the order is a cancel order
        if remaining_volume == 0:
            if client_order_id == self.bid_id:
                self.bid_id = 0
            elif client_order_id == self.ask_id:
                self.ask_id = 0

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
        # TODO: CHECK IF THE CURRENT TRADING PRICE IS A BIT DODGY
        self.logger.info(
            f"instrument: {instrument}, trade_ticks: {trade_ticks}")

        # # short circuit if None type
        # if self.pending_ask is None or self.pending_bid is None:
        #     return

        # # short circuit if no new pending id
        # # ! IDK FIX THIS
        # # if self.pending_ask_id == self.ask_id or self.pending_bid_id == self.bid_id:
        # #     return
        # # check current orders

        # # place orders

        # self.logger.info(self.pending_ask)

        # # cancel orders
        # self.send_cancel_order(self.ask_id)
        # self.send_cancel_order(self.bid_id)

        # self.send_insert_order(next(self.order_ids), *self.pending_ask)
        # self.send_insert_order(next(self.order_ids), *self.pending_bid)
