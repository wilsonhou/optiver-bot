import asyncio
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


class AutoTrader(BaseAutoTrader):
    def __init__(self, loop: asyncio.AbstractEventLoop):
        """Initialise a new instance of the AutoTrader class."""
        super(AutoTrader, self).__init__(loop)
        # counter tracks the sequence numbers and makes sure they're in order
        # check the on_order_book_update method to see usage
        self.counter = -1
        # modes store OF LENGTH 5
        self.ask_modes = []
        self.bid_modes = []
        # track theoretically optimal price
        self.theo_price = None
        # track open order_ids
        # ! make this a dictionary??? track the amount of seconds in it
        self.open_ask_ids = []
        self.open_bid_ids = []

        # hoya's stuff idk
        self.ask_id = 0
        self.ask_price = 0
        self.bid_id = 0
        self.bid_price = 0
        # self.position = 0
        # self.future_position = 0

    def on_error_message(self, client_order_id: int, error_message: bytes) -> None:
        """Called when the exchange detects an error.

        If the error pertains to a particular order, then the client_order_id
        will identify that order, otherwise the client_order_id will be zero.
        """
        # e.g:
        # on_error_message(34, )
        pass

    def on_order_book_update_message(self, instrument: int, sequence_number: int, ask_prices: List[int],
                                     ask_volumes: List[int], bid_prices: List[int], bid_volumes: List[int]) -> None:
        """Called periodically to report the status of an order book.

        The sequence number can be used to detect missed or out-of-order
        messages. The five best available ask (i.e. sell) and bid (i.e. buy)
        prices are reported along with the volume available at each of those
        price levels.
        """
        # TODO: complete the comment tasks and go through logic
        # check instrument is of correct type (0 or 1)
        if instrument == Instrument.ETF:
            # check that seq number isn't out of order with internal counter
            if sequence_number >= self.counter:
                # update counter to cur seq number
                self.counter = sequence_number

                # find the modes
                ask_mode = ask_prices[ask_volumes.index(max(ask_volumes))]
                bid_mode = bid_prices[bid_volumes.index(max(bid_volumes))]

                # check if its the right price
                # if length is less than 5 do jack shit
                if len(self.ask_modes) < 5:
                    self.ask_modes.append(ask_mode)
                    self.bid_modes.append(bid_mode)
                    return
                else:
                    self.ask_modes.append(ask_mode)
                    self.bid_modes.append(bid_mode)
                    self.ask_modes.pop(0)
                    self.bid_modes.pop(0)

                # find the average gradient of the 5
                ask_gradient = sum(numpy.gradient(
                    self.ask_modes)) / len(self.ask_modes)
                bid_gradient = sum(numpy.gradient(
                    self.bid_modes)) / len(self.bid_modes)

                # FIND OPTIMAL PRICES
                # gradient standardised between 0 and 1 * lowest ask or highest bid = P(choosing that as optimal price)
                # SET OPTIMAL PRICE
                # ORDERS SENT PER TICK (set GFDs)

                self.logger.info(
                    f"gradients a/b: {ask_gradient}, {bid_gradient}, ask mode is: {ask_mode}, bid mode is: {bid_mode}")

        # self.logger.info(
        #     f"""OB_UPDATE. instrument: {instrument} seq_num: {sequence_number} ask_prices/volume: {ask_prices}, {ask_volumes}
        #     bid_p/v: {bid_prices}, {bid_volumes}""")
        pass

    def on_order_status_message(self, client_order_id: int, fill_volume: int, remaining_volume: int, fees: int) -> None:
        """Called when the status of one of your orders changes.

        The fill_volume is the number of lots already traded, remaining_volume
        is the number of lots yet to be traded and fees is the total fees for
        this order. Remember that you pay fees for being a market taker, but
        you receive fees for being a market maker, so fees can be negative.

        If an order is cancelled its remaining volume will be zero.
        """
        pass

    def on_position_change_message(self, future_position: int, etf_position: int) -> None:
        """Called when your position changes.

        Since every trade in the ETF is automatically hedged in the future,
        future_position and etf_position will always be the inverse of each
        other (i.e. future_position == -1 * etf_position).
        """

        pass

    def on_trade_ticks_message(self, instrument: int, trade_ticks: List[Tuple[int, int]]) -> None:
        """Called periodically to report trading activity on the market.

        Each trade tick is a pair containing a price and the number of lots
        traded at that price since the last trade ticks message.
        """
        self.logger.info(
            f"TRADE_TICKS. instrument: {instrument} tt: {trade_ticks}")
        # pass
