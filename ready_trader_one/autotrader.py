import asyncio
import numpy

from typing import List, Tuple

from ready_trader_one import BaseAutoTrader, Instrument, Lifespan, Side

# TODO: GET RID OF RETARDED COMMENTS - INCLUDING THIS ONE!!!

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
        # initialise some more variables, such as an internal id counter

        # Initialising variables
        # * Don't track future position since its just negative ETF position
        self.ask_id = self.ask_price = self.bid_id = self.bid_price = self.position = 0

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

        if instrument == Instrument.ETF:
            # find the current middle market price through a weighted average

            # calculate the spread that we need

            # adjust the spread based on current and target position (multiply by a percentage)

            # aggregate order and put them on orders to post
            pass
        else:
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

        pass

    def on_position_change_message(self, future_position: int, etf_position: int) -> None:
        """Called when your position changes.

        Since every trade in the ETF is automatically hedged in the future,
        future_position and etf_position will always be the inverse of each
        other (i.e. future_position == -1 * etf_position).
        """
        # update position

        # calculate skewness of spread

        self.position = etf_position

    def on_trade_ticks_message(self, instrument: int, trade_ticks: List[Tuple[int, int]]) -> None:
        """Called periodically to report trading activity on the market.

        Each trade tick is a pair containing a price and the number of lots
        traded at that price since the last trade ticks message.
        """
        # check current orders

        # check pending orders

        # cancel previous orders and place pending orders
        pass
