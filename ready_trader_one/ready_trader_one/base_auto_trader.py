import asyncio
import logging
import struct

from typing import List, Optional, Text, Tuple, Union

from .messages import *
from .order_book import TOP_LEVEL_COUNT
from .types import Lifespan, Side

BOOK_PART = struct.Struct("!%dI" % (TOP_LEVEL_COUNT,))


class BaseAutoTrader(asyncio.Protocol, asyncio.DatagramProtocol):
    """Base class for an auto-trader."""

    def __init__(self, loop: asyncio.AbstractEventLoop):
        """Initialise a new instance of the BaseTraderProtocol class."""
        self.event_loop: asyncio.AbstractEventLoop = loop
        self.execution: Optional[asyncio.Transport] = None
        self.information: Optional[asyncio.DatagramTransport] = None
        self.logger = logging.getLogger("TRADER")
        self.team_name: Optional[bytes] = None
        self.secret: Optional[bytes] = None

        # Subclasses shouldn't try to read _data directly.
        self._data: bytes = b""

        self.amend_message: bytearray = bytearray(AMEND_MESSAGE_SIZE)
        self.cancel_message: bytearray = bytearray(CANCEL_MESSAGE_SIZE)
        self.insert_message: bytearray = bytearray(INSERT_MESSAGE_SIZE)

        HEADER.pack_into(self.amend_message, 0, AMEND_MESSAGE_SIZE, MessageType.AMEND_ORDER)
        HEADER.pack_into(self.cancel_message, 0, CANCEL_MESSAGE_SIZE, MessageType.CANCEL_ORDER)
        HEADER.pack_into(self.insert_message, 0, INSERT_MESSAGE_SIZE, MessageType.INSERT_ORDER)

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        """Called twice, when the execution channel and the information channel are established."""
        pass

    def connection_lost(self, exc: Optional[Exception]) -> None:
        """Called when the connection is lost on the execution channel."""
        if exc is not None:
            self.logger.error("lost connection on execution channel:", exc_info=exc)
        else:
            self.logger.info("lost connection on execution channel")
        self.execution.close()
        self.execution = None
        self.event_loop.stop()

    def data_received(self, data: bytes) -> None:
        """Called when data is received from the matching engine."""
        if self._data:
            self._data += data
        else:
            self._data = data

        upto = 0
        while upto < len(self._data) - HEADER_SIZE:
            length, typ = HEADER.unpack_from(self._data, upto)
            if upto + length > len(self._data):
                break
            if typ == MessageType.ERROR and length == ERROR_MESSAGE_SIZE:
                client_order_id, error_message = ERROR_MESSAGE.unpack_from(self._data, upto + HEADER_SIZE)
                self.on_error_message(client_order_id, error_message.rstrip(b"\x00"))
            elif typ == MessageType.ORDER_STATUS and length == ORDER_STATUS_MESSAGE_SIZE:
                self.on_order_status_message(*ORDER_STATUS_MESSAGE.unpack_from(self._data, upto + HEADER_SIZE))
            elif typ == MessageType.POSITION_CHANGE and length == POSITION_CHANGE_MESSAGE_SIZE:
                self.on_position_change_message(*POSITION_CHANGE_MESSAGE.unpack_from(self._data, upto + HEADER_SIZE))
            else:
                self.logger.error("received invalid execution message: length=%d type=%d", length, typ)
                self.event_loop.stop()
                return
            upto += length
        self._data = self._data[upto:]

    def datagram_received(self, data: Union[bytes, Text], addr: Tuple[str, int]) -> None:
        """Called when data is received from the matching engine."""
        if len(data) < HEADER_SIZE:
            self.logger.error("received malformed datagram: length=%d", len(data))
            self.event_loop.stop()
            return

        length, typ = HEADER.unpack_from(data)
        if length != len(data):
            self.logger.error("received malformed datagram: specified_length=%d actual_length=%d", length, len(data))
            self.event_loop.stop()
            return

        if typ == MessageType.ORDER_BOOK_UPDATE and length == ORDER_BOOK_MESSAGE_SIZE:
            inst, seq = ORDER_BOOK_HEADER.unpack_from(data, HEADER_SIZE)
            self.on_order_book_update_message(inst, seq, *BOOK_PART.iter_unpack(data[ORDER_BOOK_HEADER_SIZE:]))
        elif typ == MessageType.TRADE_TICKS and (length - TRADE_TICKS_HEADER_SIZE) % TRADE_TICK_SIZE == 0:
            inst, = TRADE_TICKS_HEADER.unpack_from(data, HEADER_SIZE)
            ticks = list(TRADE_TICK.iter_unpack(data[TRADE_TICKS_HEADER_SIZE:]))
            self.on_trade_ticks_message(inst, ticks)
        else:
            self.logger.error("received invalid information message: length=%d type=%d", length, typ)
            self.event_loop.stop()

    def on_position_change_message(self, future_position: int, etf_position: int) -> None:
        """Called when your position changes.

        Two pieces of information are reported: 1. your current position in
        the future; and 2. your current position in the ETF.
        """
        pass

    def on_error_message(self, client_order_id: int, error_message: bytes):
        """Called when the matching engine detects an error."""
        pass

    def on_order_book_update_message(self, instrument: int, sequence_number: int, ask_prices: List[int],
                                     ask_volumes: List[int], bid_prices: List[int], bid_volumes: List[int]) -> None:
        """Called periodically to report the status of the order book.

        The sequence number can be used to detect missed messages. The best
        available ask (i.e. sell) and bid (i.e. buy) prices are reported along
        with the volume available at each of those price levels.
        """
        pass

    def on_order_status_message(self, client_order_id: int, fill_volume: int, remaining_volume: int, fees: int):
        """Called when the status of one of your orders changes.

        The fill_volume is the number of lots already traded, remaining_volume
        is the number of lots yet to be traded and fees is the total fees paid
        or received for this order.

        Remaining volume will be set to zero if the order is cancelled.
        """
        pass

    def on_trade_ticks_message(self, instrument: int, trade_ticks: List[Tuple[int, int]]) -> None:
        """Called periodically to report trading activity on the market.

        Each trade tick is a pair containing a price and the volume traded at
        that price level since the last trade ticks message.
        """
        pass

    def send_amend_order(self, client_order_id: int, volume: int) -> None:
        """Amend the specified order with an updated volume.

        The specified volume must be no greater than the original volume for
        the order. If the order has already completely filled or been
        cancelled this request has no effect and no order status message will
        be received.
        """
        if self.execution:
            AMEND_MESSAGE.pack_into(self.amend_message, HEADER_SIZE, client_order_id, volume)
            self.execution.write(self.amend_message)

    def send_cancel_order(self, client_order_id: int) -> None:
        """Cancel the specified order.

        If the order has already completely filled or been cancelled this
        request has no effect and no order status message will be received.
        """
        if self.execution:
            CANCEL_MESSAGE.pack_into(self.cancel_message, HEADER_SIZE, client_order_id)
            self.execution.write(self.cancel_message)

    def send_insert_order(self, client_order_id: int, side: Side, price: int, volume: int, lifespan: Lifespan) -> None:
        """Insert a new order into the market."""
        if self.execution:
            INSERT_MESSAGE.pack_into(self.insert_message, HEADER_SIZE, client_order_id, side, price, volume, lifespan)
            self.execution.write(self.insert_message)

    def set_team_name(self, team_name: str, secret: str) -> None:
        """Set the team name for this auto-trader"""
        self.team_name = team_name.encode()
        self.secret = secret.encode()

    def set_transports(self, execution: asyncio.Transport, information: asyncio.DatagramTransport) -> None:
        """Set the asyncio transports to be used for this auto-trader."""
        self.execution = execution
        self.information = information
        self.execution.write(HEADER.pack(LOGIN_MESSAGE_SIZE, MessageType.LOGIN)
                             + LOGIN_MESSAGE.pack(self.team_name, self.secret))
