import asyncio
import logging
from typing import ItemsView, Iterator, List, Optional, Tuple

from .messages import *


MAX_DATAGRAM_SIZE = 508
MAX_TRADE_TICKS = (MAX_DATAGRAM_SIZE - TRADE_TICKS_HEADER_SIZE) // TRADE_TICK_SIZE


class InformationChannel(asyncio.DatagramProtocol):
    def __init__(self, remote_address: Optional[Tuple[str, int]] = None):
        """Initialize a new instance of the InformationChannel class."""
        self.remote_address: Optional[Tuple[str, int]] = remote_address
        self.transport: Optional[asyncio.DatagramTransport] = None

        self.logger: logging.Logger = logging.getLogger("INFORMATION")

        self.book_message = bytearray(ORDER_BOOK_MESSAGE_SIZE)
        HEADER.pack_into(self.book_message, 0, ORDER_BOOK_MESSAGE_SIZE, MessageType.ORDER_BOOK_UPDATE)

        self.ticks_message = bytearray(MAX_DATAGRAM_SIZE)

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        """Called when the datagram endpoint is created."""
        self.transport = transport

    def send_order_book_update(self, instrument: int, sequence_number: int, ask_prices: List[int],
                               ask_volumes: List[int], bid_prices: List[int], bid_volumes: List[int]) -> None:
        """Send an order book update message to the auto-trader."""
        ORDER_BOOK_HEADER.pack_into(self.book_message, HEADER_SIZE, instrument, sequence_number)
        ORDER_BOOK_MESSAGE.pack_into(self.book_message, ORDER_BOOK_HEADER_SIZE, *ask_prices, *ask_volumes, *bid_prices,
                                     *bid_volumes)
        self.transport.sendto(self.book_message, self.remote_address)

    def send_trade_ticks(self, instrument: int, trade_ticks: ItemsView[int, int]) -> None:
        """Send a trade ticks message to the auto-trader."""
        message = self.ticks_message

        count = len(trade_ticks)
        if count > MAX_TRADE_TICKS:
            count = MAX_TRADE_TICKS

        offset: int = TRADE_TICKS_HEADER_SIZE
        size: int = offset + count * TRADE_TICK_SIZE

        HEADER.pack_into(message, 0, size, MessageType.TRADE_TICKS)
        TRADE_TICKS_HEADER.pack_into(message, HEADER_SIZE, instrument)

        ticks: Iterator[Tuple[int, int]] = iter(trade_ticks)
        while offset < size:
            TRADE_TICK.pack_into(message, offset, *next(ticks))
            offset += TRADE_TICK_SIZE

        self.transport.sendto(message[:size], self.remote_address)
