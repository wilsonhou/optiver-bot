import asyncio
import logging

from typing import Optional

from .competitor import Competitor
from .limiter import FrequencyLimiter
from .market_events import MarketEvents
from .messages import *
from .types import IExecutionChannel, IController


class ExecutionChannel(asyncio.Protocol, IExecutionChannel):
    def __init__(self, loop: asyncio.AbstractEventLoop, controller: IController, market_events: MarketEvents,
                 frequency_limiter: FrequencyLimiter, speed: float):
        """Initialise a new instance of the ExecutionChannel class."""
        self.competitor: Optional[Competitor] = None
        self.controller: IController = controller
        self.closing: bool = False
        self.data: bytes = b""
        self.event_loop: asyncio.AbstractEventLoop = loop
        self.file_number: int = -1
        self.frequency_limiter: FrequencyLimiter = frequency_limiter
        self.logger: logging.Logger = logging.getLogger("EXECUTION")
        self.login_timeout: asyncio.Handle = loop.call_later(1.0, self.close)
        self.market_events: MarketEvents = market_events
        self.name: Optional[str] = None
        self.transport: Optional[asyncio.Transport] = None
        self.speed: float = speed
        self.start_time: float = 0.0

        self.account_message: bytearray = bytearray(POSITION_CHANGE_MESSAGE_SIZE)
        self.error_message: bytearray = bytearray(ERROR_MESSAGE_SIZE)
        self.order_message: bytearray = bytearray(ORDER_STATUS_MESSAGE_SIZE)

        HEADER.pack_into(self.error_message, 0, ERROR_MESSAGE_SIZE, MessageType.ERROR)
        HEADER.pack_into(self.account_message, 0, POSITION_CHANGE_MESSAGE_SIZE, MessageType.POSITION_CHANGE)
        HEADER.pack_into(self.order_message, 0, ORDER_STATUS_MESSAGE_SIZE, MessageType.ORDER_STATUS)

    def __del__(self):
        """Clean up this instance of the ExecutionChannel class."""
        self.login_timeout.cancel()

    def close(self):
        """Close the connection associated with this ExecutionChannel instance."""
        self.login_timeout.cancel()
        self.closing = True
        if not self.transport.is_closing():
            self.transport.close()

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        """Called when a connection is established with the auto-trader."""
        sock = transport.get_extra_info("socket")
        peername = transport.get_extra_info("peername")
        if sock is not None:
            self.file_number = sock.fileno()
        self.logger.info("fd=%d accepted a new connection: peer=%s:%d", self.file_number, *(peername or ("unknown", 0)))
        self.transport = transport

    def connection_lost(self, exc: Optional[Exception]) -> None:
        """Called when the connection to the auto-trader is lost."""
        elapsed: float = (self.event_loop.time() - self.start_time) * self.speed if self.start_time else 0.0
        if self.competitor is not None:
            self.competitor.on_connection_lost(elapsed)
        self.controller.on_connection_lost(self.competitor.name if self.competitor else None)
        if not self.closing:
            self.logger.warning("fd=%d lost connection to auto-trader at time=%.3f:", self.file_number, elapsed,
                                exc_info=exc)

    def data_received(self, data: bytes) -> None:
        """Called when data is received from the auto-trader."""
        if self.data:
            self.data += data
        else:
            self.data = data

        elapsed: float = 0.0
        upto: int = 0
        data_length: int = len(self.data)
        fileno: int = self.file_number
        name: str = self.name

        while not self.closing and upto < data_length - HEADER_SIZE:
            length, typ = HEADER.unpack_from(self.data, upto)
            if upto + length > data_length:
                break

            if self.start_time:
                elapsed: float = (self.event_loop.time() - self.start_time) * self.speed
                self.market_events.process_market_events(elapsed)

            if self.frequency_limiter.check_event(elapsed):
                self.logger.info("fd=%d message frequency limit breached: now=%.6f value=%d limit=%d",
                                 fileno, elapsed, self.frequency_limiter.value, self.frequency_limiter.limit)
                if self.competitor is not None:
                    self.competitor.hard_breach(elapsed, 0, b"message frequency limit breached")
                else:
                    self.close()
                return

            if self.competitor is None and typ != MessageType.LOGIN:
                self.logger.info("fd=%d first message received was not a login", fileno)
                self.close()
                return

            if typ == MessageType.AMEND_ORDER and length == AMEND_MESSAGE_SIZE:
                coi, vol = AMEND_MESSAGE.unpack_from(self.data, upto + HEADER_SIZE)
                self.logger.debug("fd=%d '%s' received amend: time=%.6f client_order_id=%d volume=%d", fileno,
                                  name, elapsed, coi, vol)
                self.competitor.on_amend_message(elapsed, coi, vol)
            elif typ == MessageType.CANCEL_ORDER and length == CANCEL_MESSAGE_SIZE:
                coi, = CANCEL_MESSAGE.unpack_from(self.data, upto + HEADER_SIZE)
                self.logger.debug("fd=%d '%s' received cancel: time=%.6f client_order_id=%d", fileno, name,
                                  elapsed, coi)
                self.competitor.on_cancel_message(elapsed, coi)
            elif typ == MessageType.INSERT_ORDER and length == INSERT_MESSAGE_SIZE:
                coi, side, prc, vol, life = INSERT_MESSAGE.unpack_from(self.data, upto + HEADER_SIZE)
                self.logger.debug("fd=%d '%s' received insert: time=%.6f client_order_id=%d side=%d price=%d"
                                  " volume=%d lifespan=%d", fileno, name, elapsed, coi, side, prc, vol, life)
                self.competitor.on_insert_message(elapsed, coi, side, prc, vol, life)
            elif typ == MessageType.LOGIN and length == LOGIN_MESSAGE_SIZE:
                raw_name, raw_secret = LOGIN_MESSAGE.unpack_from(self.data, upto + HEADER_SIZE)
                self.on_login(raw_name.rstrip(b"\x00").decode(), raw_secret.rstrip(b"\x00").decode())
                name = self.name
            else:
                self.logger.info("fd=%d '%s' received invalid message: time=%.6f length=%d type=%d", fileno, name,
                                 elapsed, length, typ)
                self.close()
                return

            upto += length

        self.data = self.data[upto:]

    def on_login(self, name: str, secret: str) -> None:
        """Called when a login message is received."""
        self.login_timeout.cancel()

        if self.competitor is not None:
            self.logger.info("fd=%d received second login message: name='%s'", self.file_number, name)
            self.close()
            return

        self.competitor = self.controller.get_competitor(name, secret, self)
        if self.competitor is None:
            self.logger.info("fd=%d login failed: name='%s'", self.file_number, name)
            self.close()
            return

        self.logger.info("fd=%d login successful: name='%s'", self.file_number, name)
        self.name = name

    def send_error(self, client_order_id: int, error_message: bytes) -> None:
        """Send an error message to the auto-trader."""
        ERROR_MESSAGE.pack_into(self.error_message, HEADER_SIZE, client_order_id, error_message)
        self.transport.write(self.error_message)

    def send_order_status(self, client_order_id: int, fill_volume: int, remaining_volume: int, fees: int) -> None:
        """Send an order status message to the auto-trader."""
        ORDER_STATUS_MESSAGE.pack_into(self.order_message, HEADER_SIZE, client_order_id, fill_volume, remaining_volume,
                                       fees)
        self.transport.write(self.order_message)

    def send_position_change(self, future_position: int, etf_position: int) -> None:
        """Send a position change message to the auto-trader."""
        POSITION_CHANGE_MESSAGE.pack_into(self.account_message, HEADER_SIZE, future_position, etf_position)
        self.transport.write(self.account_message)

    def set_start_time(self, start_time: float) -> None:
        """Set the start time of the match."""
        self.start_time = start_time
