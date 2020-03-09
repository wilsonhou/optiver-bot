import enum
import struct

from .order_book import TOP_LEVEL_COUNT

__all__ = ("MessageType", "HEADER", "AMEND_MESSAGE", "CANCEL_MESSAGE", "INSERT_MESSAGE", "ERROR_MESSAGE",
           "LOGIN_MESSAGE", "POSITION_CHANGE_MESSAGE", "ORDER_BOOK_HEADER", "ORDER_BOOK_MESSAGE",
           "ORDER_STATUS_MESSAGE", "TRADE_TICKS_HEADER", "TRADE_TICK", "HEADER_SIZE", "AMEND_MESSAGE_SIZE",
           "CANCEL_MESSAGE_SIZE", "INSERT_MESSAGE_SIZE", "ERROR_MESSAGE_SIZE", "LOGIN_MESSAGE_SIZE",
           "POSITION_CHANGE_MESSAGE_SIZE", "ORDER_BOOK_HEADER_SIZE", "ORDER_BOOK_MESSAGE_SIZE",
           "ORDER_STATUS_MESSAGE_SIZE", "TRADE_TICKS_HEADER_SIZE", "TRADE_TICK_SIZE")


class MessageType(enum.IntEnum):
    AMEND_ORDER = 1
    CANCEL_ORDER = 2
    ERROR = 3
    INSERT_ORDER = 4
    LOGIN = 5
    ORDER_BOOK_UPDATE = 6
    ORDER_STATUS = 7
    POSITION_CHANGE = 8
    TRADE_TICKS = 10


# Standard message header: message length (2 bytes) and type (1 byte)
HEADER = struct.Struct("!HB")  # Length, message type

# Auto-trader to matching engine messages
AMEND_MESSAGE = struct.Struct("!II")  # Client order id and new volume
CANCEL_MESSAGE = struct.Struct("!I")  # Client order id
INSERT_MESSAGE = struct.Struct("!IBIIB")  # Client order id, side, price, volume and lifespan
LOGIN_MESSAGE = struct.Struct("!20s50s")  # Name

# Matching engine to auto-trader messages
ERROR_MESSAGE = struct.Struct("!I50s")  # message
ORDER_BOOK_HEADER = struct.Struct("!BI")  # Instrument and sequence number
ORDER_BOOK_MESSAGE = struct.Struct("!%dI" % (4 * TOP_LEVEL_COUNT,))  # Ask prices & volumes and bid prices & volumes
ORDER_STATUS_MESSAGE = struct.Struct("!IIIi")  # Client order id, fill volume, remaining volume and fees
POSITION_CHANGE_MESSAGE = struct.Struct("!ii")  # Future position and ETF position
TRADE_TICKS_HEADER = struct.Struct("!B")  # Instrument
TRADE_TICK = struct.Struct("!II")  # Price and volume

# Cumulative message sizes
HEADER_SIZE: int = HEADER.size

AMEND_MESSAGE_SIZE: int = HEADER.size + AMEND_MESSAGE.size
CANCEL_MESSAGE_SIZE: int = HEADER.size + CANCEL_MESSAGE.size
INSERT_MESSAGE_SIZE: int = HEADER.size + INSERT_MESSAGE.size
LOGIN_MESSAGE_SIZE: int = HEADER.size + LOGIN_MESSAGE.size

ERROR_MESSAGE_SIZE: int = HEADER.size + ERROR_MESSAGE.size
ORDER_BOOK_HEADER_SIZE: int = HEADER.size + ORDER_BOOK_HEADER.size
ORDER_BOOK_MESSAGE_SIZE: int = ORDER_BOOK_HEADER_SIZE + ORDER_BOOK_MESSAGE.size
ORDER_STATUS_MESSAGE_SIZE: int = HEADER.size + ORDER_STATUS_MESSAGE.size
POSITION_CHANGE_MESSAGE_SIZE: int = HEADER.size + POSITION_CHANGE_MESSAGE.size
TRADE_TICKS_HEADER_SIZE: int = HEADER.size + TRADE_TICKS_HEADER.size
TRADE_TICK_SIZE: int = TRADE_TICK.size
