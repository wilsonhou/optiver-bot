import enum

from typing import Optional


class Instrument(enum.IntEnum):
    FUTURE = 0
    ETF = 1


class Side(enum.IntEnum):
    SELL = 0
    BUY = 1


class Lifespan(enum.IntEnum):
    FILL_AND_KILL = 0  # Fill and kill orders trade immediately if possible, otherwise they are cancelled
    GOOD_FOR_DAY = 1  # Good for day orders remain in the market until they trade or are explicitly cancelled


class ICompetitor(object):
    def disconnect(self) -> None:
        """Disconnect this competitor."""
        raise NotImplementedError()

    def on_amend_message(self, now: float, client_order_id: int, volume: int) -> None:
        """Called when an amend order request is received from the competitor."""
        raise NotImplementedError()

    def on_cancel_message(self, now: float, client_order_id: int) -> None:
        """Called when a cancel order request is received from the competitor."""
        raise NotImplementedError()

    def on_insert_message(self, now: float, client_order_id: int, side: int, price: int, volume: int,
                          lifespan: int) -> None:
        """Called when an insert order request is received from the competitor."""
        raise NotImplementedError()

    def set_start_time(self, start_time: float) -> None:
        """Set the start time of the match."""
        raise NotImplementedError()


class IExecutionChannel(object):
    def close(self):
        """Close the execution channel."""
        raise NotImplementedError()

    def send_error(self, client_order_id: int, error_message: bytes) -> None:
        """Send an error message to the auto-trader."""
        raise NotImplementedError()

    def send_order_status(self, client_order_id: int, fill_volume: int, remaining_volume: int, fees: int) -> None:
        """Send an order status message to the auto-trader."""
        raise NotImplementedError()

    def send_position_change(self, future_position: int, etf_position: int) -> None:
        """Send a position change message to the auto-trader."""
        raise NotImplementedError()

    def set_start_time(self, start_time: float) -> None:
        """Set the start time of the match."""
        raise NotImplementedError()


class IController(object):
    def get_competitor(self, name: str, secret, exec_channel: IExecutionChannel) -> Optional[ICompetitor]:
        """Return the competitor instance for the specified name.
        :param secret:
        """
        raise NotImplementedError()

    def market_events_complete(self) -> None:
        """Indicates that the controller should shut down on the next timer tick."""
        raise NotImplementedError()

    def on_connection_lost(self, name: str) -> None:
        """Indicates that a connection to an auto-trader has been lost."""
        raise NotImplementedError()

    def shutdown(self, reason: str) -> None:
        """Shut down the match."""
        raise NotImplementedError()


class ITaskListener(object):
    def on_task_complete(self, task) -> None:
        """Called when the task is complete"""
        pass
