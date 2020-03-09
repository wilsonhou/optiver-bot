from .types import Instrument, Side


class CompetitorAccount(object):
    """A competitors account."""

    def __init__(self, tick_size: float, etf_clamp: float):
        """Initialise a new instance of the CompetitorAccount class."""
        self.account_balance: int = 0
        self.buy_volume: int = 0
        self.etf_clamp: float = etf_clamp
        self.etf_position: int = 0
        self.future_position: int = 0
        self.max_drawdown: int = 0
        self.max_profit: int = 0
        self.profit_or_loss: int = 0
        self.sell_volume: int = 0
        self.tick_size: int = int(tick_size * 100.0)
        self.total_fees: int = 0

    def transact(self, instrument: Instrument, side: Side, price: int, volume: int, fee: int) -> None:
        """Update this account with the specified transaction."""
        if side == Side.SELL:
            self.account_balance += price * volume
        else:
            self.account_balance -= price * volume

        self.account_balance -= fee
        self.total_fees += fee

        if instrument == Instrument.FUTURE:
            if side == Side.SELL:
                self.future_position -= volume
            else:
                self.future_position += volume
        else:
            if side == Side.SELL:
                self.sell_volume += volume
                self.etf_position -= volume
            else:
                self.buy_volume += volume
                self.etf_position += volume

    def mark_to_market(self, future_price: int, etf_price: int) -> None:
        """Mark this account to market using the specified prices."""
        delta: int = round(self.etf_clamp * future_price)
        delta -= delta % self.tick_size
        min_price: int = future_price - delta
        max_price: int = future_price + delta
        clamped: int = min_price if etf_price < min_price else max_price if etf_price > max_price else etf_price
        self.profit_or_loss = self.account_balance + self.future_position * future_price + self.etf_position * clamped
        if self.profit_or_loss > self.max_profit:
            self.max_profit = self.profit_or_loss
        if self.max_profit - self.profit_or_loss > self.max_drawdown:
            self.max_drawdown = self.max_profit - self.profit_or_loss
