import asyncio
import collections
import logging
import socket

from typing import Any, Dict, Optional

from .account import CompetitorAccount
from .competitor import Competitor
from .execution import ExecutionChannel
from .information import InformationChannel
from .limiter import FrequencyLimiter
from .market_events import MarketEvents
from .match_events import MatchEvents
from .order_book import ITradeListener, OrderBook, TopLevels
from .types import ICompetitor, IController, IExecutionChannel, ITaskListener, Instrument
from .util import create_datagram_endpoint


# The delay between starting the server and opening the market
MARKET_OPEN_DELAY_SECONDS = 20.0


class Controller(IController, ITradeListener, ITaskListener):
    """Controller for the Ready Trader One matching engine."""

    def __init__(self, config: Dict[str, Any], loop: asyncio.AbstractEventLoop):
        """Initialise a new instance of the Controller class."""
        self.competitors: Dict[str, Competitor] = dict()
        self.competitor_count: int = 0
        self.config: Dict[str, Any] = config
        self.done: bool = False
        self.etf_book: OrderBook = OrderBook(Instrument.ETF, self, config["Fees"]["Maker"], config["Fees"]["Taker"])
        self.etf_trade_ticks: Dict[int, int] = collections.defaultdict(lambda: 0)
        self.event_loop: asyncio.AbstractEventLoop = loop
        self.future_book: OrderBook = OrderBook(Instrument.FUTURE, self, 0.0, 0.0)
        self.future_trade_ticks: Dict[int, int] = collections.defaultdict(lambda: 0)
        self.logger: logging.Logger = logging.getLogger("CONTROLLER")
        self.start_time: float = 0.0

        info = config["Information"]
        self.info_channel: InformationChannel = InformationChannel((info["Host"], info["Port"]))

        engine = config["Engine"]
        self.market_events: MarketEvents = MarketEvents(engine["MarketDataFile"], loop, self, self.future_book,
                                                        self.etf_book, self)
        self.match_events: MatchEvents = MatchEvents(engine["MatchEventsFile"], loop, self)
        self.speed: float = engine["Speed"]
        self.tick_interval: float = engine["TickInterval"] / engine["Speed"]

    def get_competitor(self, name: str, secret: str, exec_channel: IExecutionChannel) -> Optional[ICompetitor]:
        """Return the competitor object for this match."""
        if name in self.competitors or name not in self.config["Traders"] or self.config["Traders"][name] != secret:
            return None

        instrument = self.config["Instrument"]
        limits = self.config["Limits"]

        account = CompetitorAccount(instrument["TickSize"], instrument["EtfClamp"])
        competitor = Competitor(name, self, exec_channel, self.future_book, self.etf_book, account, self.match_events,
                                limits["PositionLimit"], limits["ActiveOrderCountLimit"], limits["ActiveVolumeLimit"],
                                instrument["TickSize"])
        self.competitors[name] = competitor

        self.logger.info("'%s' is ready!", name)

        if self.start_time != 0.0:
            self.logger.warning("competitor logged in after market open: name='%s'", name)
            competitor.set_start_time(self.start_time)

        return competitor

    def market_events_complete(self) -> None:
        """Indicates that the controller should shut down on the next timer tick."""
        self.done = True

    def on_connection_lost(self, name: Optional[str]) -> None:
        """Called when a client disconnects."""
        self.competitor_count -= 1

    def on_new_connection(self) -> ExecutionChannel:
        """Called when a new connection is received on the server."""
        self.competitor_count += 1
        engine = self.config["Engine"]
        limits = self.config["Limits"]
        frequency_limiter = FrequencyLimiter(limits["MessageFrequencyInterval"] / engine["Speed"],
                                             limits["MessageFrequencyLimit"])
        return ExecutionChannel(self.event_loop, self, self.market_events, frequency_limiter, engine["Speed"])

    def on_task_complete(self, task) -> None:
        """Called when the match events writer task is complete"""
        if task is self.match_events:
            self.event_loop.stop()

    def on_timer_tick(self, tick_time: float, sequence_number: int) -> None:
        """Called when it is time to send an order book update and trade ticks."""
        try:
            now: float = self.event_loop.time()

            if self.competitor_count == 0:
                self.shutdown("no remaining competitors")
                return

            elapsed: float = (now - self.start_time) * self.speed
            self.market_events.process_market_events(elapsed)
            for comp in self.competitors.values():
                comp.on_timer_tick(elapsed, self.future_book.last_traded_price(), self.etf_book.last_traded_price())

            if self.done:
                self.shutdown("match complete")
                return

            # There may have been a delay, so work out which tick this really is
            skipped_ticks: float = (now - tick_time) // self.tick_interval
            sequence_number += int(skipped_ticks)

            for inst, book, ticks in ((Instrument.FUTURE, self.future_book, self.future_trade_ticks),
                                      (Instrument.ETF, self.etf_book, self.etf_trade_ticks)):
                top: TopLevels = book.top_levels()
                self.info_channel.send_order_book_update(inst, sequence_number, top.ask_prices, top.ask_volumes,
                                                         top.bid_prices, top.bid_volumes)
                if ticks:
                    self.info_channel.send_trade_ticks(inst, ticks.items())
                    ticks.clear()

            tick_time += self.tick_interval + self.tick_interval * skipped_ticks
            self.event_loop.call_at(tick_time, self.on_timer_tick, tick_time, sequence_number + 1)
        except Exception as e:
            self.logger.error("exception in on_timer_tick:", exc_info=e)
            self.shutdown("exception in on_timer_tick")

    def on_trade(self, instrument: Instrument, price: int, volume: int) -> None:
        """Called when a trade occurs in one of the order books."""
        if instrument == Instrument.FUTURE:
            self.future_trade_ticks[price] += volume
        else:
            self.etf_trade_ticks[price] += volume

    def shutdown(self, reason: str) -> None:
        """Shut down the match."""
        elapsed = (self.event_loop.time() - self.start_time) * self.speed
        self.logger.info("shutting down the match: time=%.6f reason='%s'", elapsed, reason)
        for competitor in self.competitors.values():
            competitor.disconnect()
        self.match_events.finish()

    async def start(self) -> None:
        """Start running the match."""
        self.logger.info("starting the match")

        host = self.config["Execution"]["ListenAddress"]
        port = self.config["Execution"]["Port"]
        server = await self.event_loop.create_server(self.on_new_connection, host, port, family=socket.AF_INET)

        info = self.config["Information"]
        if info["AllowBroadcast"]:
            await self.event_loop.create_datagram_endpoint(lambda: self.info_channel, family=socket.AF_INET,
                                                           proto=socket.IPPROTO_UDP, allow_broadcast=True)
        else:
            await create_datagram_endpoint(self.event_loop, lambda: self.info_channel,
                                           remote_addr=(info["Host"], info["Port"]), family=socket.AF_INET,
                                           interface=info["Interface"])

        self.market_events.start()
        self.match_events.start()

        # Give the auto-traders time to start up and connect
        await asyncio.sleep(MARKET_OPEN_DELAY_SECONDS)
        server.close()

        self.logger.info("market open")
        self.start_time = self.event_loop.time()
        for competitor in self.competitors.values():
            competitor.set_start_time(self.start_time)

        self.on_timer_tick(self.start_time, 1)
