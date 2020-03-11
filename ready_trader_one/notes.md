# Notes

- Instead of print, use: self.logger.info() or self.logger.error() --> it will log in the file of the autotrader

- Instrument, Side and Lifespan are IntEnum types. They're useless apart from checking in callbacks. E.g: to check if variable 'side' is a sell order, do: if side == Side.SELL:

- Use callback functions listed in base_auto_trader.py to do market operators.

- Position is the literal volume of ETFs we own (or future, but we don't care about that). E.g Sell 20 ETFs, and from position of 0 and our position will become -20.

## Interpreting the log files

- All log files (and match_events.csv) have been gitignored. Generate them by running run.py

- Square brackets [INFO ] mean it's an info log vs error log as [ERROR ]

- Anything else in square brackets is the python file thel og came from.

- Anything logged with self.logger will appear in .log files

## TODO

- Make a basic strategy for the bot.

## VERY HELPFUL LINKS

Peer reviewed article on micro-pricing: how do we implement this?
https://www-tandfonline-com.ezproxy2.library.usyd.edu.au/doi/full/10.1080/14697688.2018.1489139
