import asyncio
import socket
import sys

from .application import Application
from .controller import Controller

# From Python 3.8, the proactor event loop is used by default on Windows
if sys.platform == "win32" and hasattr(asyncio, "WindowsSelectorEventLoopPolicy"):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


def __validate_hostname(config, section, key):
    try:
        config[section][key] = socket.gethostbyname(config[section][key])
    except socket.error:
        raise Exception("Could not validate hostname in %s configuration" % section)


def __validate_object(config, section, required_keys, value_types):
    obj = config[section]
    if type(obj) is not dict:
        raise Exception("%s configuration should be a JSON object" % section)
    if any(k not in obj for k in required_keys):
        raise Exception("A required key is missing from the %s configuration" % section)
    if any(type(obj[k]) is not t for k, t in zip(required_keys, value_types)):
        raise Exception("Element of inappropriate type in %s configuration" % section)


def __exchange_config_validator(config):
    """Return True if the specified config is valid, otherwise raise an exception."""
    if type(config) is not dict:
        raise Exception("Configuration file contents should be a JSON object")
    if any(k not in config for k in ("Engine", "Execution", "Fees", "Information", "Instrument", "Limits", "Traders")):
        raise Exception("A required key is missing from the configuration")

    __validate_object(config, "Engine", ("MarketDataFile", "MatchEventsFile", "Speed", "TickInterval"),
                      (str, str, float, float))
    __validate_object(config, "Execution", ("ListenAddress", "Port"), (str, int))
    __validate_object(config, "Fees", ("Maker", "Taker"), (float, float))
    __validate_object(config, "Information", ("AllowBroadcast", "Host", "Interface", "Port"), (bool, str, str, int))
    __validate_object(config, "Instrument", ("EtfClamp", "TickSize",), (float, float))
    __validate_object(config, "Limits", ("ActiveOrderCountLimit", "ActiveVolumeLimit", "MessageFrequencyInterval",
                                         "MessageFrequencyLimit", "PositionLimit"), (int, int, float, int, int))

    __validate_hostname(config, "Execution", "ListenAddress")
    __validate_hostname(config, "Information", "Host")
    __validate_hostname(config, "Information", "Interface")

    if type(config["Traders"]) is not dict:
        raise Exception("Traders configuration should be a JSON object")
    if any(type(k) is not str for k in config["Traders"]):
        raise Exception("Key of inappropriate type in Traders configuration")
    if any(type(v) is not str for v in config["Traders"].values()):
        raise Exception("Element of inappropriate type in Traders configuration")

    return True


def main():
    app = Application("exchange", __exchange_config_validator)
    ctrl = Controller(app.config, app.event_loop)
    app.event_loop.create_task(ctrl.start())
    app.run()
