import argparse
import logging.handlers
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path
from time import sleep
from dataclasses import dataclass
from typing import Optional

import psutil
import tqdm
import tqdm.contrib.telegram
from functools import partial

import zfs_helpers
import util
from telegram_log_handler import TelegramHandler

_LOGGER = logging.getLogger(__name__)
_LOG_FILE_PATH = Path(__file__).with_suffix(".log")
_LAST_EXECUTION_FILE_PATH = Path(__file__).parent / "last-execution.txt"

logging.basicConfig(format="[%(asctime)s] {%(module)s:%(lineno)d} %(levelname)s - %(message)s", datefmt='%H:%M:%S',
                    level=logging.DEBUG,
                    handlers=[logging.StreamHandler(),
                              logging.handlers.TimedRotatingFileHandler(_LOG_FILE_PATH, when="midnight", backupCount=3)])

logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)


_ALL_ZPOOLS = zfs_helpers.get_all_zpools()


@dataclass
class TelegramCredentials:
    api_token: str
    chat_id: str


def read_last_execution_time():
    # Read the last execution time from the file
    try:
        with open(_LAST_EXECUTION_FILE_PATH, "r", encoding="utf-8") as file:
            return datetime.fromisoformat(file.read().strip())
    except (SystemExit, KeyboardInterrupt):
        raise
    except BaseException:
        return None


def write_last_execution_time():
    # Write the current time to the file
    try:
        with open(_LAST_EXECUTION_FILE_PATH, "w", encoding="utf-8") as file:
            file.write(datetime.now().isoformat())
    except Exception as e:
        _LOGGER.warning(f"Error writing last execution time:\n\n{e}")


def is_execution_necessary(execution_after):
    last_execution_time = read_last_execution_time()
    if last_execution_time is None:
        return True

    _time_since_last_execution = datetime.now() - last_execution_time
    if execution_after == "week" and _time_since_last_execution < timedelta(weeks=1):
        return False
    elif execution_after == "2weeks" and _time_since_last_execution < timedelta(weeks=2):
        return False
    elif execution_after == "day" and _time_since_last_execution < timedelta(days=1):
        return False
    elif execution_after == "month" and _time_since_last_execution < timedelta(days=30):
        return False
    return True


def _run_and_monitor_scrub(zpool_name: str, telegram_credentials: Optional[TelegramCredentials]):
    zfs_helpers.start_scrub(zpool_name=zpool_name, timeout_seconds=5)
    _tqdm_fn = tqdm.tqdm if telegram_credentials is None else \
        partial(tqdm.contrib.telegram.tqdm, token=telegram_credentials.api_token, chat_id=telegram_credentials.chat_id)
    try:
        with _tqdm_fn(total=100.0, desc="Scrubbing") as _pbar:
            while True:
                _status, _addval = zfs_helpers.get_scrub_status(zpool_name=zpool_name)
                if _status != zfs_helpers.ScrubStatus.SCANNING:
                    break
                _pbar.update(_addval - _pbar.n)
                sleep(2)
            if _status == zfs_helpers.ScrubStatus.NO_ERRORS:
                _pbar.update(100 - _pbar.n)
                _LOGGER.info(f"Scrub finished with no errors for zpool '{zpool_name}'\n\n{_addval}")
            elif _status == zfs_helpers.ScrubStatus.ERRORS:
                _LOGGER.warning(f"Scrub finished with an error for zpool '{zpool_name}':\n\n{_addval}")
            else:
                raise RuntimeError("We shouldn't have ended-up here!")
    except (SystemExit, KeyboardInterrupt) as e:
        _LOGGER.warning(f"Script was interrupted by a {type(e).__name__}. The actual scrub of zpool '{zpool_name}', however, continues.")
        raise


def _is_already_running() -> bool:
    _script_name = Path(__file__).name
    for _process in psutil.process_iter(["pid", "name", "cmdline"]):
        try:
            if _process.pid != os.getpid() and _script_name in _process.cmdline():
                return True
        except psutil.Error:
            pass
    return False


if __name__ == '__main__':
    _parser = argparse.ArgumentParser(description="Retrieve information about ZFS pools")
    _parser.add_argument("-z", "--zpool", type=str, help=f"The ZFS pool to scrub (default: '{_ALL_ZPOOLS[0] if len(_ALL_ZPOOLS) else ''}')")
    _parser.add_argument("-e", "--execution-after", type=str, choices=["day", "week", "2weeks", "month"], help="Optional. Specifies the allowed timeframe for script execution (week, day, or month).")
    _parser.add_argument("-t", "--telegram-api-token", type=str, help="Telegram API token")
    _parser.add_argument("-c", "--telegram-chat-id", type=str, help="Telegram chat ID")

    _args = _parser.parse_args()
    _zpool = _args.zpool
    _execution_after = _args.execution_after
    _telegram_api_token = _args.telegram_api_token
    _telegram_chat_id = _args.telegram_chat_id

    _LOGGER.debug("===================================")

    # Handle Telegram-specific args
    if sum(x is not None for x in (_telegram_api_token, _telegram_chat_id)) not in (0, 2):
        _LOGGER.error("Either none of Telegram API token and chat ID must be given, or both of them! Exiting now.")
        exit(1)
    if _telegram_api_token is None:
        _telegram_credentials = None
        _LOGGER.debug("Running without Telegram")
    else:
        _telegram_credentials = TelegramCredentials(api_token=_telegram_api_token, chat_id=_telegram_chat_id)
        _telegram_log_handler = TelegramHandler(token=_telegram_api_token, ids=[_telegram_chat_id])
        _telegram_log_handler.setLevel(logging.INFO)
        logging.getLogger().addHandler(_telegram_log_handler)
        _LOGGER.debug("Running with Telegram")

    if _is_already_running():
        _LOGGER.debug("Another instance of myself is already active. Exiting now.")
        exit(0)
    if len(_ALL_ZPOOLS) == 0:
        _LOGGER.info("No zpools available on the system. Exiting now.")
        exit(0)
    if util.IS_DEBUGGER is False and util.IS_SUDO is False:
        _LOGGER.error("This script must be launched with sudo permissions! Exiting now.")
        exit(1)

    # Check the last execution time
    if _execution_after is not None and is_execution_necessary(_execution_after) is False:
        _LOGGER.debug(f"The script was run within the last {_execution_after}. Execution not necessary right now, exiting.")
        exit(0)

    if _zpool is None:
        _zpool = _ALL_ZPOOLS[0]
    if _zpool not in _ALL_ZPOOLS:
        _LOGGER.error(f"Zpool '{_zpool}' does not exist on the system! Exiting now.")

    _run_and_monitor_scrub(zpool_name=_zpool, telegram_credentials=_telegram_credentials)
    write_last_execution_time()
