import argparse
import logging.handlers
import logging
from datetime import datetime, timedelta
from pathlib import Path
from time import sleep

import zfs_helpers
import util

_LOGGER = logging.getLogger(__name__)
_LOG_FILE_PATH = Path(__file__).with_suffix(".log")
_LAST_EXECUTION_FILE_PATH = Path(__file__).parent / "last-execution.txt"

logging.basicConfig(format="[%(asctime)s] {%(module)s:%(lineno)d} %(levelname)s - %(message)s", datefmt='%H:%M:%S',
                    level=logging.DEBUG,
                    handlers=[logging.StreamHandler(),
                              logging.handlers.TimedRotatingFileHandler(_LOG_FILE_PATH, when="midnight", backupCount=3)])


_ALL_ZPOOLS = zfs_helpers.get_all_zpools()


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
    elif execution_after == "day" and _time_since_last_execution < timedelta(days=1):
        return False
    elif execution_after == "month" and _time_since_last_execution < timedelta(days=30):
        return False
    return True


def _run_and_monitor_scrub(zpool_name: str):
    zfs_helpers.run_scrub(zpool_name=zpool_name)
    while True:
        _status, _addval = zfs_helpers.get_scrub_status(zpool_name=zpool_name)
        if _status != zfs_helpers.ScrubStatus.SCANNING:
            break
        sleep(2)
        # TODO Telegram set tqdm
    if _status == zfs_helpers.ScrubStatus.NO_ERRORS:
        # TODO Telegram tqdm to 100%
        _LOGGER.info(f"Scrub finished with no errors for zpool '{zpool_name}'")
    elif _status == zfs_helpers.ScrubStatus.ERRORS:
        _LOGGER.warning(f"Scrub finished with an error for zpool '{zpool_name}':\n\n{_addval}")
    else:
        raise RuntimeError("We shouldn't have ended-up here!")


if __name__ == '__main__':
    _LOGGER.debug("===================================")
    if len(_ALL_ZPOOLS) == 0:
        _LOGGER.info("No zpools available on the system. Exiting now.")
        exit(0)
    if util.IS_DEBUGGER is False and util.IS_SUDO is False:
        _LOGGER.error("This script must be launched with sudo permissions! Exiting now.")
        exit(1)

    _parser = argparse.ArgumentParser(description="Retrieve information about ZFS pools")
    _parser.add_argument("-z", "--zpool", type=str, help=f"The ZFS pool to scrub (default: '{_ALL_ZPOOLS[0]}')")
    _parser.add_argument("-e", "--execution-after", type=str, choices=["week", "day", "month"], help="Optional. Specifies the allowed timeframe for script execution (week, day, or month).")

    _args = _parser.parse_args()
    _zpool = _args.zpool
    _execution_after = _args.execution_after

    # Check the last execution time
    if _execution_after is not None and is_execution_necessary(_execution_after) is False:
        print(f"The script was run within the last {_execution_after}. Execution not necessary right now, exiting.")
        exit(0)

    if _zpool is None:
        _zpool = _ALL_ZPOOLS[0]
    if _zpool not in _ALL_ZPOOLS:
        _LOGGER.error(f"Zpool '{_zpool}' does not exist on the system! Exiting now.")

    _run_and_monitor_scrub(zpool_name=_zpool)
    write_last_execution_time()
