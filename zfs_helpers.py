import logging
import subprocess
import re
from enum import Enum
from time import sleep, time

import util

_LOGGER = logging.getLogger(__name__)


class ScrubStatus(Enum):
    SCANNING = 1
    NO_ERRORS = 2
    ERRORS = 3


def get_all_zpools() -> list[str]:
    # Run 'zpool list' command and capture the output
    _result = subprocess.run(["zpool", "list", "-H", "-o", "name"], capture_output=True, text=True)
    if _result.returncode != 0:
        _err = "Error while getting all zpools"
        _LOGGER.error(_err)
        raise RuntimeError(_err)
    zpool_list = [line for line in _result.stdout.strip().split("\n")]
    _LOGGER.debug(f"Available zpools: {zpool_list}")
    return zpool_list


def get_scrub_status(zpool_name: str) -> tuple[ScrubStatus, float|str]:
    _result = subprocess.run(["zpool", "status", zpool_name], capture_output=True, text=True)
    if _result.returncode != 0:
        _err = f"Error while getting scrub status of zpool '{zpool_name}'"
        _LOGGER.error(_err)
        raise RuntimeError(_err)
    _matches = re.findall(r"(\d{1,2}\.\d{1,2})%", _result.stdout)
    if len(_matches) > 1:
        _err = f"Unexpected output format of scrub status of zpool '{zpool_name}': \n{_result.stdout}"
        _LOGGER.error(_err)
        raise RuntimeError(_err)
    if len(_matches) == 1:
        _percent = float(_matches[0])
        return ScrubStatus.SCANNING, _percent
    _match = re.search(r"^errors:\s*(.*)\s*$", _result.stdout, flags=re.IGNORECASE | re.MULTILINE)
    if _match is None:
        _err = f"Unexpected output format of scrub status of zpool '{zpool_name}': \n{_result.stdout}"
        _LOGGER.error(_err)
        raise RuntimeError(_err)
    _err_str = _match.group(1)
    if _err_str.lower().startswith("no "):
        return ScrubStatus.NO_ERRORS, _result.stdout
    else:
        return ScrubStatus.ERRORS, _result.stdout


def run_scrub(zpool_name: str, timeout_seconds: float) -> None:
    if util.IS_DEBUGGER is True:
        _LOGGER.info(f"Waiting for user to run 'zpool scrub {zpool_name}' manually..")
    else:
        _result = subprocess.run(["zpool", "scrub", zpool_name], capture_output=True, text=True)
        if _result.returncode != 0:
            _err = f"Error while running scrub on zpool '{zpool_name}'"
            _LOGGER.error(_err)
            raise RuntimeError(_err)
    _started_at = time()
    while True:
        sleep(0.5)
        _status, _addval = get_scrub_status(zpool_name)
        if _status == ScrubStatus.SCANNING:
            break
        elif (time() - _started_at) > timeout_seconds:
            _err = f"Timeout while starting scrub on zpool '{zpool_name}: \n{_addval}'"
            _LOGGER.error(_err)
            raise RuntimeError(_err)
