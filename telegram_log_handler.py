import logging
from logging import LogRecord
import traceback

import requests


URL = "https://api.telegram.org/bot{token}/sendMessage"
MAX_MESSAGE_LENGTH = 3000

LOGGERS = [logging.getLogger("urllib3"),
           logging.getLogger("requests")]

_LOGGER = logging.getLogger(__name__)

# More telegram formatting options, see here:
# https://core.telegram.org/bots/api#formatting-options
DEFAULT_FORMATTER = logging.Formatter("<code>%(asctime)s UTC</code>\n"
                                      "<code>%(name)s</code>\n"
                                      "<code>%(filename)s::%(funcName)s</code>\n"
                                      "<code>Line %(lineno)s</code>\n"
                                      "<b>%(levelname)s</b>\n"
                                      "<pre>%(message)s</pre>")

class TelegramHandler(logging.Handler):
    """
    A handler class which asynchronously sends a Telegram message for each logging event.
    """
    def __init__(self, token, ids, formatter=DEFAULT_FORMATTER):
        """
        Initialize the handler.

        Initialize the instance with the bot's token and a list of chat_id(s)
        of the conversations that should be notified by the handler.
        """
        logging.Handler.__init__(self)
        self.setFormatter(formatter)
        self.token = token
        self.ids = ids
        self.url = URL.format(token=self.token)

    def __transmit_message(self, log_record: LogRecord) -> bool:
        """
        Transmits the actual log record. Will be called from within worker thread.

        :param log_record: The log record that is supposed to be transmitted.
        :return: Returns True if successful, otherwise False.
        """
        propagate_values = []
        for _logger in LOGGERS:
            propagate_values.append(_logger.propagate)
            _logger.propagate = False

        _LOGGER.debug("Starting to emit Telegram log message")

        try:

            # Remove HTML-tag-like symbols
            def _clean_func(s: str) -> str:
                return s.replace("<", "*").replace(">", "*")

            log_record.funcName = _clean_func(log_record.funcName)
            log_record.msg = _clean_func(log_record.msg)
            if len(log_record.msg) > MAX_MESSAGE_LENGTH:
                log_record.msg = f"  <b>...</b>\n{log_record.msg[MAX_MESSAGE_LENGTH:]}\n\n<i>Message was shortened</i>"
                _LOGGER.debug("Shortened a too long log message")
            # log_record.message = _clean_func(log_record.message)  -->  Not necessary

            for chat_id in self.ids:
                payload = {
                    "chat_id": chat_id,
                    "text": self.format(log_record),
                    "parse_mode": "html"
                }

                try:
                    response = requests.post(self.url, data=payload, timeout=10)
                    _LOGGER.debug("Successfully emitted Telegram log message")
                    return True
                except requests.RequestException as e:
                    # level = self.level
                    # self.setLevel(logging.CRITICAL)
                    # logger.warning(
                    #     "RequestException occurred when trying to post a Telegram message: {}".format(repr(e)))
                    # self.setLevel(level)
                    _LOGGER.log(level=self.level - 10,
                                msg=f"RequestException occurred when trying to post a Telegram message: {repr(e)}")
        except (KeyboardInterrupt, SystemExit):
            raise
        except:  # noqa
            # level = self.level
            # self.setLevel(logging.CRITICAL)
            # logger.exception(f"Unexpected exception occurred when trying to post a Telegram message\n\n"
            #                  f"{traceback.format_exc()}")
            # logger.debug(log_record)
            # self.setLevel(level)
            # # self.handleError(record)
            _LOGGER.log(self.level - 10, msg=f"Unexpected exception occurred when trying to post a Telegram message\n\n"
                                          f"{traceback.format_exc()}")
            _LOGGER.log(level=self.level - 10, msg=log_record)
        finally:
            for _logger, propagate in zip(LOGGERS, propagate_values):
                _logger.propagate = propagate
        return False

    def emit(self, record):
        """
        Emit a record. Format the record and send it to the specified chats. Gets called by logger.
        """
        self.__transmit_message(record)