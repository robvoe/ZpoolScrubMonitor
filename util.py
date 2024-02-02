import os
import sys

__gettrace = getattr(sys, 'gettrace', None)
IS_DEBUGGER = True if (__gettrace is not None and __gettrace()) else False

IS_SUDO = (os.geteuid() == 0)
