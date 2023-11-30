# COPYRIGHT NOTICE

# Copyright © 2022 Breakwater Solutions, LLC. All rights reserved.
#
#CONFIDENTIAL AND PROPRIETARY
# This is unpublished proprietary source code for a program. The copyright notice
# above does not evidence any actual or intended publication of the source code,
# and the source code is not otherwise divested of its trade secrets, irrespective
# of what may have been deposited with the U.S. Copyright Office in connection
# with any registration relating to the program.

#

import logging
import logging.handlers
import os
import sys
from pathlib import Path

from dynaconf import settings

try:
    import colorlog
except ImportError:
    pass


DATE_FORMAT = "%y/%m/%d %T"
LOG_LEVEL = settings.get("LOG_LEVEL") if settings.get("LOG_LEVEL") else "WARN"

if LOG_LEVEL == "DEBUG":
    FORMAT = "%(asctime)s %(levelname)s %(name)s[%(lineno)s] %(funcName)s: %(message)s"
else:
    FORMAT = "%(asctime)s %(levelname)s %(name)s: %(message)s"


def get_logger(name=""):
    logger = logging.getLogger(name)  # get root logger
    if logger.handlers:
        return logger

    # file handler
    log_location = settings.LOG_FILE_LOCATION
    Path(log_location).mkdir(parents=True, exist_ok=True)
    log_file_path = str(Path(log_location) / settings.LOG_FILE_NAME)
    logging.basicConfig(
        format=FORMAT,
        filemode="a+",
        filename=log_file_path,
        datefmt=DATE_FORMAT,
    )

    # console handler
    stream_handler = logging.StreamHandler()
    if "colorlog" in sys.modules and os.isatty(2):
        cformat = "%(log_color)s" + FORMAT

        formatter = colorlog.ColoredFormatter(
            cformat,
            DATE_FORMAT,
            log_colors={
                "DEBUG": "reset",
                "INFO": "reset",
                "WARNING": "bold_yellow",
                "ERROR": "bold_red",
                "CRITICAL": "bold_red",
            },
        )
    else:
        formatter = logging.Formatter(fmt=FORMAT, datefmt=DATE_FORMAT)

    stream_handler.setFormatter(formatter)
    stream_handler.setLevel(LOG_LEVEL)

    logger = logging.getLogger(name)
    logger.setLevel(LOG_LEVEL)
    logger.addHandler(stream_handler)

    return logger
