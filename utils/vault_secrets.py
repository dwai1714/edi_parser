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
import os
from datetime import datetime

# noinspection PyProtectedMember
from dynaconf import settings

logger = logging.getLogger(__name__)


class SettingsConnector:
    """
    Connector to Dynaconf
    """

    def __init__(self, dynaconf_file=None):
        try:
            self.dynaconf_file = dynaconf_file
            if self.dynaconf_file is not None:
                settings.load_file(path=dynaconf_file)
                if "ENV_FOR_DYNACONF" in os.environ:
                    settings.setenv(os.environ["ENV_FOR_DYNACONF"])
                else:
                    settings.setenv("development")
        except Exception as error:
            logging.error(
                f"error in SecretsConnector __init__() "
                f"error = {error} at {datetime.now()}"
            )
            raise

    # noinspection PyBroadException
    @staticmethod
    def get_value(key_name):
        """

        Args:
            key_name (str): Key Name

        Returns:
            str: Value of Key

        """
        try:
            value_of_key = settings.get(key_name)
            if value_of_key is not None:
                return value_of_key
            else:
                return os.environ[key_name]
        except Exception:
            return None

    def __getitem__(self, key_name):
        return self.get_value(key_name)

    def get(self, key_name):
        """

        Args:
            key_name (str): Key Name

        Returns:
            str: Value of Key

        """
        return self.get_value(key_name)

    def __getattr__(self, key_name):
        return self.get_value(key_name)
