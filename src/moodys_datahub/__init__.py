"""
Top-level package for the Moody's DataHub tools.

This module exposes the main public API so users can do e.g.:

    from moodys_datahub import Sftp
    # or, if you add more symbols to __all__:
    # from moodys_datahub import Sftp, some_helper

Anything you add to ``__all__`` will be importable directly from
``moodys_datahub``.
"""

# Public class from tools.py
from .tools import Sftp

# If you have public (non-underscore) helpers in tools.py or utils.py
# that you want to expose at the top level, import them here, e.g.:
#
# from .tools import search_dictionary, search_country_codes
# from .utils import fuzzy_query
#
# and then add their names to __all__ below.

__all__ = [
    "Sftp",
    # "search_dictionary",
    # "search_country_codes",
    # "fuzzy_query",
]
