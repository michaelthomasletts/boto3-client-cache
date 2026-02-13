__all__ = []

from . import cache, exceptions
from .cache import *  # noqa: F403
from .exceptions import *  # noqa: F403

# controlling star imports
__all__ += cache.__all__
__all__ += exceptions.__all__

# package metadata
__version__ = "0.1.0"
__title__ = "boto3-client-cache"
__author__ = "Mike Letts"
__maintainer__ = "Mike Letts"
__license__ = "Apache License 2.0"
__email__ = "lettsmt@gmail.com"
__description__ = (
    "A simple Python package with a cache for boto3 clients. "
    "Includes LRU and LFU caching."
)
