import importlib.metadata

from laufband.laufband import close, laufband

__all__ = ["laufband", "close"]
__version__ = importlib.metadata.version("laufband")
