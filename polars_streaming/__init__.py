from .core import StreamProcessor
from importlib import resources
try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib

# Version of the polars-streaming package
__version__ = "0.3.0"