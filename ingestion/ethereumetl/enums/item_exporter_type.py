from enum import Enum

class ItemExporterType(str, Enum):
    CONSOLE = "console"
    KAFKA = "kafka"
    UNKNOWN = "unknown"