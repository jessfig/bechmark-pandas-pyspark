from enum import Enum


class ScaleFactorTPCH(Enum):
    DEZ_MB = 0.01
    CEM_MB = 0.1
    UM_GIGA = 1
    DEZ_GIGA = 10
    CEM_GIGA = 100