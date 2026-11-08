# src/getfactormodels/registries/__init__.py
"""Read-only registries."""
from types import MappingProxyType

from . import _jkp, _q

JKP_FACTORS = MappingProxyType(_jkp.JKP_FACTORS)
JKP_THEMES = MappingProxyType(_jkp.JKP_THEMES)
JKP_REGIONS = tuple(_jkp.JKP_REGIONS)
# S3 endpoint accepts factors or themes, so validation can 
# use the union. TODO:
#JKP_REGISTRY = MappingProxyType({**JKP_THEMES, **JKP_FACTORS})

Q_ANOMALIES = MappingProxyType(_q.Q_ANOMALIES)
Q_THEMES = MappingProxyType(_q.Q_THEMES)
Q_THEME_FACTORS = MappingProxyType(_q.Q_THEME_FACTORS)

Q_REGISTRY = MappingProxyType(Q_THEMES | Q_ANOMALIES)


__all__ = [
    "JKP_FACTORS", "JKP_THEMES", "JKP_REGIONS", #"JKP_REGISTRY",
    "Q_ANOMALIES", 
    "Q_THEMES",
    "Q_THEME_FACTORS", 
    "Q_REGISTRY",
]
