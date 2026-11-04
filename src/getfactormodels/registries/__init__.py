# src/getfactormodels/registries/__init__.py
"""Read-only registries."""
from types import MappingProxyType
from getfactormodels.registries import _jkp

JKP_FACTORS = MappingProxyType(_jkp.JKP_FACTORS)
JKP_THEMES = MappingProxyType(_jkp.JKP_THEMES)
JKP_REGIONS = tuple(_jkp.JKP_REGIONS)

# S3 endpoint accepts factors or themes, so validation can 
# use the union. TODO:
#JKP_REGISTRY = MappingProxyType({**JKP_THEMES, **JKP_FACTORS})

__all__ = ["JKP_FACTORS", "JKP_THEMES", "JKP_REGIONS",]
