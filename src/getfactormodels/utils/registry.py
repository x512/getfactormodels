# getfactormodels: https://github.com/x512/getfactormodels
# Copyright (C) 2025-2026 S. Martin <x512@pm.me>
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# Distributed WITHOUT ANY WARRANTY. See LICENSE for full terms.
from types import MappingProxyType
from typing import Final

_MODEL_REGISTRY: Final = MappingProxyType({
    "ff3": {
        "model_class": "FamaFrenchFactors",
        "name": "Fama-French 3-Factor Model",
        "aliases": ["3", "famafrench3"],
    },
    "ff4": {
        "model_class": "CarhartFactors",
        "name": "Carhart/Fama-French 4-Factors",
        "aliases": ["4", "car", "carhart", "famafrench4"],
    },
    "ff5": {
        "model_class": "FamaFrenchFactors",
        "name": "Fama-French 5-Factors",
        "aliases": ["5", "famafrench5"],
    },
    "ff6": {
        "model_class": "FamaFrenchFactors",
        "name": "Fama-French 6-Factors",
        "aliases": ["6", "famafrench6"],
    },
    "q": {
        "model_class": "QFactors",
        "name": "Augmented q5 Factors",
        "aliases": ["q5", "hmxz", "qfactors"],
    },
    "qc": {
        "model_class": "QFactors",
        "name": "q-Factors (Classic)",
        "aliases": ["q4", "classicq", "qclassic"],
    },
    "hmld": {
        "model_class": "HMLDevilFactors",
        "name": "AQR: HML Devil",
        "aliases": ["devil", "hmldevil"],
    },
    "qmj": {
        "model_class": "QMJFactors",
        "name": "AQR: Quality Minus Junk",
        "aliases": ["quality", "quality-minus-junk"],
    },
    "bab": {
        "model_class": "BABFactors",
        "name": "AQR: Betting Against Beta",
        "aliases": ["betting", "betting-against-beta"],
    },
    "vme": {
        "model_class": "VMEFactors",
        "name": "AQR: Value & Momentum Everywhere",
        "aliases": ["valmom", "value-and-momentum-everywhere"],
    },
    "aqr6": {
        "model_class": "AQR6Factors",
        "name": "AQR: 6-Factor Model",
        "aliases": ["aqr", "aqrfactors", "a6"],
    },
    "mis": {
        "model_class": "MispricingFactors",
        "name": "Mispricing Factors",
        "aliases": ["misp", "mispricing"],
    },
    "liq": {
        "model_class": "LiquidityFactors",
        "name": "Liquidity Factors",
        "aliases": ["liquidity"],
    },
    "icr": {
        "model_class": "ICRFactors",
        "name": "Intermediary Capital Ratio",
        "aliases": ["intermediary"],
    },
    "dhs": {
        "model_class": "DHSFactors",
        "name": "DHS Behavioral Factors",
        "aliases": ["behaviour", "behavior", "dhs"],
    },
    "bs": {
        "model_class": "BarillasShankenFactors",
        "name": "Barillas-Shanken 6-Factors",
        "aliases": ["bs6", "barillasshanken"],
    },
    "hcapm": {
        "model_class": "HighIncomeCCAPM",
        "name": "High Income (Affluent Household) Conditional CAPM",
        "aliases": ["hcapm", "hicapm", "ccapm-hi"],
    },
    "pcapm": {
        "model_class": "ConditionalCAPM",
        "name": "Premium-Labour Conditional CAPM",
        "aliases": ["jwcapm", "plcapm", "ccapm-pl"],
    },
    "ch3": {
        "model_class": "CHFactors",
        "name": "CH-3: Size and Value in China",
        "aliases": ["ch3", "ch-3"],
    },
    "ch4": {
        "model_class": "CHFactors",
        "name": "CH-4: Size and Value in China",
        "aliases": ["ch4", "ch-4"],
    },

})


_LOOKUP: Final[MappingProxyType[str, str]] = MappingProxyType({
    alias.lower().replace("-", "").replace("_", ""): key 
    for key, data in _MODEL_REGISTRY.items() 
    for alias in data.get("aliases", []) + [key]
})


def get_model_key(user_input: str | int) -> str:
    """Resolves model name or alias to its registry key.

    Args:
        user_input: The model name, ID, or alias provided by the user.

    Returns (str): 
        The model's registry key.

    Raises:
        ValueError: If the model or portfolio is empty or unknown.
    """
    if not user_input:
        raise ValueError("Model name cannot be empty.")

    val = str(user_input).lower().strip().replace("-", "").replace("_", "")

    if val in _LOOKUP:
        return _LOOKUP[val]

    raise ValueError(f"Unknown model or portfolio: '{user_input}'")


def get_model_class(key: str) -> str:
    """Retrieve the model class name string from the registry for a given key.

    Args:
        key: The model registry key or alias.

    Returns:
        The model class name.
    """
    if key not in _MODEL_REGISTRY:
        key = get_model_key(key)
    return _MODEL_REGISTRY[key]["model_class"]


def list_models() -> dict:
    """Return a copy of the model registry."""
    return dict(_MODEL_REGISTRY)

