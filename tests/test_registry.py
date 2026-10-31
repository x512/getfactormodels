# getfactormodels: https://github.com/x512/getfactormodels
# Copyright (C) 2025-2026 S. Martin <x512@pm.me>
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# Distributed WITHOUT ANY WARRANTY. See LICENSE for full terms.
import pytest

from getfactormodels.utils.registry import (
    get_model_key,
    get_model_class,
    list_models,
)


@pytest.mark.parametrize(
    "user_input,expected_key",
    [
        ("ff3", "ff3"),
        ("3", "ff3"),
        ("famafrench3", "ff3"),
        ("CH-3", "ch3"),
        ("ccapm_hi", "hcapm"),
        ("Q5", "q"),
    ]
)
def test_get_model_key_success(user_input, expected_key):
    assert get_model_key(user_input) == expected_key


@pytest.mark.parametrize(
    "invalid_input",
    [
        "",
        None,
        "not-a-real-model",
    ]
)
def test_get_model_key_raises_value_error(invalid_input):
    with pytest.raises(ValueError):
        get_model_key(invalid_input)


def test_get_model_class_direct_and_alias():
    assert get_model_class("ff3") == "FamaFrenchFactors"
    assert get_model_class("car") == "CarhartFactors"


def test_list_models():
    models = list_models()
    assert isinstance(models, dict)
    assert "ff3" in models
    assert models["ff3"]["model_class"] == "FamaFrenchFactors"
