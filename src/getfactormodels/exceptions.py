# getfactormodels: https://github.com/x512/getfactormodels
# Copyright (C) 2025-2026 S. Martin <x512@pm.me>
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# Distributed WITHOUT ANY WARRANTY. See LICENSE for full terms.
"""Custom exceptions and warnings for getfactormodels."""

class GetFactorModelsError(Exception):
    """Base exception for all getfactormodels errors."""

class ModelNotFoundError(GetFactorModelsError, ValueError):
    """Raised when a requested model key or alias is not in the registry."""

class DownloadError(GetFactorModelsError, RuntimeError):
    """Raised when a remote dataset cannot be downloaded."""

class GetFactorModelsWarning(UserWarning):
    """Base warning for all getfactormodels warnings."""

class GetFactorModelsDeprecationWarning(GetFactorModelsWarning, DeprecationWarning):
    """Raised when a deprecated feature is used."""

