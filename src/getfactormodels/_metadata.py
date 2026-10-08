import textwrap
from importlib.metadata import PackageNotFoundError, version

__title__ = "getfactormodels"
__description__ = "A Python package to retrieve financial factor model data."
__author__ = "S. Martin"
__author_email__ = "x512@pm.me"
__license__ = "AGPL-3.0-or-later"

try:
    __version__ = version(__title__)
except PackageNotFoundError:
    __version__ = "unknown"

__copyright__ = f"Copyright (C) 2025-2026 {__author__} <{__author_email__}>"

__license_text__ = textwrap.dedent(f"""\
    {__title__} (ver. {__version__}).
    {__copyright__}

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <https://www.gnu.org/licenses/>.
""")
