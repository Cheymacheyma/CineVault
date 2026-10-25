"""Alias module for the Pik Pickle database layer.

The canonical implementation lives in :mod:`database`; this module simply
re-exports its public API under the name the Pik Pickle application and its
documentation refer to (``import pik_pickle as db``).

The default database *file* used by the app is chosen explicitly by ``app.py``
(``pik_pickle.db`` next to these files, or ``$PIK_PICKLE_DB``), so importing
this module has no hidden side effects.
"""

from __future__ import annotations

from database import *  # noqa: F401,F403  (re-export the public API)
from database import __all__ as _database_all

__all__ = list(_database_all)
