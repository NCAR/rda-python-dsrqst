"""rda_python_dsrqst: dataset request (dsrqst) utility package.

This package exposes two parallel APIs:

1. Legacy module-based API (back-compat). Import the capitalized
   submodules and call their module-level functions, e.g.::

       from rda_python_dsrqst import PgRqst, PgRDARqst, PgSubset

2. Class-based API (preferred for new code). Import the class from the
   lower-case module and either instantiate or subclass it, e.g.::

       from rda_python_dsrqst.pg_rqst import PgRqst
       from rda_python_dsrqst.pg_rdarqst import PgRDARqst
       from rda_python_dsrqst.pg_subset import PgSubset

The legacy submodules are eagerly imported below so that
``from rda_python_dsrqst import PgRqst`` continues to return the module
object that existing callers expect.
"""

from . import PgRqst, PgRDARqst, PgSubset

__version__ = "2.0.4"

__all__ = [
   "PgRqst",
   "PgRDARqst",
   "PgSubset",
   "__version__",
]
