# dsrqst

Python project to add and process user requests for the
[NSF NCAR Geoscience Data Exchange (GDEX)](https://gdex.ucar.edu).

The user guide for this utility tool can be viewed at:
[User guide](https://gdex-docs-dsrqst.readthedocs.io).

## Programs

The package installs the following command-line utility, running as the
current user:

- `dsrqst` — add and process user data requests

Run with `--help` (or `-h`) for full usage details.

## Environment setup

Create a Python environment first; package installs in the next section run
inside whichever environment you activate here.

### Option A — Python venv (DECS machines)

```bash
python3 -m venv $ENVHOME          # e.g. /glade/u/home/gdexdata/gdexmsenv
source $ENVHOME/bin/activate
```

### Option B — Conda (DAV/Casper)

```bash
conda create --prefix $ENVHOME python=3.12   # e.g. /glade/work/gdexdata/conda-envs/pg-gdex
conda activate $ENVHOME
```

## Installing rda-python-dsrqst

Pick whichever install mode fits your workflow.  All variants pull in the
transitive dependencies (`rda_python_common`, `rda_python_miscs`)
automatically.

For local development, clone this repo alongside your project and install it
in editable mode so that changes are picked up without re-installing:

```bash
git clone https://github.com/NCAR/rda-python-dsrqst.git
cd rda-python-dsrqst
pip install -e .
```

To test a specific branch (e.g. an in-progress feature or fix branch), pass
`-b/--branch` to `git clone`:

```bash
git clone -b <branch-name> https://github.com/NCAR/rda-python-dsrqst.git
cd rda-python-dsrqst
pip install -e .
```

For a regular (non-editable) install from a checkout:

```bash
pip install /path/to/rda-python-dsrqst
```

For a production install on a system that uses the published distribution:

```bash
pip install rda_python_dsrqst
```

## Documentation sync

The user guide at
[gdex-docs-dsrqst.readthedocs.io](https://gdex-docs-dsrqst.readthedocs.io) is
generated from `src/rda_python_dsrqst/dsrqst.usg`.  Keep all user-facing content
in `dsrqst.usg` — no manual RST editing is required.

When a pull request modifying `dsrqst.usg` is opened, an automated workflow
converts it into RST source files and the version number from this repository's
`pyproject.toml` into the
[gdex-docs-dsrqst](https://github.com/NCAR/gdex-docs-dsrqst) repository, then
opens a pull request from `automated-update-branch` against its `main` branch for
review.

To publish to Read the Docs:

- **Merge** that pull request into `main` to serve the content as the `latest`
  version.
- **Create a GitHub release** in `gdex-docs-dsrqst` to serve the latest release
  as the `stable` version.
