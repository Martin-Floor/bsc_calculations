"""Tests for mn5.jobArrays program= dispatch.

Each new MN5 program preset must:
  - be in the available_programs list
  - emit the right modules + exports + conda env when used with
    jobArrays(program=...)
  - stay on the requested CPU partition (or auto-route to acc for GPU codes)
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), os.pardir))
from bsc_calculations import mn5


def _read_script(script_path):
    with open(script_path) as fh:
        return fh.read()


def test_program_registry_lists_chemshell():
    """available_programs in mn5.py should include chemshell."""
    import inspect
    src = inspect.getsource(mn5.jobArrays)
    assert '"chemshell"' in src


def test_chemshell_preset_cpu(tmp_path, monkeypatch):
    """program='chemshell' loads the MN5 GPP ChemShell stack (openmpi-gcc +
    orca/5.0.3), activates the chemshell_qmmm conda env, extends PATH with
    the chemsh.x launcher + ORCA + DL_POLY, exports the LD_LIBRARY_PATH
    workaround for orca/5.0.3, and stays on the requested CPU partition."""
    monkeypatch.chdir(tmp_path)
    script_path = tmp_path / "run.sh"
    mn5.jobArrays(
        jobs=["chemsh system.py > chemsh.log 2>&1"],
        script_name=str(script_path),
        job_name="chemsh_job",
        partition="gp_bscls",
        ntasks=1,
        cpus_per_task=32,
        time=12,
        program="chemshell",
    )
    text = _read_script(script_path)
    for marker in (
        "module purge",
        "module load openmpi/4.1.5-gcc",
        "module load orca/5.0.3",
        "source activate /gpfs/projects/bsc72/mfloor/conda_envs/chemshell_qmmm",
        "ORCA_BIN=/apps/GPP/ORCA/5.0.3/OPENMPI/orca",
        "LD_LIBRARY_PATH=/apps/GPP/ORCA/5.0.3/OPENMPI:${LD_LIBRARY_PATH}",
        "CHEMSH_ROOT=/gpfs/projects/bsc72/mfloor/chemsh-py-25.0.5",
        "CHEMSH_ARCH=gnu",
        "OMPI_MCA_rmaps_base_oversubscribe=1",
        "/gpfs/projects/bsc72/mfloor/chemsh-py-25.0.5/bin/gnu",
        "/apps/GPP/ORCA/5.0.3/OPENMPI",
        "/gpfs/projects/bsc72/mfloor/dl-poly/build/bin",
        "chemshell_run() {",
        "if [ -f _dl_poly.inp ] && [ ! -f CONTROL ]; then",
        'chemsh "$driver"',
    ):
        assert marker in text, f"missing {marker!r}"
    # CPU code: must NOT be re-routed to a GPU (acc) partition.
    assert "--qos=gp_bscls" in text
    assert "acc_bscls" not in text


def test_program_registry_lists_q6():
    """available_programs in mn5.py should include Q6."""
    import inspect
    src = inspect.getsource(mn5.jobArrays)
    assert '"Q6"' in src


def test_q6_preset_cpu(tmp_path, monkeypatch):
    """program='Q6' loads openmpi/4.1.5-gcc (runtime for Qdyn6p), puts the MN5
    shared Q6 bin on PATH, sources the qtools init script, and stays on the
    requested CPU partition. Guards against the old broken preset that pointed at
    a nonexistent 'q6' module and another user's home."""
    monkeypatch.chdir(tmp_path)
    script_path = tmp_path / "run.sh"
    mn5.jobArrays(
        jobs=["Qdyn6 relax_001.inp > relax_001.log"],
        script_name=str(script_path),
        job_name="q6_job",
        partition="gp_bscls",
        ntasks=1,
        cpus_per_task=20,
        time=12,
        program="Q6",
    )
    text = _read_script(script_path)
    for marker in (
        "module load openmpi/4.1.5-gcc",
        "/gpfs/projects/bsc72/Programs/Q6/bin",
        "source /gpfs/projects/bsc72/Programs/qtools/qtools_init.sh",
    ):
        assert marker in text, f"missing {marker!r}"
    # the old broken preset must be gone
    assert "bsc072181" not in text
    assert "module load q6" not in text
    # CPU code: stays on the requested CPU partition.
    assert "--qos=gp_bscls" in text
    assert "acc_bscls" not in text
