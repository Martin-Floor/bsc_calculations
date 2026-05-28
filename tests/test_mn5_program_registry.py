"""Tests for mn5.jobArrays program= dispatch (chai1, protenix, rfaa).

Each new cofolder program must:
  - be in available_programs list
  - emit the right module + extras when used with jobArrays(program=...)
  - auto-route gp_bscls -> acc_bscls (GPU partition)
"""
import os
import sys
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), os.pardir))
from bsc_calculations import mn5


def _read_script(script_path):
    with open(script_path) as fh:
        return fh.read()


@pytest.mark.parametrize("program,markers,partition_after", [
    ("chai1", [
        "/gpfs/projects/bsc72/conda_envs/chai_v061",
        "CHAI_DOWNLOADS_DIR=/gpfs/projects/bsc72/weights/chai_v061",
    ], "acc_bscls"),
    ("protenix", [
        "/gpfs/projects/bsc72/conda_envs/protenix",
        "PROTENIX_DATA_ROOT_DIR=",
    ], "acc_bscls"),
    ("rfaa", [
        "/gpfs/scratch/bsc72/bsc072523/envs/rfaa_pip2",
        "PYTHONPATH=/gpfs/projects/bsc72/Programs/RoseTTAFold-All-Atom",
        "PATH=/gpfs/projects/bsc72/Programs/hhsuite-3.3.0/bin",
        "HHLIB=/gpfs/projects/bsc72/Programs/hhsuite-3.3.0",
        "RFAA_PDB100_DIR=/gpfs/projects/bsc72/databases/pdb100_2021Mar03",
        "RFAA_WEIGHTS=/gpfs/projects/bsc72/weights/rfaa/RFAA_paper_weights.pt",
        "cuda/11.8",
    ], "acc_bscls"),
    ("esmfold", [
        "anaconda/2023.07",
        "HF_HUB_OFFLINE=1",
        "TRANSFORMERS_OFFLINE=1",
    ], "acc_bscls"),
    ("esmfold2", [
        "cuda/12.6",
        "HF_HOME=/gpfs/projects/bsc72/mfloor/envs/huggingface_cache",
        "HF_HUB_OFFLINE=1",
        "TRANSFORMERS_OFFLINE=1",
    ], "acc_bscls"),
])
def test_program_registry_emits_correct_extras(tmp_path, monkeypatch, program, markers, partition_after):
    """When program=<chai1|protenix|rfaa> + partition=gp_bscls is passed,
    mn5.jobArrays should auto-route to acc_bscls and emit the program-specific
    activation lines."""
    monkeypatch.chdir(tmp_path)
    script_path = tmp_path / "run.sh"
    mn5.jobArrays(
        jobs=["echo hello"],
        script_name=str(script_path),
        job_name="test_job",
        partition="gp_bscls",  # should auto-route
        gpus=1,
        time=1,
        program=program,
    )
    text = _read_script(script_path)
    # All expected strings present
    for marker in markers:
        assert marker in text, f"missing {marker!r} for program={program}"
    # Auto-routed to acc partition
    assert f"--qos={partition_after}" in text or f"-p {partition_after}" in text or partition_after in text


def test_program_registry_lists_new_cofolders():
    """available_programs in mn5.py should include chai1, protenix, rfaa, esmfold, esmfold2."""
    import inspect
    src = inspect.getsource(mn5.jobArrays)
    assert '"chai1"' in src
    assert '"protenix"' in src
    assert '"rfaa"' in src
    assert '"esmfold"' in src
    assert '"esmfold2"' in src


def test_unknown_program_raises():
    with pytest.raises(ValueError, match="Program not found"):
        mn5.jobArrays(
            jobs=["echo hi"],
            script_name="/tmp/x.sh",
            job_name="x",
            partition="gp_bscls",
            time=1,
            program="not_a_real_program_xyz",
        )
