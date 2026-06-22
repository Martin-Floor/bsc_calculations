import os

def jobArrays(
    jobs,
    script_name=None,
    job_name=None,
    ntasks=1,
    gpus=1,
    mem_per_cpu=None,
    highmem=False,
    partition=None,
    cpus_per_task=None,
    output=None,
    mail=None,
    time=None,
    module_purge=False,
    modules=None,
    conda_env=None,
    unload_modules=None,
    program=None,
    conda_eval_bash=False,
    account="bsc72",
    jobs_range=None,
    group_jobs_by=None,
    pythonpath=None,
    local_libraries=False,
    msd_version=None,
    mpi=False,
    pathMN=None,
    extras=None,
    exports=None,
    sources=None,
):
    """
    Set up job array scripts for marenostrum slurm job manager.

    Parameters
    ==========
    jobs : list
        List of jobs. Each job is a string representing the command to execute.
    script_name : str
        Name of the SLURM submission script.
    jobs_range : (list, tuple)
        The range of job IDs to be included in the job array (one-based numbering).
        This restart the indexing of the slurm array IDs, so keep count of the original
        job IDs based on the supplied jobs list. Also, the range includes the last job ID.
        Useful when large IDs cannot enter the queue.
    group_jobs_by : int
        Group jobs to enter in the same job array (useful for launching many short
        jobs when there are a max_job_allowed limit per user.
    local_libraries : bool
        Add local libraries (e.g., prepare_proteins) to PYTHONPATH?
    """

    # --- Normalize and clamp walltime (accepts None | int hours | (hours, minutes))
    def _normalize_time(partition_name, time_val):
        """
        Returns:
          sbatch_time_str: 'HH:MM:SS'
          norm_tuple: (hours, minutes)
        """
        def as_tuple(tv):
            if tv is None:
                return None
            if isinstance(tv, int):
                return (tv, 0)
            if isinstance(tv, (tuple, list)) and len(tv) == 2:
                h, m = int(tv[0]), int(tv[1])
                if m >= 60:
                    h += m // 60
                    m = m % 60
                return (max(h, 0), max(m, 0))
            raise ValueError("time must be None, int (hours), or (hours, minutes)")

        p = (partition_name or "").lower()
        is_debug = "debug" in p or p in {"gp_debug", "acc_debug"}
        is_bscls = p.endswith("bscls") or p in {"gp_bscls", "acc_bscls"}

        default = (2, 0) if is_debug else ((48, 0) if is_bscls else (24, 0))
        cap_minutes = 120 if is_debug else (48 * 60 if is_bscls else None)

        ht = as_tuple(time_val) or default
        total = ht[0] * 60 + ht[1]
        if cap_minutes is not None and total > cap_minutes:
            print(
                f"Requested time {ht[0]}h{ht[1]:02d} exceeds partition cap; "
                f"clamping to {cap_minutes//60}h{cap_minutes%60:02d}."
            )
            total = cap_minutes
        hours, minutes = divmod(total, 60)
        return f"{hours:02d}:{minutes:02d}:00", (hours, minutes)

    # Check input
    if isinstance(jobs, str):
        jobs = [jobs]

    if jobs_range != None:
        if (
            not isinstance(jobs_range, (list, tuple))
            or len(jobs_range) != 2
            or not all([isinstance(x, int) for x in jobs_range])
        ):
            raise ValueError(
                "The given jobs_range must be a tuple or a list of 2-integers"
            )

    if partition is None:
        raise ValueError(
            "You must select a partition. Available partitions are: "
            "acc_debug, acc_bscls, gp_debug, gp_bscls"
        )

    # Capture whether the caller passed an explicit walltime *before* the
    # generic normalisation rewrites None into the partition default.
    # Program-specific blocks (e.g. alphafold3) consult this so they can
    # apply their own default only when the user has not opted in.
    _user_supplied_time = time is not None
    sbatch_time, time = _normalize_time(partition, time)

    # Group jobs to enter in the same job array (useful for launching many short
    # jobs when there are a max_job_allowed limit per user.)
    if isinstance(group_jobs_by, int):
        grouped_jobs = []
        gj = ""
        for i, j in enumerate(jobs):
            gj += j.rstrip("\n") + "\n"
            if (i + 1) % group_jobs_by == 0:
                grouped_jobs.append(gj)
                gj = ""
        if gj != "":
            grouped_jobs.append(gj)
        jobs = grouped_jobs

    elif not isinstance(group_jobs_by, type(None)):
        raise ValueError("You must give an integer to group jobs by this number.")

    # Check PYTHONPATH variable
    if pythonpath == None:
        pythonpath = []

    if pathMN == None:
        pathMN = []

    # Normalize new/changed params
    if extras is None:
        extras = []

    if sources is not None and isinstance(sources, str):
        sources = [sources]

    #! Programs
    available_programs = [
        "gromacs",
        "alphafold",
        "alphafold3",
        "hmmer",
        "asitedesign",
        "blast",
        "pyrosetta",
        "openmm",
        "gamd",
        "Q6",
        "bioml",
        "rosetta",
        "bioemu",
        "PLACER",
        "RFDiffusion",
        "bioemu_af",
        "cp2k",
        "chemshell",
        "boltz2",
        "ligandmpnn",
        "mlcg",
        "bindcraft",
        "mood",
        "orca",
    ]

    # available_programs = ['pele', 'peleffy', 'rosetta', 'predig', 'pyrosetta', 'rosetta2', 'blast',
    #                      'msd', 'pml', 'netsolp', 'alphafold', 'asitedesign']

    if program != None:
        if program not in available_programs:
            raise ValueError(
                "Program not found. Available programs: "
                + " ,".join(available_programs)
            )

    if program == "gromacs":
        if modules == None:
            modules = ["cuda", "nvidia-hpc-sdk/23.11", "gromacs/2023.3"]
        else:
            modules += ["cuda", "nvidia-hpc-sdk/23.11", "gromacs/2023.3"]
        extras = [
            "export SLURM_CPU_BIND=none",
            "export OMP_NUM_THREADS=$SLURM_CPUS_PER_TASK",
            "export GMX_ENABLE_DIRECT_GPU_COMM=1",
            "export GMX_GPU_PME_DECOMPOSITION=1",
            'GMXBIN="mpirun --bind-to none -report-bindings gmx_mpi"',
        ]

        if cpus_per_task > 1:
            warning_message = """
            ----------------------------------------------------------------------------------------------
            |                                          WARNING                                           |
            ----------------------------------------------------------------------------------------------

            With cpus_per_task != 1 you might encounter the following
            GROMACS error:

            | Fatal error:
            | There is no domain decomposition for {cpus_per_task} ranks that is
            | compatible with the given box and a minimum cell size of
            | ___ nm
            | Change the number of ranks or mdrun option -rcon or -dds or
            | your LINCS settings. Look in the log file for details on the
            | domain decomposition
            """
            print(warning_message)

        # Update mpi and omp options to match cpu and gpus
        gromacs_jobs = []
        for job in jobs:
            gromacs_jobs.append(job.replace("mdrun", f"mdrun -pin on -pinoffset 0"))
        jobs = gromacs_jobs

    if program == "openmm":
        openmm_modules = ["anaconda", "cuda/11.8"]
        if modules == None:
            modules = openmm_modules
        else:
            modules += openmm_modules

        conda_env = "/gpfs/projects/bsc72/conda_envs/openmm_cuda"

    if program == "gamd":
        gamd_modules = ["anaconda", "cuda/11.8"]
        if modules == None:
            modules = gamd_modules
        else:
            modules += gamd_modules

        conda_env = "/gpfs/projects/bsc72/conda_envs/openmm_cuda"
        pathMN.append("/gpfs/projects/bsc72/Programs/gamd-openmm")

    if program == "protmlx":
        bioml_modules = ["anaconda", "perl/5.38.2"]
        module_purge = True
        if modules == None:
            modules = bioml_modules
        else:
            modules += bioml_modules

        conda_env = "/gpfs/projects/bsc72/conda_envs/bioml"

    if program == "alphafold":
        if modules == None:
            modules = ["singularity", "alphafold/2.3.2", "cuda"]
        else:
            modules += ["singularity", "alphafold/2.3.2", "cuda"]

    if program == "alphafold3":
        module_purge = True
        af3_modules = ["singularity", "cuda/12.6", "alphafold/3.0.0"]
        if modules is None:
            modules = af3_modules
        else:
            modules += af3_modules

        if partition == "gp_bscls":
            partition = "acc_bscls"

        # AF3 default walltime is 2h (most jobs finish in ~30–40 min on H100).
        # Honour an explicit ``time=`` from the caller — earlier this branch
        # unconditionally clamped to 2h and silently dropped longer requests,
        # which timed out long-MSA jobs even though acc_bscls allows up to 48h.
        if not _user_supplied_time:
            time = (2, 0)
        sbatch_time, time = _normalize_time(partition, time)

        if exports is None:
            exports = []

        required_exports = [
            "WEIGHTS=/gpfs/projects/bsc72/weights/AF3/",
        ]
        for export in required_exports:
            if export not in exports:
                exports.append(export)

    if program == "blast":
        if modules == None:
            modules = ["blast"]
        else:
            modules += ["blast"]

    if program == "pyrosetta":
        pyrosetta_modules = ["anaconda"]
        if modules == None:
            modules = pyrosetta_modules
        else:
            modules += pyrosetta_modules
        if mpi:
            conda_env = "/gpfs/projects/bsc72/conda_envs/mood"
        else:
            conda_env = "/gpfs/projects/bsc72/conda_envs/pyrosetta"

    if program == "ligandmpnn":
        conda_env = "/gpfs/projects/bsc72/conda_envs/ligandmpnn"

    if program == "mood":
        # MOOD multi-objective optimizer runs the outer driver via the
        # miniforge system Python (no top-level conda_env). Inner metric
        # subprocesses (ESMC, LigandMPNN, ...) activate their own envs
        # through MOOD's --*-conda-env CLI flags, so we:
        #  - load miniforge + source conda init so child `conda activate` works
        #  - put MOOD on PYTHONPATH; point HuggingFace at the offline cache
        #  - clear inherited CONDA_* state so that child `conda activate`
        #    in metric subprocesses can swap python cleanly. On MN5,
        #    `module load miniforge` leaks CONDA_PREFIX=ANACONDA/2023.07
        #    while CONDA_PYTHON_EXE points at MINIFORGE — that conflicting
        #    state makes child activations partial-fail (activate.d hooks
        #    fire but PATH never updates), so `python` keeps resolving to
        #    the outer miniforge 3.10 instead of the inner env's binary.
        if modules is None:
            modules = ["miniforge"]
        elif "miniforge" not in modules:
            modules += ["miniforge"]
        miniforge_conda_sh = "/apps/ACC/MINIFORGE/24.3.0-0/etc/profile.d/conda.sh"
        if sources is None:
            sources = [miniforge_conda_sh]
        elif miniforge_conda_sh not in sources:
            sources = list(sources) + [miniforge_conda_sh]
        mood_repo = "/gpfs/projects/bsc72/mfloor/Repos/multiObjectiveOptimizationDesign"
        if mood_repo not in pythonpath:
            pythonpath.append(mood_repo)
        if exports is None:
            exports = []
        mood_exports = [
            f"MOOD_REPO={mood_repo}",
            "HF_HUB_CACHE=/gpfs/projects/bsc72/mfloor/cache/hf_hub",
            "HF_HUB_OFFLINE=1",
        ]
        for e in mood_exports:
            if e not in exports:
                exports.append(e)
        unset_line = (
            "unset CONDA_PREFIX CONDA_DEFAULT_ENV CONDA_PYTHON_EXE "
            "CONDA_SHLVL CONDA_PROMPT_MODIFIER CONDA_EXE _CE_M _CE_CONDA"
        )
        if unset_line not in extras:
            extras.append(unset_line)
        if partition == "gp_bscls":
            partition = "acc_bscls"

    if program == "mlcg":
        conda_env = "/gpfs/projects/bsc72/conda_envs/mlcg"

    if program == "hmmer":
        hmmer_modules = ["anaconda"]
        if modules == None:
            modules = hmmer_modules
        else:
            modules += hmmer_modules
        conda_env = "/gpfs/projects/bsc72/conda_envs/hmm"

    if program == "Q6":
        # Q6 EVB/FEP engine (Qprep6/Qdyn6/Qfep6 serial + Qdyn6p MPI) compiled on MN5
        # 2026-06-21, plus the qtools input generators, both under bsc72 shared Programs.
        # The previous preset pointed at a nonexistent "q6" module and another user's
        # home (bsc072181) -- neither resolved on MN5; fixed to the real shared install.
        q6_modules = ["openmpi/4.1.5-gcc"]  # runtime for the MPI binary Qdyn6p
        if modules == None:
            modules = q6_modules
        else:
            modules += q6_modules

        pathMN.append("/gpfs/projects/bsc72/Programs/Q6/bin")
        extras = [
            "source /gpfs/projects/bsc72/Programs/qtools/qtools_init.sh"
        ]

    if program == "asitedesign":
        if modules == None:
            modules = ["anaconda", "intel", "openmpi", "mkl", "gcc", "bsc"]
        else:
            modules += ["anaconda", "intel", "openmpi", "mkl", "gcc", "bsc"]
        pythonpath.append("/gpfs/projects/bsc72/Repos/AsiteDesign")
        pathMN.append("/gpfs/projects/bsc72/Repos/AsiteDesign")
        conda_env = "/gpfs/projects/bsc72/conda_envs/asite"

    if program == "rosetta":
        rosetta_modules = ["gcc/12.3.0", "rosetta/3.14"]
        if modules == None:
            modules = rosetta_modules
        else:
            modules += rosetta_modules

    if program == "bioemu":
        if modules == None:
            modules = ["anaconda"]
        else:
            modules += ["anaconda"]
        conda_env = "/gpfs/projects/bsc72/conda_envs/bioemu2"
        if exports == None:
            exports = []
        exports += [
            "COLABFOLD_DIR=/gpfs/projects/bsc72/conda_envs/bioemu2/colabfold",
            "PATH=$PATH:/gpfs/projects/bsc72/Programs/bioemu_colabfold/bin",
        ]

    if program == 'bioemu_af':
        if modules == None:
            modules = ["anaconda"]+["singularity", "alphafold/2.3.2", "cuda"]
        else:
            modules += ["anaconda"]+["singularity", "alphafold/2.3.2", "cuda"]
        conda_env = '/gpfs/projects/bsc72/conda_envs/bioemu2'
        if exports == None:
            exports = []
        exports += ['COLABFOLD_DIR=/gpfs/projects/bsc72/conda_envs/bioemu2/colabfold']

    if program == 'PLACER':
        extras = ["source activate /gpfs/projects/bsc72/conda_envs/PLACER"]

    if program == 'RFDiffusion':
        if modules == None:
            modules = ["anaconda/2024.02"]
        else:
            modules += ["anaconda/2024.02"]
        conda_env = 'RFDiffusion'

    if program == 'cp2k':
        # Use the same stack you built with (GNU + OpenMPI(GCC12.3) + MKL)
        module_purge = True
        specific_modules = ['gcc/12.3.0', 'openmpi/4.1.5-gcc12.3', 'mkl/2023.2.0']
        if modules is None:
            modules = specific_modules
        else:
            modules += specific_modules
        # Do NOT set conda_env here. We must source the toolchain setup file, not "activate" it.
        toolchain_setup = '/gpfs/projects/bsc72/Programs/cp2k-2025.2/tools/toolchain/install/setup'
        if exports is None:
            exports = []

        exports += [
            'OMP_NUM_THREADS=1',
            'MKL_NUM_THREADS=1',
            'MKL_DYNAMIC=FALSE',
            'MKL_DISABLE_FAST_MM=${MKL_DISABLE_FAST_MM:-1}',
            'CP2K_BLAS_AUTO_THREADS=${CP2K_BLAS_AUTO_THREADS:-0}'
        ]

        if sources is None:
            sources = []
        sources.append('/gpfs/projects/bsc72/Programs/cp2k-2025.2/tools/toolchain/install/setup')
        pathMN.append("/gpfs/projects/bsc72/Programs/cp2k-2025.2.clean/exe/local")

    if program == "orca":
        # ORCA-native runs (QM-only OR ORCA's own QM/MM via orca_mm), MPI
        # parallel. This is the path to use when ORCA drives the whole job
        # itself -- NOT when ChemShell drives ORCA (use program="chemshell"
        # for that; its module stack and oversubscribe handling differ).
        #
        # MUST be built/submitted from the GPP login node (marenostrum_gp):
        # the binaries live under /apps/GPP/... and the matching OpenMPI is a
        # GPP module. Submitting from the ACC login pulls /apps/ACC variants
        # and Intel impi, which break orca_mm and the liborca .so loads.
        #
        # Module recipe (MN5 GPP):
        #  - `module unload impi` (NOT `module purge`): purge wipes the base
        #    stack and breaks module resolution on GPP; unloading impi is
        #    enough to stop ORCA's mpirun from landing on Intel Hydra.
        #  - `openmpi/4.1.5` (NOT the `-gcc` variant chemshell uses): the
        #    plain module resolves to the Intel-built OpenMPI, under which
        #    orca's parallel binaries (orca_gtoint_mpi, orca_mm) run cleanly;
        #    the -gcc variant crashes orca_gtoint_mpi in the native path.
        #  - `orca/5.0.3`.
        # Set SLURM --ntasks equal to the ORCA `%pal nprocs N` so OpenMPI has
        # exactly N slots (no oversubscribe needed). The optimal nprocs is
        # system-dependent (QM-region size, basis, exchange) and does not
        # belong here -- benchmark per system and record the result in the
        # project, not in this shared preset.
        if unload_modules is None:
            unload_modules = []
        if "impi" not in unload_modules:
            unload_modules.append("impi")
        orca_modules = ["openmpi/4.1.5", "orca/5.0.3"]
        if modules is None:
            modules = orca_modules
        else:
            modules += orca_modules
        # orca/5.0.3 only prepends /apps/GPP/ORCA/5.0.3/ to PATH, but the
        # binaries (orca, orca_mm, orca_2mkl, ...) live in the OPENMPI/
        # subdir. Add it so orca_mm resolves, and export it on
        # LD_LIBRARY_PATH so liborca_tools_5_0_3.so loads.
        pathMN.append("/apps/GPP/ORCA/5.0.3/OPENMPI")
        if exports is None:
            exports = []
        for e in (
            "ORCA_BIN=/apps/GPP/ORCA/5.0.3/OPENMPI/orca",
            "LD_LIBRARY_PATH=/apps/GPP/ORCA/5.0.3/OPENMPI:${LD_LIBRARY_PATH}",
        ):
            if e not in exports:
                exports.append(e)
        # ORCA refuses parallel runs unless invoked by absolute path (the
        # mpirun-spawned workers need it), so write `${ORCA_BIN} input.inp`.
        # For ORCA-native QM/MM the job command must first convert the Amber
        # topology to ORCA's force-field format, once per run dir:
        #     orca_mm -convff -AMBER <stem>.prmtop   # -> <stem>.ORCAFF.prms
        #     ${ORCA_BIN} system.inp > system.out
        # (the qmbio ORCAQMMMInputBuilder already writes ORCAFFFilename as
        # the converted <stem>.ORCAFF.prms name to match.)

    if program == "chemshell":
        # Py-ChemShell 25.0.5 on MN5 GPP, built against openmpi/4.1.5-gcc
        # with a linked DL_POLY 5.1.0 from /gpfs/projects/bsc72/mfloor/
        # dl-poly. The chemsh.x binary calls ORCA 5.0.3 by system() for
        # the QM step and invokes the linked libdl_poly.so via runLib()
        # for the MM step (so MM energies populate); the full build
        # recipe lives in the project's mn5_chemshell_mpi_build memo.
        #
        # Sizing (80-atom QM region under B3LYP/def2-SVP/D3BJ/RIJCOSX/
        # TightSCF on a 63 k-atom solvated substrate, benched 2026-06-13):
        # the DL_POLY linked-library path peaks at ~156 GB resident, so
        # ``cpus-per-task=32`` on the highmem partition (8 GB/cpu ->
        # 256 GB) is the minimum that survives without OOM.
        #
        # MPI history (resolved 2026-06-14): an earlier verdict that ORCA
        # MPI was "broken" turned out to be two separate bugs in the
        # hand-rolled launchers that DIDN'T go through this preset:
        #   (a) ``module unload impi`` alone (no ``module purge``) left
        #       Intel MPI auto-loaded via ``bsc/1.0``, so ORCA's mpirun
        #       call landed on Intel Hydra and choked on OpenMPI's
        #       ``--oversubscribe`` flag;
        #   (b) even with the right OpenMPI mpirun, SLURM's
        #       ``--ntasks=1`` only exposes 1 MPI slot, so ORCA's
        #       ``mpirun -np N`` (with N > 1) is refused by OpenMPI
        #       unless oversubscribe is explicitly enabled.
        # This preset fixes both: ``module_purge=True`` wipes the impi
        # residue, and ``OMPI_MCA_rmaps_base_oversubscribe=1`` lets
        # OpenMPI accept ``nprocs > SLURM_NTASKS`` from inside the
        # ChemShell-driven ORCA call. Verified 2026-06-14: ORCA QM/MM
        # SP at ``nprocs=4`` now SCF-converges and writes
        # ``FINAL SINGLE POINT ENERGY``.
        # NO `module purge`: ORCA's parallel binary (orca_gtoint_mpi) is built
        # against the Intel-compiled openmpi/4.1.5 (the GPP default), and that
        # module needs a prerequisite stack which `module purge` strips (the
        # `-gcc` variant tolerated purge, the Intel one does not). Use
        # `module unload impi` instead -- same handling as program="orca" --
        # so ORCA's MPI binary matches its build. chemsh.x is ABI-compatible
        # with the same OpenMPI 4.1.5 (libmpi.so.40), so this serves both the
        # serial driver and the parallel ORCA QM step.
        if unload_modules is None:
            unload_modules = []
        if "impi" not in unload_modules:
            unload_modules.append("impi")
        chemsh_modules = ['openmpi/4.1.5', 'orca/5.0.3']
        if modules is None:
            modules = chemsh_modules
        else:
            modules += chemsh_modules
        if exports is None:
            exports = []
        # The orca/5.0.3 module's prepend_path on LD_LIBRARY_PATH does
        # not always survive a module-purge-clean environment, so export
        # the ORCA library dir explicitly. Without this orca crashes
        # with "liborca_tools_5_0_3.so.5: cannot open shared object
        # file" on the first QM step.
        exports.append('ORCA_BIN=/apps/GPP/ORCA/5.0.3/OPENMPI/orca')
        exports.append('LD_LIBRARY_PATH=/apps/GPP/ORCA/5.0.3/OPENMPI:${LD_LIBRARY_PATH}')
        exports.append('CHEMSH_ROOT=/gpfs/projects/bsc72/mfloor/chemsh-py-25.0.5')
        exports.append('CHEMSH_ARCH=gnu')
        # Parallel ORCA QM step (`%pal nprocs N`): request N REAL SLURM slots
        # (`ntasks=N`, like program="orca") -- NOT ntasks=1 + oversubscribe.
        # The real blocker for parallel ORCA under ChemShell is that chemsh.x
        # runs as an OpenMPI *singleton* and leaks OMPI_MCA_ess=singleton /
        # OMPI_APP_CTX_NUM_PROCS=1 into ORCA's environment; ORCA's nested
        # `mpirun -np N` then launches N orca_gtoint_mpi ranks that each believe
        # they are a 1-proc singleton and abort silently in GTOInt. The env-strip
        # ORCA wrapper (ORCA_EXE) emitted below removes the inherited
        # OMPI_/PMIX_/PMI_ vars before ORCA runs, which fixes it. Harmless for
        # serial ORCA (no ranks to confuse), so it is always emitted.
        # The chemsh.x launcher, ORCA binaries and the patched DL_POLY
        # binaries on PATH so ``chemsh system.py`` and the downstream
        # tools resolve directly.
        pathMN.append('/gpfs/projects/bsc72/mfloor/chemsh-py-25.0.5/bin/gnu')
        pathMN.append('/apps/GPP/ORCA/5.0.3/OPENMPI')
        pathMN.append('/gpfs/projects/bsc72/mfloor/dl-poly/build/bin')
        # Pair with the conda env the build was linked against (Python
        # 3.12.11 + numpy 2.2.6); activating any other env will fail
        # with ABI errors on the ChemShell Python module imports.
        conda_env = '/gpfs/projects/bsc72/mfloor/conda_envs/chemshell_qmmm'
        # ChemShell QM/MM is a CPU code: stay on the requested CPU
        # partition (gp_bscls / gp_debug) -- do NOT auto-route to GPU.
        #
        # ChemShell + linked DL_POLY needs a background watcher to copy
        # _dl_poly.inp -> CONTROL (DL_POLY's runLib path reads CONTROL,
        # but chemsh writes _dl_poly.inp fresh each cycle), plus a clean
        # working directory at the start (otherwise stale CONFIG / FIELD
        # / REVCON / _orca.* from a prior failed run get reused and
        # silently corrupt the next QM/MM step).
        #
        # Emit a ``chemshell_run`` bash helper that wraps stale-file
        # cleanup + watcher start + chemsh invocation + watcher stop +
        # exit code propagation. Callers' jobs should be:
        #
        #     cd /path/to/run_dir && chemshell_run system.py
        #
        # The helper takes the ChemShell driver basename as its single
        # positional argument (defaults to ``system.py``).
        if extras is None:
            extras = []
        extras.extend([
            "# Env-strip ORCA wrapper: chemsh.x leaks OpenMPI singleton vars",
            "# (OMPI_MCA_ess=singleton, OMPI_APP_CTX_NUM_PROCS=1) into the ORCA",
            "# child, making ORCA's nested `mpirun -np N` crash orca_gtoint_mpi.",
            "# Strip OMPI_/PMIX_/PMI_/HYDRA_/I_MPI_ before ORCA runs. ORCA_EXE",
            "# points the qmbio driver (and chemsh) at this wrapper. No-op for",
            "# serial ORCA. Requires N real SLURM slots (ntasks=N) for nprocs=N.",
            "# Per-job dir (${SLURM_JOB_ID}): several jobs launched from the same",
            "# submit dir must NOT share one wrapper file -- concurrent exec of a",
            "# single script on GPFS races (ETXTBSY -> exit 126).",
            'ORCAWRAP_DIR="$SLURM_SUBMIT_DIR/_orcawrap_${SLURM_JOB_ID}"',
            'mkdir -p "$ORCAWRAP_DIR"',
            "cat > \"$ORCAWRAP_DIR/orca\" <<'ORCAWRAP'",
            "#!/bin/bash",
            "for v in $(env | grep -oE '^(OMPI_|PMIX_|PMI_|HYDRA_|I_MPI_)[A-Za-z0-9_]+'); do unset \"$v\"; done",
            'exec /apps/GPP/ORCA/5.0.3/OPENMPI/orca "$@"',
            "ORCAWRAP",
            'chmod +x "$ORCAWRAP_DIR/orca"',
            'export ORCA_EXE="$ORCAWRAP_DIR/orca"',
            "",
            "# Helper wired by mn5.jobArrays(program='chemshell'): runs",
            "# the ChemShell driver with the stale-file cleanup and the",
            "# DL_POLY-runLib CONTROL watcher both handled. Call as",
            "# `cd <run_dir> && chemshell_run [driver.py]`.",
            "chemshell_run() {",
            "    local driver=${1:-system.py}",
            "    rm -f CONFIG FIELD CONTROL OUTPUT STATIS REVCON REVIVE \\",
            "          _dl_poly.inp _dl_poly.out _chemsh_run.log chemsh_log.txt \\",
            "          _orca.inp _orca.out _orca.gbw _orca.engrad \\",
            "          qmbio_chemshell_result.json test_status",
            "    (",
            "        while true; do",
            "            if [ -f _dl_poly.inp ] && [ ! -f CONTROL ]; then",
            "                cp _dl_poly.inp CONTROL",
            "            fi",
            "            sleep 1",
            "        done",
            "    ) &",
            "    local watcher_pid=$!",
            "    chemsh \"$driver\" 2>&1 | tee chemsh_log.txt",
            "    local rc=${PIPESTATUS[0]}",
            "    kill $watcher_pid 2>/dev/null",
            "    return $rc",
            "}",
        ])

    if program == "boltz2":
        if modules == None:
            modules = ["intel/2023.1"]
            modules += ["miniforge"]
        else:
            modules += ["intel/2023.1"]
            modules += ["miniforge"]
        extras = ["source activate /gpfs/scratch/bsc72/ismael/conda_envs/boltz2"]

    if program == "bindcraft":
        bc_modules = ["miniforge"]
        if modules == None:
            modules = bc_modules
        else:
            modules += bc_modules
        extras = [
            "source activate /apps/ACC/MINIFORGE/24.3.0-0/envs/BindCraft1.5.1",
        ]
        if exports is None:
            exports = []
        exports += [
            "LD_LIBRARY_PATH=/apps/ACC/MINIFORGE/24.3.0-0/lib:$LD_LIBRARY_PATH",
        ]
        if partition == "gp_bscls":
            partition = "acc_bscls"

    #! Partitions
    available_partitions = ["acc_debug", "acc_bscls", "gp_debug", "gp_bscls"]

    if job_name == None:
        raise ValueError("job_name == None. You need to specify a name for the job")
    if output == None:
        output = job_name
    if partition not in available_partitions:
        raise ValueError(
            "Wrong partition selected. Available partitions are:"
            + ", ".join(available_partitions)
        )

    if script_name == None:
        script_name = "slurm_array.sh"
    elif not script_name.endswith(".sh"):
        script_name += ".sh"

    if modules != None:
        if isinstance(modules, str):
            modules = [modules]
        if not isinstance(modules, list):
            raise ValueError(
                "Modules to load must be given as a list or as a string (for loading one module only)"
            )
    if unload_modules != None:
        if isinstance(unload_modules, str):
            unload_modules = [unload_modules]
        if not isinstance(unload_modules, list):
            raise ValueError(
                "Modules to unload must be given as a list or as a string (for unloading one module only)"
            )
    if conda_env != None:
        if not isinstance(conda_env, str):
            raise ValueError("The conda environment must be given as a string")

    if sources is not None:
        if not isinstance(sources, list) or not all(isinstance(s, str) for s in sources):
            raise ValueError("sources must be a list of strings or a single string")

    # Slice jobs if a range is given
    if jobs_range != None:
        jobs = jobs[jobs_range[0] - 1 : jobs_range[1]]

    # Write jobs as array
    with open(script_name, "w") as sf:
        sf.write("#!/bin/bash\n")
        sf.write("#SBATCH --job-name=" + job_name + "\n")
        sf.write("#SBATCH --qos=" + partition + "\n")
        sf.write("#SBATCH --time=" + sbatch_time + "\n")
        sf.write("#SBATCH --ntasks " + str(ntasks) + "\n")
        if "acc" in partition:
            sf.write("#SBATCH --gres gpu:" + str(gpus) + "\n")
            cpus = gpus * 20
            sf.write("#SBATCH --cpus-per-task " + str(cpus) + "\n")
        sf.write("#SBATCH --account=" + account + "\n")
        if highmem:
            sf.write("#SBATCH --constraint=highmem\n")
        if mem_per_cpu != None:
            sf.write("#SBATCH --mem-per-cpu " + str(mem_per_cpu) + "\n")
        if cpus_per_task != None:
            sf.write("#SBATCH --cpus-per-task " + str(cpus_per_task) + "\n")
        sf.write("#SBATCH --array=1-" + str(len(jobs)) + "\n")
        sf.write("#SBATCH --output=" + output + "_%a_%A.out\n")
        sf.write("#SBATCH --error=" + output + "_%a_%A.err\n")
        if mail != None:
            sf.write("#SBATCH --mail-user=" + mail + "\n")
            sf.write("#SBATCH --mail-type=END,FAIL\n")
        sf.write("\n")

        if module_purge:
            sf.write("module purge\n")
        if unload_modules != None:
            for module in unload_modules:
                sf.write("module unload " + module + "\n")
            sf.write("\n")
        if modules != None:
            for module in modules:
                sf.write("module load " + module + "\n")
            sf.write("\n")
        if sources != None:
            for s in sources:
                sf.write("source " + s + "\n")
            sf.write("\n")
        if conda_eval_bash:
            sf.write('eval "$(conda shell.bash hook)"\n')
        if conda_env != None:
            sf.write("source activate " + conda_env + "\n")
            sf.write("\n")

        if exports != None:
            for export in exports:
                sf.write(f"export {export}\n")
            sf.write("\n")

        if cpus_per_task != None:
            sf.write("export SRUN_CPUS_PER_TASK=${SLURM_CPUS_PER_TASK}" + "\n")

        for pp in pythonpath:
            sf.write("export PYTHONPATH=$PYTHONPATH:" + pp + "\n")
            sf.write("\n")

        for pp in pathMN:
            sf.write("export PATH=$PATH:" + pp + "\n")
            sf.write("\n")

        for extra in extras:
            sf.write(extra + "\n")

    for i in range(len(jobs)):

        if program == "RFDiffusion":
            if "SCRIPT_PATH" in jobs[i]:
                jobs[i] = jobs[i].replace(
                    "SCRIPT_PATH", "/gpfs/projects/bsc72/RFdiffusion/scripts"
                )

        with open(script_name, "a") as sf:
            sf.write("if [[ $SLURM_ARRAY_TASK_ID = " + str(i + 1) + " ]]; then\n")
            sf.write(jobs[i])
            if jobs[i].endswith("\n"):
                sf.write("fi\n")
            else:
                sf.write("\nfi\n")
            sf.write("\n")

    if conda_env != None:
        with open(script_name, "a") as sf:
            sf.write("conda deactivate \n")
            sf.write("\n")


def setUpPELEForMarenostrum(
    jobs,
    general_script="pele_slurm.sh",
    scripts_folder="pele_slurm_scripts",
    print_name=False,
    partition="gp_bscls",
    **kwargs,
):
    """
    Creates submission scripts for Marenostrum for each PELE job inside the jobs variable.

    Parameters
    ==========
    jobs : list
        Commands for run PELE. This is the output of the setUpPELECalculation() function.
    """

    if not os.path.exists(scripts_folder):
        os.mkdir(scripts_folder)

    if not general_script.endswith(".sh"):
        general_script += ".sh"

    zfill = len(str(len(jobs)))
    with open(general_script, "w") as ps:
        for i, job in enumerate(jobs):
            job_name = str(i + 1).zfill(zfill) + "_" + job.split("\n")[0].split("/")[1]
            singleJob(
                job,
                job_name=job_name,
                script_name=scripts_folder + "/" + job_name + ".sh",
                program="pele",
                partition=partition,
                **kwargs,
            )
            if print_name:
                ps.write("echo Launching job " + job_name + "\n")
            ps.write("sbatch " + scripts_folder + "/" + job_name + ".sh\n")


def singleJob(
    job,
    script_name=None,
    job_name=None,
    ntasks=1,
    cpus_per_task=112,
    gpus=1,
    mem_per_cpu=None,
    highmem=False,
    partition=None,
    threads=None,
    output=None,
    mail=None,
    time=None,
    pythonpath=None,
    account="bsc72",
    modules=None,
    conda_env=None,
    unload_modules=None,
    program=None,
    conda_eval_bash=False,
    pathMN=None,
    exports=None,
):

    # Default: do not purge modules. The program-specific branches below may
    # override this (e.g. program=="pele" sets module_purge=True). Without
    # this initialisation, calling singleJob without `program` triggered an
    # UnboundLocalError at the `if module_purge:` block downstream.
    module_purge = False

    # Check PYTHONPATH variable
    if pythonpath == None:
        pythonpath = []

    if pathMN == None:
        pathMN = []

    available_programs = ["pele", "bioml"]
    if program != None:
        if program not in available_programs:
            raise ValueError(
                "Program not found. Available progams: " + " ,".join(available_programs)
            )

    if program == "pele":
        module_purge = True
        if modules == None:
            modules = []
        modules += modules + [
            "intel",
            "impi",
            "mkl",
            "cmake",
            "boost",
            "anaconda",
            "bsc",
            "transfer",
        ]
        conda_eval_bash = True
        conda_env = "/gpfs/projects/bsc72/conda_envs/platform"

    if program == 'bioml':
        bioml_modules = ["anaconda", "perl/5.38.2"]
        module_purge = True
        if modules == None:
            modules = bioml_modules
        else:
            modules += bioml_modules
        conda_eval_bash = True
        conda_env = "/gpfs/projects/bsc72/conda_envs/bioml"

    available_partitions = ["acc_debug", "acc_bscls", "gp_debug", "gp_bscls"]
    if job_name == None:
        raise ValueError("job_name == None. You need to specify a name for the job")
    if output == None:
        output = job_name
    if partition == None:
        raise ValueError(
            "You must select a partion. Available partitions are:"
            + ", ".join(available_partitions)
        )
    if partition not in available_partitions:
        raise ValueError(
            "Wrong partition selected. Available partitions are:"
            + ", ".join(available_partitions)
        )
    if script_name == None:
        script_name = "slurm_job.sh"
    if modules != None:
        if isinstance(modules, str):
            modules = [modules]
        if not isinstance(modules, list):
            raise ValueError(
                "Modules to load must be given as a list or as a string (for loading one module only)"
            )
    if unload_modules != None:
        if isinstance(unload_modules, str):
            unload_modules = [unload_modules]
        if not isinstance(unload_modules, list):
            raise ValueError(
                "Modules to unload must be given as a list or as a string (for unloading one module only)"
            )
    if conda_env != None:
        if not isinstance(conda_env, str):
            raise ValueError("The conda environment must be given as a string")

    if exports != None:
        if isinstance(exports, str):
            exports = [exports]
        if not isinstance(exports, list):
            raise ValueError(
                "Exports to load must be given as a list or as a string (for loading one export only)"
            )

    if isinstance(time, int):
        time = (time, 0)
    if partition in ["gp_debug", "acc_debug"] and time == None:
        time = (2, 0)
    elif partition in ["gp_debug", "acc_debug"] and time != None:
        if time[0] * 60 + time[1] > 120:
            print("Setting time at maximum allowed for the debug partition (2 hours).")
            time = (2, 0)
    elif partition in ["gp_bscls", "acc_bscls"] and time == None:
        time = (48, 0)
    elif partition in ["gp_bscls", "acc_bscls"] and time != None:
        if time[0] * 60 + time[1] > 2880:
            print(
                "Setting time at maximum allowed for the bsc_ls partition (48 hours)."
            )
            time = (48, 0)

    # Write jobs as array
    with open(script_name, "w") as sf:
        sf.write("#!/bin/bash\n")
        sf.write("#SBATCH --job-name=" + job_name + "\n")
        sf.write("#SBATCH --qos=" + partition + "\n")
        sf.write("#SBATCH --time=" + str(time[0]) + ":" + str(time[1]) + ":00\n")
        sf.write("#SBATCH --ntasks " + str(ntasks) + "\n")
        sf.write("#SBATCH --cpus-per-task " + str(cpus_per_task) + "\n")
        sf.write("#SBATCH --account=" + account + "\n")
        if "acc" in partition:
            sf.write("#SBATCH --gres gpu:" + str(gpus) + "\n")
        #     cpus = gpus*20
        #     sf.write("#SBATCH --cpus-per-task" + str(cpus) + "\n")
        # Have to check if these work
        # ---
        if highmem:
            sf.write("#SBATCH --constraint=highmem\n")
        if mem_per_cpu != None:
            sf.write("#SBATCH --mem-per-cpu " + str(mem_per_cpu) + "\n")
        if threads != None:
            sf.write("#SBATCH -c " + str(threads) + "\n")
        sf.write("#SBATCH --output=" + output + "_%a_%A.out\n")
        sf.write("#SBATCH --error=" + output + "_%a_%A.err\n")
        if mail != None:
            sf.write("#SBATCH --mail-user=" + mail + "\n")
            sf.write("#SBATCH --mail-type=END,FAIL\n")
        sf.write("\n")
        # ---

        if unload_modules != None:
            for module in unload_modules:
                sf.write("module unload " + module + "\n")
            sf.write("\n")

        if module_purge:
            sf.write("module purge \n")
            for module in modules:
                sf.write("module load " + module + "\n")
            sf.write("\n")
        if conda_eval_bash:
            sf.write('eval "$(conda shell.bash hook)"\n')
        if conda_env != None:
            sf.write("source activate " + conda_env + "\n")
            sf.write("\n")

        if exports != None:
            for export in exports:
                sf.write(f"export {export}\n")
            sf.write("\n")

        for pp in pythonpath:
            sf.write("export PYTHONPATH=$PYTHONPATH:" + pp + "\n")
            sf.write("\n")

        for pp in pathMN:
            sf.write("export PATH=$PATH:" + pp + "\n")
            sf.write("\n")

    with open(script_name, "a") as sf:
        sf.write(job)
        if not job.endswith("\n"):
            sf.write("\n\n")
        else:
            sf.write("\n")

    if conda_env != None:
        with open(script_name, "a") as sf:
            sf.write("conda deactivate \n")
            sf.write("\n")
