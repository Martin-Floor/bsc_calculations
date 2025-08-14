import os


def jobArrays(
    jobs,
    account='bsc72',
    script_name=None,
    job_name=None,
    tasks=1,
    cpus_per_task=1,
    mem_per_cpu=None,
    highmem=False,
    partition="bsc_ls",
    threads=None,
    output=None,
    mail=None,
    time=48,
    module_purge=False,
    modules=None,
    conda_env=None,
    unload_modules=None,
    program=None,
    conda_eval_bash=False,
    jobs_range=None,
    group_jobs_by=None,
    mpi=False,
    pythonpath=None,
    pathMN=None,
    exports=None,
    sources=None,
):
    """
    Generate a Slurm **job array** submission script tailored for BSC clusters
    (e.g., Nord4 / MareNostrum family) and common scientific stacks (Rosetta,
    PyRosetta, Foldseek, CP2K, etc.).

    This utility writes a single `bash` script that:
      * Sets Slurm job headers for account, QoS/partition, wall time, resources.
      * Optionally loads/unloads environment **modules**.
      * Optionally activates a **conda** environment.
      * Exports additional environment variables (e.g., CP2K_DATA_DIR).
      * Extends `PYTHONPATH` and `PATH` as requested.
      * Dispatches one *shell command* per **array task** based on `jobs`.

    Parameters
    ----------
    jobs : list[str] or str
        Commands to run. Each element is a shell snippet executed when the
        corresponding array index is active. If a single string is provided,
        it is converted to a one‑element list.
        Example elements:
            - "cp2k.psmp -i run.inp -o run.out\\n"
            - "python script.py arg1 arg2 && echo DONE\\n"

    account : str, default='bsc72'
        Slurm account (project code) used in `#SBATCH --account`.

    script_name : str or None, default=None
        Output filename for the submission script. If `None`, uses
        `"slurm_array.sh"`. If provided without `.sh`, the suffix is added.

    job_name : str or None, default=None
        Slurm job name (`#SBATCH --job-name`). **Required**.

    tasks : int, default=1
        `#SBATCH --ntasks`. For MPI jobs, set >1 (and usually combine with
        `cpus_per_task` for hybrid MPI+OpenMP).

    cpus_per_task : int, default=1
        `#SBATCH --cpus-per-task`. For threaded codes, set >1 to allocate
        OpenMP threads per MPI rank.

    mem_per_cpu : int or None, default=None
        `#SBATCH --mem-per-cpu` in MB. If `None`, do not set.

    highmem : bool, default=False
        If `True`, adds `#SBATCH --constraint=highmem`.

    partition : {"debug", "bsc_ls"}, default="bsc_ls"
        **Cluster QoS/partition tag** written to `#SBATCH --qos`. On some
        systems you may need `--partition` instead; this function uses `--qos`
        consistently and validates against these two names.

    threads : int or None, default=None
        Additional shorthand for Slurm `-c` (threads). If not `None`,
        emits `#SBATCH -c <threads>`. Usually redundant when using
        `cpus_per_task`.

    output : str or None, default=None
        Base name for `#SBATCH --output/--error` files. If `None`, falls back
        to `job_name`. Files are formatted as `<output>_%a_%A.out` and
        `<output>_%a_%A.err`.

    mail : str or None, default=None
        If set, adds email notifications:
        `#SBATCH --mail-user=<mail>` and `--mail-type=END,FAIL`.

    time : int or tuple[int, int], default=48
        Wall time. If `int`, interpreted as hours (H). If a tuple `(H, M)`,
        interpreted as hours and minutes. The function caps to typical limits:
        - `debug`: ≤ 2 hours
        - `bsc_ls`: ≤ 48 hours

    module_purge : bool, default=False
        If `True`, writes `module purge` before loading/unloading modules.

    modules : list[str] or str or None, default=None
        Modules to `module load`. You can pass a single string or a list.
        The function may append program‑specific modules (see `program`).

    conda_env : str or None, default=None
        Absolute path (or name) of conda env to activate via
        `source activate <conda_env>`. If `conda_eval_bash=True`, the script
        will first evaluate `conda shell.bash hook`.

    unload_modules : list[str] or str or None, default=None
        Modules to `module unload` before loading new ones.

    program : {None, "rosetta","pyrosetta","pml","netsolp","blast","msd",
               "alphafold","hmmer","asitedesign","foldseek","cp2k"}, default=None
        Convenience presets that append recommended modules/paths/envs.
        Examples:
        - `"cp2k"`: loads `ANACONDA`, sets `conda_env` to
          `/gpfs/projects/bsc72/conda_envs/cp2k_env`, and exports
          `CP2K_DATA_DIR=/gpfs/projects/bsc72/conda_envs/cp2k_env/share/cp2k/data`.
        - `"foldseek"`: loads `ANACONDA`, sets `conda_env` accordingly.
        - Several others have “needs update” notes for N4; the function sets
          their typical modules/paths if selected.

    conda_eval_bash : bool, default=False
        If `True`, add `eval "$(conda shell.bash hook)"` before `source activate`.

    jobs_range : tuple[int, int] or list[int] or None, default=None
        One‑based inclusive slice of `jobs` to submit as the array, e.g.
        `(101, 200)` selects the 100 jobs with original indices 101..200.
        Useful to re‑queue in chunks. Internally reindexes the Slurm array
        from 1..N for the selected slice.

    group_jobs_by : int or None, default=None
        If set to an integer `k`, concatenates `k` consecutive commands into
        a **single** array element. Handy for thousands of very short jobs
        when user array limits apply. The concatenation is literal: commands
        are appended in order; ensure each ends with `\\n` or `;` as needed.

    mpi : bool, default=False
        Hint for certain `program` presets (e.g., `pyrosetta`) to choose a
        specific conda env for MPI builds. The function does not itself add
        `mpirun`—you must include it in your `jobs` commands if needed.

    pythonpath : list[str] or None, default=None
        Paths appended to `PYTHONPATH` in the script.

    pathMN : list[str] or None, default=None
        Paths appended to `PATH` in the script.

    exports : list[str] or None, default=None
        Raw `export` entries added verbatim, e.g.,
        `["OMP_NUM_THREADS=8", "MKL_NUM_THREADS=8"]`.

    Returns
    -------
    None
        Writes the Slurm script to disk (`script_name`) and prints messages
        when time or CPU settings are coerced for partition limits.

    Side Effects
    ------------
    * Creates or overwrites `script_name`.
    * Prints warnings when capping time/CPU for `debug` or `bsc_ls`.

    Notes
    -----
    * This function writes `#SBATCH --qos=<partition>`. On some BSC systems,
      you may need to use `--partition` instead of `--qos`. Adjust the code
      or the cluster’s submission policy accordingly.
    * For hybrid MPI+OpenMP workloads (e.g., `cp2k.psmp`), a typical Slurm
      configuration is:
          `--ntasks=<MPI ranks>`, `--cpus-per-task=<OMP threads per rank>`,
      and inside your job command:
          `export OMP_NUM_THREADS=$SLURM_CPUS_PER_TASK`
      followed by `mpirun -np $SLURM_NTASKS ...`.
      This function does not inject `mpirun`—put the exact launch line in
      each `jobs[i]` command as appropriate for your code.

    Examples
    --------
    Basic: one command per array task
    >>> jobArrays(
    ...     jobs=["python run_case.py --id 1\\n", "python run_case.py --id 2\\n"],
    ...     job_name="my_array",
    ...     script_name="submit.sh",
    ...     partition="debug",
    ...     tasks=1,
    ...     cpus_per_task=4,
    ...     conda_env="/gpfs/projects/bsc72/conda_envs/myenv",
    ... )

    Submit a **CP2K** hybrid job array (MPI+OpenMP), 4 ranks × 8 threads:
    >>> jobs = [
    ...   "export OMP_NUM_THREADS=$SLURM_CPUS_PER_TASK\\n"
    ...   "mpirun -np $SLURM_NTASKS cp2k.psmp -i input_${SLURM_ARRAY_TASK_ID}.inp "
    ...   "-o output_${SLURM_ARRAY_TASK_ID}.out\\n"
    ... ]
    >>> jobArrays(
    ...     jobs=jobs,
    ...     job_name="cp2k_scan",
    ...     script_name="cp2k_array.sh",
    ...     partition="bsc_ls",
    ...     tasks=4,
    ...     cpus_per_task=8,
    ...     program="cp2k",  # sets ANACONDA + CP2K env & CP2K_DATA_DIR
    ... )

    Restart a subset with reindexed array (jobs 101..200 only)
    >>> jobArrays(
    ...     jobs=[f"python run.py --case {i}\\n" for i in range(1, 1001)],
    ...     job_name="restart_block",
    ...     jobs_range=(101, 200),
    ...     partition="bsc_ls",
    ... )

    Group 10 very short commands per array element
    >>> jobArrays(
    ...     jobs=[f"do_short_thing {i};\\n" for i in range(1, 1001)],
    ...     job_name="batched",
    ...     group_jobs_by=10,
    ...     partition="debug",
    ... )
    """

    # Check input
    if isinstance(jobs, str):
        jobs = [jobs]

    # Check input
    if jobs_range != None:
        if (
            not isinstance(jobs_range, (list, tuple))
            or len(jobs_range) != 2
            or not all([isinstance(x, int) for x in jobs_range])
        ):
            raise ValueError(
                "The given jobs_range must be a tuple or a list of 2-integers"
            )

    # Group jobs to enter in the same job array (useful for launching many short
    # jobs when there are a max_job_allowed limit per user.)
    if isinstance(group_jobs_by, int):
        grouped_jobs = []
        gj = ""
        for i, j in enumerate(jobs):
            gj += j
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

    available_programs = [
        "rosetta",
        "pyrosetta",
        "pml",
        "netsolp",
        "blast",
        "msd",
        "alphafold",
        "hmmer",
        "asitedesign",
        'foldseek',
        # 'cp2k'
    ]
    if program != None:
        if program not in available_programs:
            raise ValueError(
                "Program not found. Available progams: " + " ,".join(available_programs)
            )

    if program == "rosetta": # Updated for N4s
        rosetta_modules = ["gcc", "rosetta/3.13"]
        if modules == None:
            modules = rosetta_modules
        else:
            modules += rosetta_modules

    if program == "pyrosetta": # Needs update for N4
        pyrosetta_modules = ["anaconda"]
        if modules == None:
            modules = pyrosetta_modules
        else:
            modules += pyrosetta_modules
        if mpi:
            conda_env = "/gpfs/projects/bsc72/masoud/conda/envs/EDesignTools-MKL"
        else:
            conda_env = "/gpfs/projects/bsc72/conda_envs/pyrosetta"

    if program == "pml": # Needs update for N4
        pml_modules = ["anaconda"]
        if modules == None:
            modules = pml_modules
        else:
            modules += pml_modules
        conda_env = "/gpfs/projects/bsc72/conda_envs/pml"

    if program == "msd": # Needs update for N4
        msd_modules = [
            "anaconda",
            "intel/2021.4",
            "impi/2021.4",
            "mkl/2021.4",
            "rosetta/3.13",
        ]
        if modules == None:
            modules = msd_modules
        else:
            modules += msd_modules
        conda_env = "/gpfs/projects/bsc72/conda_envs/msd_"+msd_version

    if program == "netsolp": # Needs update for N4
        netsolp_modules = ["anaconda"]
        if modules == None:
            modules = netsolp_modules
        else:
            modules += netsolp_modules
        conda_env = "/gpfs/projects/bsc72/conda_envs/netsolp"

        for i, job in enumerate(jobs):
            if "NETSOLP_PATH" in jobs[i]:
                jobs[i] = jobs[i].replace(
                    "NETSOLP_PATH", "\/gpfs\/projects\/bsc72\/programs\/netsolp-1.0"
                )

    if program == "blast": # Needs update for N4
        blast_modules = ["blast"]
        if modules == None:
            modules = blast_modules
        else:
            modules += blast_modules

    available_partitions = ["debug", "bsc_ls"]

    if program == "alphafold":
        c

    if program == "hmmer": # Needs update for N4
        hmmer_modules = ["anaconda"]
        if modules == None:
            modules = hmmer_modules
        else:
            modules += hmmer_modules
        conda_env = "/gpfs/projects/bsc72/conda_envs/hmm"

    if program == "asitedesign": # Needs update for N4
        if modules == None:
            modules = [
                "anaconda",
                "mkl",
                "bsc/1.0",
                "gcc/10.1.0" "openmpi/4.1.3",
            ]
        else:
            modules += [
                "anaconda",
                "mkl",
                "bsc/1.0",
                "gcc/10.1.0" "openmpi/4.1.3",
            ]
        pythonpath.append("/gpfs/projects/bsc72/MN4/bsc72/masoud/EDesign_V4")
        pathMN.append("/gpfs/projects/bsc72/MN4/bsc72/masoud/EDesign_V4")
        conda_env = "/gpfs/projects/bsc72/MN4/bsc72/masoud/conda/envs/EDesignTools-MKL"

    if program == 'foldseek': # Updated for N4
        if modules == None:
            modules = ['ANACONDA']
        else:
            modules += ['ANACONDA']
        conda_env = "/gpfs/projects/bsc72/conda_envs/foldseek"

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
            'OMP_NUM_THREADS=${SLURM_CPUS_PER_TASK}',
            'OMP_PROC_BIND=spread',
            'OMP_PLACES=cores',       # literal "cores", not a variable
            'MKL_NUM_THREADS=${OMP_NUM_THREADS}',
            'MKL_DYNAMIC=FALSE'
        ]
        if sources is None:
            sources = []
        sources.append('/gpfs/projects/bsc72/Programs/cp2k-2025.2/tools/toolchain/install/setup')
        pathMN.append("/gpfs/projects/bsc72/Programs/cp2k-2025.2.clean/exe/local")

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

    if not isinstance(script_name, str):
        raise ValueError("script_name must be a string")

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

    if isinstance(time, int):
        time = (time, 0)
    if partition == "debug" and cpus_per_task > 64:
        cpus_per_task = 64
        print("Setting cpus at maximum allowed for the debug partition (64)")

    if partition == "debug" and time == None:
        time = (2, 0)
    elif partition == "debug" and time != None:
        if time[0] * 60 + time[1] > 120:
            print("Setting time at maximum allowed for the debug partition (2 hours).")
            time = (2, 0)
    elif partition == "bsc_ls" and time == None:
        time = (48, 0)
    elif partition == "bsc_ls" and time != None:
        if time[0] * 60 + time[1] > 2880:
            print(
                "Setting time at maximum allowed for the bsc_ls partition (48 hours)."
            )
            time = (48, 0)

    # Slice jobs if a range is given
    if jobs_range != None:
        jobs = jobs[jobs_range[0] - 1 : jobs_range[1]]

    # Write jobs as array
    with open(script_name, "w") as sf:
        sf.write("#!/bin/bash\n")
        sf.write("#SBATCH --account=" + account + "\n")
        sf.write("#SBATCH --job-name=" + job_name + "\n")
        sf.write("#SBATCH --qos=" + partition + "\n")
        sf.write("#SBATCH --time=" + str(time[0]) + ":" + str(time[1]) + ":00\n")
        if tasks:
            sf.write("#SBATCH --ntasks " + str(tasks) + "\n")
        if cpus_per_task:
            sf.write("#SBATCH --cpus-per-task " + str(cpus_per_task) + "\n")
        if highmem:
            sf.write("#SBATCH --constraint=highmem\n")
        if mem_per_cpu != None:
            sf.write("#SBATCH --mem-per-cpu " + str(mem_per_cpu) + "\n")
        if threads != None:
            sf.write("#SBATCH -c " + str(threads) + "\n")
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
        if sources is not None:
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

        for pp in pythonpath:
            sf.write("export PYTHONPATH=$PYTHONPATH:" + pp + "\n")
            sf.write("\n")

        for pp in pathMN:
            sf.write("export PATH=$PATH:" + pp + "\n")
            sf.write("\n")

    for i in range(len(jobs)):
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

def singleJob(
    job,
    script_name=None,
    job_name=None,
    account='bsc72',
    cpus=96,
    mem_per_cpu=None,
    partition=None,
    threads=None,
    output=None,
    mail=None,
    time=None,
    purge=False,
    modules=None,
    tasks=None,
    cpus_per_task=None,
    conda_env=None,
    unload_modules=None,
    program=None,
    conda_eval_bash=False,
    exports=None,
):
    available_programs = ["pele", "pyrosetta", "pml", "netsolp"]
    if program != None:
        if program not in available_programs:
            raise ValueError(
                "Program not found. Available progams: " + " ,".join(available_programs)
            )

    if program == "pele":
        purge = True
        if modules == None:
            modules = []
        modules += modules + [
                'ANACONDA',
                'intel',
                'impi',
                'mkl',
                'boost/1.64.0-mpi']
        conda_eval_bash = True
        conda_env = "/gpfs/projects/bsc72/conda_envs/platform"
        if exports == None:
            exports = []
        exports += exports + [
            "PELE_EXEC=/gpfs/projects/bsc72/MN4/bsc72/PELE++/mniv/1.8.0/bin/PELE_mpi",
            "export PELE_DATA=/gpfs/projects/bsc72/MN4/bsc72/PELE++/mniv/1.8.0/Data",
            "export PELE_DOCUMENTS=/gpfs/projects/bsc72/MN4/bsc72/PELE++/mniv/1.8.0/Documents"
        ]

    if program == "pyrosetta":
        pyrosetta_modules = ["anaconda"]
        if modules == None:
            modules = pyrosetta_modules
        else:
            modules += pyrosetta_modules
        conda_env = "/gpfs/projects/bsc72/conda_envs/pyrosetta"

    if program == "pml":
        pml_modules = ["anaconda"]
        if modules == None:
            modules = pml_modules
        else:
            modules += pml_modules
        conda_env = "/gpfs/projects/bsc72/conda_envs/pml"

    if program == "netsolp":
        netsolp_modules = ["anaconda"]
        if modules == None:
            modules = netsolp_modules
        else:
            modules += netsolp_modules
        conda_env = "/gpfs/projects/bsc72/conda_envs/netsolp"

        for i, job in enumerate(jobs):
            if "NETSOLP_PATH" in jobs[i]:
                jobs[i] = jobs[i].replace(
                    "NETSOLP_PATH", "\/gpfs\/projects\/bsc72\/programs\/netsolp-1.0"
                )

    available_partitions = ["debug", "bsc_ls"]
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
    if partition == "debug" and cpus > 64:
        cpus = 64
        print("Setting cpus at maximum allowed for the debug partition (64)")

    if partition == "debug" and time == None:
        time = (2, 0)
    elif partition == "debug" and time != None:
        if time[0] * 60 + time[1] > 120:
            print("Setting time at maximum allowed for the debug partition (2 hours).")
            time = (2, 0)
    elif partition == "bsc_ls" and time == None:
        time = (48, 0)
    elif partition == "bsc_ls" and time != None:
        if time[0] * 60 + time[1] > 2880:
            print(
                "Setting time at maximum allowed for the bsc_ls partition (48 hours)."
            )
            time = (48, 0)

    # Write jobs as array
    with open(script_name, "w") as sf:
        sf.write("#!/bin/bash\n")
        sf.write("#SBATCH --account=" + account + "\n")
        sf.write("#SBATCH --job-name=" + job_name + "\n")
        sf.write("#SBATCH --qos=" + partition + "\n")
        sf.write("#SBATCH --time=" + str(time[0]) + ":" + str(time[1]) + ":00\n")
        if tasks:
            sf.write("#SBATCH --ntasks " + str(tasks) + "\n")
        else:
            sf.write("#SBATCH --ntasks " + str(cpus) + "\n")
        if cpus_per_task:
            sf.write("#SBATCH --cpus-per-task " + str(cpus_per_task) + "\n")
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

        if purge:
            sf.write("module purge\n")
        if unload_modules != None:
            for module in unload_modules:
                sf.write("module unload " + module + "\n")
            sf.write("\n")
        if modules != None:
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


def setUpPELEForNord4(
    jobs,
    account='bsc72',
    qos='bsc_ls',
    general_script="pele_slurm.sh",
    scripts_folder="pele_slurm_scripts",
    print_name=False,
    partition="bsc_ls",
    cpus=96,
    time=None,
):
    """
    Creates submission scripts for Marenostrum for each PELE job inside the jobs variable.

    Parameters
    ==========
    jobs : list
        Commands for run PELE. This is the output of the setUpPELECalculation() function.
    """

    if not isinstance(jobs, list):
        raise ValueError("PELE jobs must be given as a list!")

    if not os.path.exists(scripts_folder):
        os.mkdir(scripts_folder)

    zfill = len(str(len(jobs)))
    with open(general_script, "w") as ps:
        for i, job in enumerate(jobs):
            job_name = str(i + 1).zfill(zfill) + "_" + job.split("\n")[0].split("/")[-1]
            singleJob(
                job,
                cpus=cpus,
                partition=partition,
                program="pele",
                time=time,
                job_name=job_name,
                script_name=scripts_folder + "/" + job_name + ".sh",
            )
            if print_name:
                ps.write("echo Launching job " + job_name + "\n")
            ps.write("sbatch -A "+account+' -q '+qos+' '+ scripts_folder + "/" + job_name + ".sh\n")
