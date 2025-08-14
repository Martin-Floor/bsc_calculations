import os


def jobArrays(
    jobs,
    script_name=None,
    job_name=None,
    ntasks=1,
    gpus=1,
    mem_per_cpu=None,
    highmem=False,
    partition="gp_bscls",
    cpus_per_task=None,
    output=None,
    mail=None,
    time=48,
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

    sbatch_time, time = _normalize_time(partition, time)

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

    # Normalize new/changed params
    if extras is None:
        extras = []

    if sources is not None and isinstance(sources, str):
        sources = [sources]

    #! Programs
    available_programs = ["gromacs", "alphafold", "hmmer", "asitedesign", "blast", "pyrosetta", "openmm","Q6",
                          "bioml", "rosetta", 'bioemu','PLACER', 'RFDiffusion', 'bioemu_af', 'cp2k']

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
            gromacs_jobs.append(job.replace('mdrun', f'mdrun -pin on -pinoffset 0'))
        jobs = gromacs_jobs

    if program == 'openmm':
        openmm_modules = ["anaconda", "cuda/11.8"]
        if modules == None:
            modules = openmm_modules
        else:
            modules += openmm_modules

        conda_env = "/gpfs/projects/bsc72/conda_envs/openmm_cuda"


    if program == 'bioml':
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

    if program == 'blast':
        if modules == None:
            modules = ["blast"]
        else:
            modules += ["blast"]

    if program == 'pyrosetta':
        pyrosetta_modules = []
        if modules == None:
            modules = pyrosetta_modules
        else:
            modules += pyrosetta_modules
        if mpi:
            conda_env = "/gpfs/projects/bsc72/conda_envs/mood"
        else:
            conda_env = "/gpfs/projects/bsc72/MN4/bsc72/conda_envs/pyrosetta"

    if program == "hmmer":
        hmmer_modules = ["anaconda"]
        if modules == None:
            modules = hmmer_modules
        else:
            modules += hmmer_modules
        conda_env = "/gpfs/projects/bsc72/conda_envs/hmm"

    if program == 'Q6':
        q6_modules = ['oneapi','q6']
        if modules == None:
            modules = q6_modules
        else:
            modules += q6_modules

        extras = ['source /home/bsc/bsc072181/programs/qtools/bin/qtools/qtools_init.sh']

    if program == "asitedesign":
        if modules == None:
            modules = ["anaconda", "intel", "openmpi", "mkl", "gcc", "bsc"]
        else:
            modules += ["anaconda", "intel", "openmpi", "mkl", "gcc", "bsc"]
        pythonpath.append("/gpfs/projects/bsc72/Repos/AsiteDesign")
        pathMN.append("/gpfs/projects/bsc72/Repos/AsiteDesign")
        conda_env = "/gpfs/projects/bsc72/conda_envs/asite"

    if program == 'rosetta':
        rosetta_modules = ['gcc/12.3.0', 'rosetta/3.14']
        if modules == None:
            modules = rosetta_modules
        else:
            modules += rosetta_modules

    if program == 'bioemu':
        if modules == None:
            modules = ["anaconda"]
        else:
            modules += ["anaconda"]
        conda_env = '/gpfs/projects/bsc72/conda_envs/bioemu2'
        if exports == None:
            exports = []
        exports += ['COLABFOLD_DIR=/gpfs/projects/bsc72/conda_envs/bioemu2/colabfold']

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
            sf.write("#SBATCH --cpus-per-task " + str(cpus) +  "\n")
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

        if program == 'RFDiffusion':
            if 'SCRIPT_PATH' in jobs[i]:
                jobs[i] = jobs[i].replace('SCRIPT_PATH', '/gpfs/projects/bsc72/RFdiffusion/scripts')

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
    partition='gp_bscls',
    **kwargs
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
                **kwargs
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
