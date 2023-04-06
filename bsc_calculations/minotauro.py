def jobArrays(jobs, script_name=None, job_name=None, cpus_per_task=40, gpus=1, ntasks=1,
              nodes=1, output=None, mail=None, time=48, modules=None, conda_env=None, constraint=None,
              unload_modules=None, program=None, pythonpath=None, partition='bsc_ls', purge=False,
              group_jobs_by=None):

    """
    Set up job array scripts for marenostrum slurm job manager.

    Parameters
    ==========
    jobs : list
        List of jobs. Each job is a string representing the command to execute.
    script_name : str
        Name of the SLURM submission script.
    """

    available_programs = ['openmm', 'alphafold']
    available_partitions = ['debug', 'bsc_ls']

    if isinstance(jobs, str):
        jobs = [jobs]

    if job_name == None:
        raise ValueError('job_name == None. You need to specify a name for the job')
    if output == None:
        output = job_name

    # Group jobs to enter in the same job array (useful for launching many short
    # jobs when there are a max_job_allowed limit per user.)
    if isinstance(group_jobs_by, int):
        grouped_jobs = []
        gj = ''
        for i,j in enumerate(jobs):
            gj += j
            if  (i+1) % groups_jobs_by == 0:
                grouped_jobs.append(gj)
                gj = ''
        if gj != '':
            grouped_jobs.append(gj)
        jobs = grouped_jobs

    elif not isinstance(group_jobs_by, type(None)):
        raise ValueError('You must give an integer to group jobs by this number.')

    if partition not in available_partitions:
        raise ValueError('Wrong partition set up selected. Available partitions are: '+
                         ', '.join(available_partitions))

    if program not in available_programs and program != None:
        raise ValueError('Wrong program set up selected. Available programs are: '+
                         ', '.join(available_programs))
    if program == 'openmm':
        if modules == None:
            modules = ['openmpi/3.0.0', 'python/3.6.5', ]
        else:
            modules += ['openmpi/3.0.0', 'python/3.6.5']
        if pythonpath == None:
            pythonpath = ['/home/bsc72/bsc72523/Programs/sbm-openmm/compiled/lib/python3.6/site-packages']
        else:
            pythonpath += ['/home/bsc72/bsc72523/Programs/sbm-openmm/compiled/lib/python3.6/site-packages']

    if program == 'alphafold':
        purge = True
        if modules == None:
            modules = ['singularity', 'alphafold']
        else:
            modules += ['singularity', 'alphafold']
        cpus_per_task = 16
        ntasks = 1
        gpus = 2
        constraint = 'k80'

    if script_name == None:
        script_name = 'slurm_array.sh'
    if modules != None:
        if isinstance(modules, str):
            modules = [modules]
        if not isinstance(modules, list):
            raise ValueError('Modules to load must be given as a list or as a string (for loading one module only)')
    if unload_modules != None:
        if isinstance(unload_modules, str):
            unload_modules = [unload_modules]
        if not isinstance(unload_modules, list):
            raise ValueError('Modules to unload must be given as a list or as a string (for unloading one module only)')
    if conda_env != None:
        if not isinstance(conda_env, str):
            raise ValueError('The conda environment must be given as a string')

    if partition == 'debug':
        time = 1
    elif partition == 'bsc_ls':
        if time > 48:
            print('Setting time at maximum allowed for the bsc_ls partition (48 hours).')
            time=48

    #Write jobs as array
    with open(script_name,'w') as sf:
        sf.write('#!/bin/bash\n')
        sf.write('#SBATCH --job-name='+job_name+'\n')
        sf.write('#SBATCH --qos='+partition+'\n')
        sf.write('#SBATCH --time='+str(time)+':00:00\n')
        sf.write('#SBATCH --cpus-per-task='+str(cpus_per_task)+'\n')
        sf.write('#SBATCH --nodes='+str(nodes)+'\n')
        sf.write('#SBATCH --gres gpu:'+str(gpus)+'\n')
        sf.write('#SBATCH --ntasks='+str(ntasks)+'\n')
        sf.write('#SBATCH --array=1-'+str(len(jobs))+'\n')
        if constraint:
            sf.write('#SBATCH --constraint='+constraint+'\n')
        sf.write('#SBATCH --output='+output+'_%a_%A.out\n')
        sf.write('#SBATCH --error='+output+'_%a_%A.err\n')
        if mail != None:
            sf.write('#SBATCH --mail-user='+mail+'\n')
            sf.write('#SBATCH --mail-type=END,FAIL\n')
        sf.write('\n')

        if purge:
            sf.write('module purge\n')
        if unload_modules != None:
            for module in unload_modules:
                sf.write('module unload '+module+'\n')
            sf.write('\n')
        if modules != None:
            for module in modules:
                sf.write('module load '+module+'\n')
            sf.write('\n')
        if conda_env != None:
            sf.write('source activate '+conda_env+'\n')
            sf.write('\n')
        if pythonpath != None:
            for pp in pythonpath:
                sf.write('export PYTHONPATH=$PYTHONPATH:'+pp+'\n')
                sf.write('\n')

    for i in range(len(jobs)):
        with open(script_name,'a') as sf:
            sf.write('if [[ $SLURM_ARRAY_TASK_ID = '+str(i+1)+' ]]; then\n')
            sf.write(jobs[i])
            if jobs[i].endswith('\n'):
                sf.write('fi\n')
            else:
                sf.write('\nfi\n')
            sf.write('\n')

    if conda_env != None:
        with open(script_name,'a') as sf:
            sf.write('conda deactivate \n')
            sf.write('\n')

def singleJob(job, script_name=None, job_name=None, partition='class_a', cpus=24, time=1,
              gpus=1, output=None, mail=None, modules=None, conda_env=None, graphical_job=False):

    available_partitions = ['bsc_ls', 'debug', ]

    if partition not in available_partitions:
        raise ValueError('Incorrect partition selected for Minotauro cluster')

    if job_name == None:
        raise ValueError('job_name == None. You need to specify a name for the job')
    if output == None:
        output = job_name
    if script_name == None:
        script_name = 'sub_script.sh'
    if mail == None:
        mail = 'martinfloor@gmail.com'
    if modules != None:
        if isinstance(modules, str):
            modules = [modules]
        if not isinstance(modules, list):
            raise ValueError('Modules to load must be given as a list or as a string (for loading one module only)')

    if conda_env != None:
        if not isinstance(conda_env, str):
            raise ValueError('The conda environment must be given as a string')

    if partition == 'debug':
        time = 2
    elif partition == 'bsc_ls':
        if time > 48:
            print('Setting time at maximum allowed for the bsc_ls partition (48 hours).')
            time=48

    #Write slurm script
    with open(script_name,'w') as sf:
        sf.write('#!/bin/bash\n')
        sf.write('#SBATCH --job-name='+job_name+'\n')
        sf.write('#SBATCH --partition='+partition+'\n')
        sf.write('#SBATCH --time='+str(time)+':00:00\n')
        sf.write('#SBATCH -n '+str(cpus)+'\n')
        sf.write('#SBATCH --output='+output+'_%a_%A.out\n')
        sf.write('#SBATCH --error='+output+'_%a_%A.err\n')
        sf.write('#SBATCH --mail-user='+mail+'\n')
        sf.write('#SBATCH --mail-type=END,FAIL\n')
        sf.write('#@ gpus_per_node = '+str(gpus)+'\n')
        sf.write('#@ X11 = '+str(int(graphical_job))+'\n')
        sf.write('\n')

        if modules != None:
            for module in modules:
                sf.write('module load '+module+'\n')
            sf.write('\n')

        if conda_env != None:
            sf.write('source activate '+conda_env+'\n')
            sf.write('\n')

        sf.write(job+'\n')

        if conda_env != None:
            sf.write('conda deactivate \n')
            sf.write('\n')
