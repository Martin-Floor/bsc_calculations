def jobArrays(jobs, script_name=None, job_name=None, cpus=1, mem_per_cpu=None,
              partition='bsc_ls', threads=None, output=None, mail=None, time=48, export=None,
              modules=None, conda_env=None, unload_modules=None, program='schrodinger'):
    """
    Set up job array scripts for marenostrum slurm job manager.

    Parameters
    ==========
    jobs : list
        List of jobs. Each job is a string representing the command to execute.
    script_name : str
        Name of the SLURM submission script.
    """

    available_partitions = ['debug', 'bsc_ls']
    available_programs = ['schrodinger']

    if program not in available_programs and program != None:
        raise ValueError('Wrong program set up selected. Available programs are: '+
                         ', '.join(available_programs))

    if program == 'schrodinger':
        export = [ 'LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/gpfs/projects/bsc72/Link_for_Schrodinger2021',
                   'LIBRARY_PATH=$LIBRARY_PATH:/gpfs/projects/bsc72/Link_for_Schrodinger2021',
                   'PATH=$PATH:/gpfs/projects/bsc72/Link_for_Schrodinger2021',
                   'LD_PRELOAD=/lib64/libcrypto.so.1.1.1k',
                   'PATH=$PATH:/gpfs/projects/bsc72/Schrodinger2021-4_2',
                   'SCHRODINGER=/gpfs/projects/bsc72/Schrodinger2021-4_2']

    if job_name == None:
        raise ValueError('job_name == None. You need to specify a name for the job')
    if output == None:
        output = job_name
    if partition not in available_partitions:
        raise ValueError('Wrong partition selected. Available partitions are:'+
                         ', '.join(available_partitions))
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
        time = 2
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
        if program == 'schrodinger':
            sf.write('#SBATCH --constraint=schrodinger\n')
        sf.write('#SBATCH -n '+str(cpus)+'\n')
        if mem_per_cpu != None:
            sf.write('#SBATCH --mem-per-cpu '+str(mem_per_cpu)+'\n')
        if threads != None:
            sf.write('#SBATCH -c '+str(threads)+'\n')
        sf.write('#SBATCH --array=1-'+str(len(jobs))+'\n')
        sf.write('#SBATCH --output='+output+'_%a_%A.out\n')
        sf.write('#SBATCH --error='+output+'_%a_%A.err\n')
        if mail != None:
            sf.write('#SBATCH --mail-user='+mail+'\n')
            sf.write('#SBATCH --mail-type=END,FAIL\n')
        sf.write('\n')

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

        for e in export:
            sf.write('export '+e+'\n')
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
