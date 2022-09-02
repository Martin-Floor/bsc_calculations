import os

def jobArrays(jobs, script_name=None, job_name=None, cpus=1, mem_per_cpu=None,
              partition='bsc_ls', threads=None, output=None, mail=None, time=48, module_purge=False,
              modules=None, conda_env=None, unload_modules=None, program=None, conda_eval_bash=False):
    """
    Set up job array scripts for marenostrum slurm job manager.

    Parameters
    ==========
    jobs : list
        List of jobs. Each job is a string representing the command to execute.
    script_name : str
        Name of the SLURM submission script.
    """

    available_programs = ['pele', 'peleffy', 'rosetta', 'predig', 'pyrosetta']
    if program != None:
        if program not in available_programs:
            raise ValueError('Program not found. Available progams: '+' ,'.join(available_programs))

    if program == 'pele' or program == 'peleffy':
        if modules == None:
            modules = []
        modules += modules+['ANACONDA/2019.10', 'intel', 'mkl', 'impi', 'gcc', 'boost/1.64.0']
        conda_eval_bash = True
        if program == 'pele':
            conda_env = '/gpfs/projects/bsc72/conda_envs/platform/1.6.3'
        elif program == 'peleffy':
            conda_env = '/gpfs/projects/bsc72/conda_envs/peleffy/1.3.4'

    if program == 'rosetta':
        rosetta_modules = ['gcc/7.2.0', 'impi/2017.4', 'rosetta/3.13']
        if modules == None:
            modules = rosetta_modules
        else:
            modules += rosetta_modules

    if program == 'pyrosetta':
        pyrosetta_modules = ['ANACONDA/2019.10']
        if modules == None:
            modules = pyrosetta_modules
        else:
            modules += pyrosetta_modules
        conda_env = '/gpfs/projects/bsc72/conda_envs/pyrosetta'

    if program == 'predig':
        if modules == None:
            modules = []
        modules += ['miniconda3']
        conda_eval_bash = True
        if program == 'predig':
            conda_env = '/home/bsc72/bsc72040/miniconda3/envs/predig'

    available_partitions = ['debug', 'bsc_ls']

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
        sf.write('#SBATCH --ntasks '+str(cpus)+'\n')
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

        if module_purge:
                sf.write('module purge\n')
        if unload_modules != None:
            for module in unload_modules:
                sf.write('module unload '+module+'\n')
            sf.write('\n')
        if modules != None:
            for module in modules:
                sf.write('module load '+module+'\n')
            sf.write('\n')
        if conda_eval_bash:
            sf.write('eval "$(conda shell.bash hook)"\n')
        if conda_env != None:
            sf.write('source activate '+conda_env+'\n')
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

def setUpPELEForMarenostrum(jobs, general_script='pele_slurm.sh', print_name=False, partition='bsc_ls', cpus=96, time=None):
    """
    Creates submission scripts for Marenostrum for each PELE job inside the jobs variable.

    Parameters
    ==========
    jobs : list
        Commands for run PELE. This is the output of the setUpPELECalculation() function.
    """
    if not os.path.exists('pele_slurm_scripts'):
        os.mkdir('pele_slurm_scripts')

    zfill = len(str(len(jobs)))
    with open(general_script, 'w') as ps:
        for i,job in enumerate(jobs):
            job_name = str(i+1).zfill(zfill)+'_'+job.split('\n')[0].split('/')[-1]
            singleJob(job, cpus=cpus, partition=partition, program='pele', time=time,
                      job_name=job_name, script_name='pele_slurm_scripts/'+job_name+'.sh')
            if print_name:
                ps.write('Launching job '+job_name+'\n')
            ps.write('sbatch pele_slurm_scripts/'+job_name+'.sh\n')

def singleJob(job, script_name=None, job_name=None, cpus=96, mem_per_cpu=None,
              partition=None, threads=None, output=None, mail=None, time=None,
              modules=None, conda_env=None, unload_modules=None, program=None, conda_eval_bash=False):

    available_programs = ['pele', 'rosetta', 'pyrosetta']
    if program != None:
        if program not in available_programs:
            raise ValueError('Program not found. Available progams: '+' ,'.join(available_programs))

    if program == 'pele':
        if modules == None:
            modules = []
        modules += modules+['ANACONDA/2019.10', 'intel', 'mkl', 'impi', 'gcc', 'boost/1.64.0']
        conda_eval_bash = True
        conda_env = '/gpfs/projects/bsc72/conda_envs/platform/1.6.3'

    if program == 'rosetta':
        rosetta_modules = ['gcc/7.2.0', 'impi/2017.4', 'rosetta/3.13']
        if modules == None:
            modules = rosetta_modules
        else:
            modules += rosetta_modules

    if program == 'pyrosetta':
        pyrosetta_modules = ['ANACONDA/2019.10']
        if modules == None:
            modules = pyrosetta_modules
        else:
            modules += pyrosetta_modules
        conda_env = '/gpfs/projects/bsc72/conda_envs/pyrosetta'

    available_partitions = ['debug', 'bsc_ls']
    if job_name == None:
        raise ValueError('job_name == None. You need to specify a name for the job')
    if output == None:
        output = job_name
    if partition == None:
        raise ValueError('You must select a partion. Available partitions are:'+
                         ', '.join(available_partitions))
    if partition not in available_partitions:
        raise ValueError('Wrong partition selected. Available partitions are:'+
                         ', '.join(available_partitions))
    if script_name == None:
        script_name = 'slurm_job.sh'
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

    if isinstance(time, int):
        time = (time, 0)
    if partition == 'debug' and time == None:
        time= (2,0)
    elif partition == 'debug' and time != None:
        if time[0]*60+time[1] > 120:
            print('Setting time at maximum allowed for the debug partition (2 hours).')
            time = (2,0)
    elif partition == 'bsc_ls' and time == None:
        time = (48,0)
    elif partition == 'bsc_ls' and time != None:
        if time[0]*60+time[1] > 2880:
            print('Setting time at maximum allowed for the bsc_ls partition (48 hours).')
            time=(48,0)

    #Write jobs as array
    with open(script_name,'w') as sf:
        sf.write('#!/bin/bash\n')
        sf.write('#SBATCH --job-name='+job_name+'\n')
        sf.write('#SBATCH --qos='+partition+'\n')
        sf.write('#SBATCH --time='+str(time[0])+':'+str(time[1])+':00\n')
        sf.write('#SBATCH --ntasks '+str(cpus)+'\n')
        if mem_per_cpu != None:
            sf.write('#SBATCH --mem-per-cpu '+str(mem_per_cpu)+'\n')
        if threads != None:
            sf.write('#SBATCH -c '+str(threads)+'\n')
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
        if conda_eval_bash:
            sf.write('eval "$(conda shell.bash hook)"\n')
        if conda_env != None:
            sf.write('source activate '+conda_env+'\n')
            sf.write('\n')

    with open(script_name,'a') as sf:
        sf.write(job)
        if not job.endswith('\n'):
            sf.write('\n\n')
        else:
            sf.write('\n')

    if conda_env != None:
        with open(script_name,'a') as sf:
            sf.write('conda deactivate \n')
            sf.write('\n')
