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
