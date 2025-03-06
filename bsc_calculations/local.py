import os
import math

def parallel(jobs, cpus=None, script_name='commands'):
    """
    Generates scripts to run jobs simultaneously in N Cpus in a local computer,
    i.e., without a job manager. The input jobs must be a list representing each
    job to execute as a string and the list order will be prioritized upon execution.

    Two different scripts are written to execute the jobs in bash language. For
    example, if the script_name variable is set to commands and the cpus to 4, five
    scripts will be written:

    - commands
    - commands_0
    - commands_1
    - commands_2
    - commands_3
    ...

    The jobs to execute are distributed into the numbered scripts. Each numbered
    script contains a sub set of jobs that will be executed in a sequential manner.
    The numberless script execute all the numbered scripts in the background, using
    the nohup command, and redirecting the output to different files for each numbered
    script. To execute the jobs is only necessary to execute:

    'bash commands'

    Parameters
    ----------
    jobs : list
        List of strings containing the commands to execute jobs.
    cpus : int
        Number of CPUs to use in the execution.
    script_name : str
        Name of the output scripts to execute the jobs.
    """
    # Write parallel execution scheme #

    if jobs == []:
        raise ValueError('The jobs list is empty!')

    # Check input
    if isinstance(jobs, str):
        jobs = [jobs]

    if cpus == None:
        cpus = min([len(jobs), 10])
        print(f'Number of CPU not given, using {cpus} by default.')

    if len(jobs) < cpus:
        print('The number of jobs is less than the number of CPU.')
        cpus = len(jobs)
        print('Using %s CPU' % cpus)

    # Open script files
    zf = len(str(cpus))
    scripts = {}
    for c in range(cpus):
        scripts[c] = open(script_name+'_'+str(c).zfill(zf),'w')
        scripts[c].write('#!/bin/sh\n')

    # Write jobs with list-order prioritization
    for i in range(len(jobs)):
        scripts[i%cpus].write(jobs[i])


    # Close script files
    for c in range(cpus):
        scripts[c].close()

    # Write script to execute them all in background
    with open(script_name,'w') as sf:
        sf.write('#!/bin/sh\n')
        sf.write('for script in '+script_name+'_'+'?'*zf+'; do nohup bash $script &> ${script%.*}.nohup& done\n')

def multipleGPUSimulations(
    jobs,
    parallel=3,
    gpus=4,
    script_name='gpu_commands'
):
    """
    Generates scripts to run jobs simultaneously on multiple GPUs (and in parallel on each GPU)
    without using a formal job manager (e.g., in a local environment). The function writes
    multiple bash scripts:

    1) A main script (e.g., 'gpu_commands') that calls each of the sub-scripts in the background.
    2) Sub-scripts (e.g., 'gpu_commands_00', 'gpu_commands_01', etc.) which each contain a
       subset of jobs.

    Usage:
    ------
    1) Prepare a list of commands (strings). Each command should contain the substring 'GPUID'
       to be replaced by the GPU index.
    2) Call this function to produce the scripts.
    3) Run 'bash <script_name>' (e.g., 'bash gpu_commands') to launch everything.

    Parameters
    ----------
    jobs : list of str
        Each element is a shell command to run. Must contain 'GPUID' if a GPU index is needed.
    gpus : int
        Number of distinct GPUs available.
    parallel : int
        Number of parallel processes per GPU.
    script_name : str
        Base name for the script files written.

    Returns
    -------
    None
    """
    if not jobs:
        print("No jobs provided. Exiting without creating scripts.")
        return

    # Basic validation of the parameters
    if gpus <= 0 or parallel <= 0:
        raise ValueError("Both 'gpus' and 'parallel' must be positive integers.")

    # Optionally, you could warn if 'GPUID' isn't in any job
    for idx, job_cmd in enumerate(jobs):
        if 'GPUID' not in job_cmd:
            print(f"Warning: 'GPUID' not found in job #{idx}:\n    {job_cmd}")

    # We have gpus * parallel "slots" total
    total_slots = gpus * parallel
    total_jobs = len(jobs)

    # Number of jobs that fit evenly across the slots
    jobs_per_slot = total_jobs // total_slots
    # Remainder that doesn't split evenly
    leftover = total_jobs % total_slots

    # Determine zero-fill based on maximum slot index
    # e.g. if total_slots = 12, zf might be 2 -> 00, 01, 02, ..., 11
    zf = len(str(total_slots - 1))  # or max(len(str(total_jobs)), len(str(total_slots)))

    # Create a structure to hold the jobs for each slot
    # This clarifies how we distribute jobs across slots/gpus
    slot_jobs = [[] for _ in range(total_slots)]

    # Fill each slot with the base number of jobs
    job_index = 0
    for slot_idx in range(total_slots):
        for _ in range(jobs_per_slot):
            slot_jobs[slot_idx].append(jobs[job_index])
            job_index += 1

    # Distribute leftover jobs, one per slot, until we run out
    slot_idx = 0
    while job_index < total_jobs:
        slot_jobs[slot_idx].append(jobs[job_index])
        job_index += 1
        slot_idx += 1
        if slot_idx >= total_slots:
            slot_idx = 0

    # Now write the sub-scripts
    for slot_idx, sub_jobs in enumerate(slot_jobs):
        if not sub_jobs:
            # If this slot got no jobs, skip writing the file entirely
            continue

        gpu_id = slot_idx % gpus  # cycles through 0..gpus-1
        sub_script_name = f"{script_name}_{str(slot_idx).zfill(zf)}"

        with open(sub_script_name, 'w') as sf:
            sf.write('#!/bin/sh\n\n')
            for job_cmd in sub_jobs:
                # Replace 'GPUID' with the GPU index
                sf.write(job_cmd.replace('GPUID', str(gpu_id)) + '\n')

    # Finally, write the main script to launch them all
    with open(script_name, 'w') as sf:
        sf.write('#!/bin/sh\n\n')
        sf.write('# This script runs all sub-scripts in the background via nohup.\n')
        sf.write('# Outputs are redirected to <sub_script_name>.nohup.\n\n')

        # Only launch the scripts that actually have jobs
        for slot_idx, sub_jobs in enumerate(slot_jobs):
            if not sub_jobs:
                continue
            sub_script_name = f"{script_name}_{str(slot_idx).zfill(zf)}"
            sf.write(
                f"nohup bash {sub_script_name} "
                f">& {sub_script_name}.nohup &\n"
            )

        # Optionally: add a wait or leave it to run in the background
        sf.write('\n# Uncomment the line below if you want the script to wait until all finish:\n')
        sf.write('# wait\n')

    print(f"Generated {script_name} and the corresponding sub-scripts. "
          f"To run the jobs, execute:\n    bash {script_name}")
