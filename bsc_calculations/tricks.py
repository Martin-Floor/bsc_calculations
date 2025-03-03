def batchJobsForSingleJobs(jobs, batch_size):
    """
    Splits a list of job strings into batches of a fixed size and concatenates
    each batch into a single string.

    Parameters:
    - jobs (list of str): A list of job strings to be batched.
    - batch_size (int): The number of jobs per batch.

    Returns:
    - dict: A dictionary where keys are batch numbers (starting from 1),
            and values are concatenated job strings for each batch.
    """

    # Separate jobs into single jobs scripts of a fixed batch size
    batch = 1
    jobs_batchs = {}
    for job in jobs:
        jobs_batchs.setdefault(batch, [])
        if len(jobs_batchs[batch]) == batch_size:
            batch += 1
        jobs_batchs.setdefault(batch, [])
        jobs_batchs[batch].append(job)

    for batch in jobs_batchs:
        jobs_batchs[batch] = ''.join(jobs_batchs[batch])

    return jobs_batchs
