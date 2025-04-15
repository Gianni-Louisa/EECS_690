
def generate_random_jobs(num_jobs, max_priority, max_runtime, max_release_time): 
    random_jobs = {} # dictionary of jobs at each timestamp
    
    jobs_at_time_0 = [] # jobs at time 0
    for i in range(num_jobs // 2): # half of the jobs are released at time 0
        job_id = i # job id
        priority = random.randint(1, max_priority) # priority 
        runtime = random.randint(1, max_runtime) # runtime
        release_time = 0 # release time
        
        job = Job(job_id, priority, runtime, release_time, # job object
                 job_error_func, # error function
                 lambda x, y: y if y < 0.000001 else random.uniform(0, y), # error location function
                 job_comparison_func) # job comparison function
        jobs_at_time_0.append(job) # append job to jobs at time 0
    
    jobs_at_random_times = {} # dictionary of jobs at random times
    for i in range(num_jobs // 2, num_jobs): # remaining jobs
        job_id = i # job id
        priority = random.randint(1, max_priority) # priority
        runtime = random.randint(1, max_runtime) # runtime
        release_time = random.randint(1, max_release_time) # release time
        
        job = Job(job_id, priority, runtime, release_time, # job object
                 job_error_func, # error function
                 lambda x, y: y if y < 0.000001 else random.uniform(0, y), # error location function
                 job_comparison_func) # job comparison function
        
        if release_time not in jobs_at_random_times: 
            jobs_at_random_times[release_time] = [] # if the release time is not in the dictionary, create a new list
        jobs_at_random_times[release_time].append(job) # append the job to the list
    
    random_jobs[0] = jobs_at_time_0 # add the jobs at time 0 to the dictionary
    for release_time, jobs in jobs_at_random_times.items():
        random_jobs[release_time] = jobs # add the jobs at random times to the dictionary
    
    return random_jobs

jobs = generate_random_jobs(num_jobs=50, max_priority=5, max_runtime=5, max_release_time=1)

