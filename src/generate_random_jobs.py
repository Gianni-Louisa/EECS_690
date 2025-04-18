import random

import novelalgo
import LPTorg
import randomafscheduler
from modules.job import Job

error_placement_function = lambda x, y, z : z if z < 0.000001 else random.uniform( 0, z )

# Takes in job info quad and returns proper lambda closure
def job_info_to_lambda(quad):
    return lambda error_func, job_compare_func : Job(*quad, error_func, error_placement_function, job_compare_func)


# Define parameters for each algorithm
LPTLambdaParams = [LPTorg.job_error_func, LPTorg.job_comparison_func]
NovelLambdaParams = [novelalgo.job_error_func, novelalgo.job_comparison_func]
RandomAlgoParams = [randomafscheduler.job_error_func, randomafscheduler.job_comparison_func]

# Currently I have it setup to randomly generate jobs, half at time 0 and the rest at random times
def generate_random_jobs(num_jobs, max_priority, max_runtime, max_release_time):
    random_jobs = {} # dictionary of jobs at each timestamp
    
    jobs_at_time_0 = [] # jobs at time 0
    for i in range(num_jobs // 2): # half of the jobs are released at time 0
        job_id = i # job id
        priority = random.randint(1, max_priority) # priority 
        runtime = random.randint(1, max_runtime) # runtime
        release_time = 0 # release time

        '''
        job = Job(job_id, priority, runtime, release_time, # job object
                 job_error_func, # error function
                 lambda x, y, z : z if z < 0.000001 else random.uniform( 0, z ), # error location function
                 job_comparison_func) # job comparison function
        '''
        job = job_id, priority, runtime, release_time
        jobs_at_time_0.append(job_info_to_lambda(job)) # append job to jobs at time 0
    
    jobs_at_random_times = {} # dictionary of jobs at random times
    for i in range(num_jobs // 2, num_jobs): # remaining jobs
        job_id = i # job id
        priority = random.randint(1, max_priority) # priority
        runtime = random.randint(1, max_runtime) # runtime
        release_time = random.randint(1, max_release_time) # release time

        '''
        job = Job(job_id, priority, runtime, release_time, # job object
                 job_error_func, # error function
                 lambda x, y, z : z if z < 0.000001 else random.uniform( 0, z ), # error location function
                 job_comparison_func) # job comparison function
        '''
        job = job_id, priority, runtime, release_time
        
        if release_time not in jobs_at_random_times: 
            jobs_at_random_times[release_time] = [] # if the release time is not in the dictionary, create a new list
        jobs_at_random_times[release_time].append(job_info_to_lambda(job)) # append the job to the list

    '''
    random_jobs[0] = jobs_at_time_0 # add the jobs at time 0 to the dictionary
    for release_time, jobs in jobs_at_random_times.items():
        random_jobs[release_time] = jobs # add the jobs at random times to the dictionary
    '''
    
    #return random_jobs
    
    return { 0 : jobs_at_time_0 } | jobs_at_random_times
