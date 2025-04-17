from modules.job import Job
from modules.globalscheduler import GlobalScheduler
from modules.machine import Machine
import random
import math

MEW = 3
LAMBDA = 1 / MEW
CHECKPOINTING_OVERHEAD = 0.1
MIGRATION_OVERHEAD = 0.3
RECOVERY_OVERHEAD = 0.3

PERIOD = ( 2 * MEW * CHECKPOINTING_OVERHEAD ) ** (0.5)

def job_error_func( job : Job, current_timestamp ):
    ret = random.uniform( 0, 1 )
    p = 1 - ( math.e ) ** ( -LAMBDA * ( current_timestamp - job.get_last_checkpoint_time() ) )

    if p > ret:
        return True

    else:
        return False
    
def job_comparison_func( job1 : Job, job2 : Job ):
    return 0
    
def machine_progression_func( machine : Machine, current_timestamp, progression_amount ):
    if machine.is_machine_free():
        return ( False, progression_amount )
    
    else:
        curr_job = machine.get_curr_job()

        if( current_timestamp >= machine.get_checkpoint_time() ):
            curr_job.set_last_checkpoint_time( machine.get_checkpoint_time() )                
            machine.progress_checkpoint_time()

        if( progression_amount > machine._lock_time ):
            orig_lock_time = machine._lock_time
            curr_job.add_waiting_time( machine._lock_time )
            ret = curr_job.progress( current_timestamp, progression_amount - machine._lock_time )
            machine._lock_time = 0
            return ( True, ret + orig_lock_time )

        else:
            if math.isclose( machine._lock_time, progression_amount , rel_tol=1e-9 ):
                progression_amount = machine._lock_time
                machine._lock_time = 0

            else:
                machine._lock_time -= progression_amount

            machine.get_curr_job().add_waiting_time( progression_amount )
            return ( True, progression_amount )
        

def machine_checkpointing_func( machine : Machine ):
    pass

def new_job_func( scheduler : GlobalScheduler, job : Job ):
    scheduler.task_queue.append( job )

def reschedule_func( scheduler : GlobalScheduler ):
    if len( scheduler.task_queue ) == 0:
        return

    for machine in scheduler.machines:
        if machine.is_machine_free() and machine.get_lock_time() < 0.000001 and len( scheduler.task_queue ) > 0:
            machine._lock_time = 0
            new_job = scheduler.task_queue.pop( 0 )
            new_job.set_first_schedule_time( scheduler._current_timestamp )
            machine.set_curr_job( new_job )

def curr_timestamp_func( scheduler : GlobalScheduler ):
    total_progress_map = [ [ machine.get_id(), 1 ] for machine in scheduler.machines ]

    while any( [ progress[ 1 ] != 0 for progress in total_progress_map ] ):
        for progress in total_progress_map:
            curr_machine = scheduler.machines[ progress[ 0 ] ]
            
            if progress[ 1 ] == 0 or curr_machine.get_curr_job() is None:
                curr_machine.add_waiting_time( progress[ 1 ] )
                progress[ 1 ] = 0
                continue

            ret = curr_machine.progress( scheduler._current_timestamp, progress[ 1 ] )
            curr_job = curr_machine.get_curr_job()
            
            if curr_job.is_job_complete():
                curr_job.add_waiting_time( scheduler._current_timestamp + ret - curr_job.get_release_time() - curr_job.get_job_active_running_time() )
                curr_job.set_completion_time( scheduler._current_timestamp + ret )
                scheduler.finished_tasks.append( curr_job )
                curr_machine.set_curr_job( None )

            else:
                if curr_job.is_in_error():
                    curr_machine.add_lock_time( RECOVERY_OVERHEAD )
                    curr_job.restart_job()

            progress[ 1 ] -= ret
        
        total_progress_map.sort( key=lambda x : x[ 1 ], reverse=True )


scheduler = GlobalScheduler( 2, machine_progression_func, machine_checkpointing_func, new_job_func, reschedule_func, curr_timestamp_func )

# Currently I have it setup to randomly generate jobs, half at time 0 and the rest at random times
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
                 lambda x, y, z : z if z < 0.000001 else random.uniform( 0, z ), # error location function
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
                 lambda x, y, z : z if z < 0.000001 else random.uniform( 0, z ), # error location function
                 job_comparison_func) # job comparison function
        
        if release_time not in jobs_at_random_times: 
            jobs_at_random_times[release_time] = [] # if the release time is not in the dictionary, create a new list
        jobs_at_random_times[release_time].append(job) # append the job to the list
    
    random_jobs[0] = jobs_at_time_0 # add the jobs at time 0 to the dictionary
    for release_time, jobs in jobs_at_random_times.items():
        random_jobs[release_time] = jobs # add the jobs at random times to the dictionary
    
    return random_jobs

#jobs = generate_random_jobs(num_jobs=10000, max_priority=5, max_runtime=5, max_release_time=1)

#scheduler.run_schedule( jobs )
