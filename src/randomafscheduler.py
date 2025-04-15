from modules.job import Job
from modules.globalscheduler import GlobalScheduler
from modules.machine import Machine
import random

temp_jobs = { 0 : [
    Job(0, 1, 2, 0, lambda x : random.choice( [True, False] ), lambda x, y : random.uniform( 0, y ), lambda x, y : 0),
    Job(1, 2, 2, 0, lambda x : random.choice( [True, False] ), lambda x, y : random.uniform( 0, y ), lambda x, y : 0),
    Job(2, 3, 2, 0, lambda x : random.choice( [True, False] ), lambda x, y : random.uniform( 0, y ), lambda x, y : 0),
    Job(3, 4, 2, 0, lambda x : random.choice( [True, False] ), lambda x, y : random.uniform( 0, y ), lambda x, y : 0),
    Job(4, 5, 2, 0, lambda x : random.choice( [True, False] ), lambda x, y : random.uniform( 0, y ), lambda x, y : 0),
    Job(5, 6, 2, 0, lambda x : random.choice( [True, False] ), lambda x, y : random.uniform( 0, y ), lambda x, y : 0),
    Job(6, 7, 2, 0, lambda x : random.choice( [True, False] ), lambda x, y : random.uniform( 0, y ), lambda x, y : 0),
    Job(7, 8, 2, 0, lambda x : random.choice( [True, False] ), lambda x, y : random.uniform( 0, y ), lambda x, y : 0),
] }

def machine_progression_func( machine : Machine, progression_amount ):
    if not machine.is_machine_free():
        curr_job = machine.get_curr_job()
        return curr_job.progress( progression_amount )
    else:
        return progression_amount

def machine_checkpointing_func( machine : Machine ):
    pass

def new_job_func( scheduler : GlobalScheduler, job : Job ):
    scheduler.task_queue.append( job )
    random.shuffle( scheduler.task_queue )

def reschedule_func( scheduler : GlobalScheduler ):
    for machine in scheduler.machines:
        if machine.is_machine_free() and len( scheduler.task_queue ) > 0:
            curr_job = scheduler.task_queue.pop( 0 )
            curr_job.set_first_schedule_time( scheduler._current_timestamp )
            machine.set_curr_job( curr_job )

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
                scheduler.finished_tasks.append( curr_job )
                curr_machine.set_curr_job( None )

            else:
                if curr_job.is_in_error():
                    curr_job.restart_job()

            progress[ 1 ] -= ret
        
        total_progress_map.sort( key=lambda x : x[ 1 ], reverse=True )


scheduler = GlobalScheduler( 2, machine_progression_func, machine_checkpointing_func, new_job_func, reschedule_func, curr_timestamp_func )
scheduler.run_schedule( temp_jobs )