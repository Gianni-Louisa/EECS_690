from modules.job import Job
from modules.globalscheduler import GlobalScheduler
from modules.machine import Machine
import random
import math

MEW = 3
LAMBDA = 1 / MEW
CHECKPOINTING_OVERHEAD = 0.1
MIGRATION_OVERHEAD = 0.3
RECOVERY_OVERHEAD = 0.1

time_to_checkpoint = ( 2 * MEW * CHECKPOINTING_OVERHEAD ) ** (0.5)

def job_error_func( job : Job ):
    ret = random.uniform( 0, 1 )
    p = 1 - ( math.e ) ** ( -LAMBDA * job.get_last_checkpoint_time() )

    if p < ret:
        return random.choice( [ True, False ] )
        return True

    else:
        return False
    
def job_comparison_func( job1 : Job, job2 : Job ):
    if job1._job_priority == job2._job_priority:
        if job1._runtime == job2._runtime:
            if job1._release_time == job2._release_time:
                return 0
            
            elif job1._release_time > job2._release_time:
                return -1
            
            else:
                return 1
        
        elif job1._runtime > job2._runtime:
            return 1
        
        else:
            return -1

    elif job1._job_priority > job2._job_priority:
        return 1
    
    else:
        return -1

temp_jobs = { 0 : [
    Job(0, 1, 1, 0, job_error_func, lambda x, y : y if y < 0.000001 else random.uniform( 0, y ), job_comparison_func),
    Job(1, 2, 2, 0, job_error_func, lambda x, y : y if y < 0.000001 else random.uniform( 0, y ), job_comparison_func),
    Job(2, 3, 3, 0, job_error_func, lambda x, y : y if y < 0.000001 else random.uniform( 0, y ), job_comparison_func),
    Job(3, 4, 2, 0, job_error_func, lambda x, y : y if y < 0.000001 else random.uniform( 0, y ), job_comparison_func),
    Job(4, 5, 2, 0, job_error_func, lambda x, y : y if y < 0.000001 else random.uniform( 0, y ), job_comparison_func),
    Job(5, 6, 2, 0, job_error_func, lambda x, y : y if y < 0.000001 else random.uniform( 0, y ), job_comparison_func),
    Job(6, 7, 2, 0, job_error_func, lambda x, y : y if y < 0.000001 else random.uniform( 0, y ), job_comparison_func),
],
1 : [
    Job(7, 8, 2, 5, job_error_func, lambda x, y : y if y < 0.000001 else random.uniform( 0, y ), job_comparison_func),
] }

def machine_progression_func( machine : Machine, current_timestamp, progression_amount ):
    if machine.is_machine_free():
        return ( False, progression_amount )
    
    else:
        curr_job = machine.get_curr_job()

        if( curr_job.get_last_checkpoint_time() >= time_to_checkpoint ):
            curr_job.set_last_checkpoint_time( current_timestamp )
            machine.add_lock_time( CHECKPOINTING_OVERHEAD )
            machine.trigger_checkpoint()

        if( progression_amount > machine._lock_time ):
            ret = curr_job.progress( progression_amount - machine._lock_time )
            machine._lock_time = 0
            return ( True, ret )

        else:
            if math.isclose( machine._lock_time, progression_amount , rel_tol=1e-9 ):
                progression_amount = machine._lock_time
                machine._lock_time = 0

            else:
                machine._lock_time -= progression_amount

            return ( True, progression_amount )
        

def machine_checkpointing_func( machine : Machine ):
    curr_job = machine.get_curr_job()

    if curr_job is not None:
        machine._stored_checkpoints[ curr_job.get_id() ] = curr_job.get_runtime()

def new_job_func( scheduler : GlobalScheduler, job : Job ):
    scheduler.task_queue.append( job )
    scheduler.task_queue.sort( reverse=True)

def reschedule_func( scheduler : GlobalScheduler ):
    while True and len( scheduler.task_queue ) > 0:
        machine_to_replace = None
        job_to_replace = None
        found_open_machine = False

        for machine in scheduler.machines:
            if machine._lock_time == 0:
                found_open_machine = True

                if machine.get_curr_job() is None:
                    machine_to_replace = machine.get_id()
                    job_to_replace = None
                    break

                elif job_to_replace is None or machine.get_curr_job() < job_to_replace:
                    machine_to_replace = machine.get_id()
                    job_to_replace = machine.get_curr_job()
        
        else:
            if not found_open_machine:
                break

        if job_to_replace is None:
            scheduler.machines[ machine_to_replace ].set_curr_job( scheduler.task_queue.pop( 0 ) )
            scheduler.machines[ machine_to_replace ].get_curr_job().set_first_schedule_time( scheduler._current_timestamp )
            scheduler.machines[ machine_to_replace ].get_curr_job().set_last_run_machine( machine_to_replace )

        elif scheduler.task_queue[ 0 ] > job_to_replace:
            curr_job = scheduler.machines[ machine_to_replace ].get_curr_job()
            curr_job.set_last_run_machine( machine_to_replace )
            scheduler.task_queue.append( curr_job )

            new_job = scheduler.task_queue.pop( 0 )

            if new_job.get_first_schedule_time == -1:
                new_job.set_first_schedule_time( scheduler._current_timestamp )

            kill_cost = curr_job.get_orig_runtime() - curr_job.get_runtime()
            checkpoint_cost = kill_cost - curr_job.get_last_checkpoint_time() + ( MIGRATION_OVERHEAD * ( len( scheduler.machines ) - 1 ) / len( scheduler.machines ) )

            if( kill_cost <= checkpoint_cost ):
                scheduler.machines[ machine_to_replace ].get_curr_job().restart_job()

            else:
                success = scheduler.machines[ new_job.get_last_run_machine() ].migrate_checkpoint( scheduler.machines[ machine_to_replace ], new_job )

                if success:
                    scheduler.machines[ new_job.get_last_run_machine() ].add_lock_time( MIGRATION_OVERHEAD )

                scheduler.machines[ machine_to_replace ].set_curr_job( scheduler.task_queue.pop( 0 ) )

        else:
            break

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
                    if curr_job.get_id() in curr_machine._stored_checkpoints.keys():
                        curr_job.revert_to_checkpoint( curr_machine._stored_checkpoints[ curr_job.get_id() ] )
                        curr_machine.add_lock_time( RECOVERY_OVERHEAD )

                    else:
                        curr_job.restart_job()

            progress[ 1 ] -= ret
        
        total_progress_map.sort( key=lambda x : x[ 1 ], reverse=True )


scheduler = GlobalScheduler( 2, machine_progression_func, machine_checkpointing_func, new_job_func, reschedule_func, curr_timestamp_func )
scheduler.run_schedule( temp_jobs )