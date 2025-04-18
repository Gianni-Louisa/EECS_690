from modules.job import Job
from modules.globalscheduler import GlobalScheduler
from modules.machine import Machine
import random
import math

# Define the rate of errors in the scheduler
MEW = 3
LAMBDA = 1 / MEW

# Define the checkpointing overhead
CHECKPOINTING_OVERHEAD = 0.1

# Define the migration overhead for when jobs are
# migrated to a different processor
MIGRATION_OVERHEAD = 0.3

# Define the error recovery overhead
RECOVERY_OVERHEAD = 0.3

# Define the checkpointing period for when a machine
# should trigger a checkpoint
PERIOD = ( 2 * MEW * CHECKPOINTING_OVERHEAD ) ** (0.5)

# This function defines the liklehood of an error
# within the context of the job and the current
# timestamp
def job_error_func( job : Job, current_timestamp ):
    # The error has a 50/50 chance of occurring
    return random.choice([ True, False ])

# This function compares two jobs against eachother
def job_comparison_func( job1 : Job, job2 : Job ):
    # We will randomly determine if a job has a
    # higher priority or not
    return random.choice([ -1, 0 , 1 ])

# This function determines the behavior of a machine
# that is progressing in the current machine
def machine_progression_func( machine : Machine, current_timestamp, progression_amount ):
    # Remove any floating point errors in the lock time
    if machine._lock_time < 0.000001:
        machine._lock_time = 0

    # Check to see if the machine is free
    if machine.is_machine_free():
        # In this case that the machine is free, indicate the
        # the machine was waiting for the time allocated to progress
        return ( False, progression_amount )
    
    else:
        # If the machine is not free, first get the current
        # job on the machine
        curr_job = machine.get_curr_job()

        # While the current timestamp is later than the next expected time
        # the machine is supposed to checkpoint, do the following
        while( current_timestamp >= machine.get_checkpoint_time() ):
            # Set the current job's checkpoint time to the current
            # check point for the machine 
            curr_job.set_last_checkpoint_time( machine.get_checkpoint_time() )

            # Increment the machine's checkpointing time to the next
            # time to checkpoint
            machine.progress_checkpoint_time()

        # Check to see if we can potentially remove the lock
        # on the machine and progress the job as well
        if( progression_amount > machine._lock_time ):
            # Locally store the original lock time
            orig_lock_time = machine._lock_time

            # Indicate that the current job was waiting for at
            # least lock time units of time
            curr_job.add_waiting_time( machine._lock_time )

            # Attempt to progress on the remaining units of progression
            ret = curr_job.progress( current_timestamp, progression_amount - machine._lock_time )

            # Indicate that the machine is no longer locked
            machine._lock_time = 0

            # Indicate that this progreession was active progression
            # for the lock time + any prgression made on the job
            return ( True, ret + orig_lock_time )

        else:
            # In the case that this machine has locked for more than the progression
            # that we have available, just reduce the lock time. We have this check
            # to account for floating point errors
            if math.isclose( machine._lock_time, progression_amount , rel_tol=1e-9 ):
                progression_amount = machine._lock_time
                machine._lock_time = 0

            else:
                machine._lock_time -= progression_amount

            # Indicate that the job was waiting for the progression
            # amount
            machine.get_curr_job().add_waiting_time( progression_amount )
            
            # Indicate that the machine was active for the amount
            # of progression
            return ( True, progression_amount )

# This is the machines checkpointing function,
# which does nothing
def machine_checkpointing_func( machine : Machine ):
    pass

# This function adds new jobs to the scheduler
def new_job_func( scheduler : GlobalScheduler, job : Job ):
    # Just add the job to the end of the queue and shuffle
    # it for randomness
    scheduler.task_queue.append( job )
    random.shuffle( scheduler.task_queue )

# This function defines the rescheduling behavior for
# this algorithm
def reschedule_func( scheduler : GlobalScheduler ):
    # If there are no jobs in the queue, early exit
    # from the function
    if len( scheduler.task_queue ) == 0:
        return

    # For every machine that is in the scheduler, do the following
    for machine in scheduler.machines:
        # If the machine is free and is not locked, schedule the new job
        if machine.is_machine_free() and machine.get_lock_time() < 0.000001 and len( scheduler.task_queue ) > 0:
            machine._lock_time = 0
            new_job = scheduler.task_queue.pop( 0 )
            new_job.set_first_schedule_time( scheduler._current_timestamp )
            machine.set_curr_job( new_job )

# This function determines the behavior of the scheduler during the current timestamp
def curr_timestamp_func( scheduler : GlobalScheduler ):
    # First initalize the progress map to indicate that every machine
    # can make 1 unit of progress
    total_progress_map = [ [ machine.get_id(), 1 ] for machine in scheduler.machines ]

    # The following code will be executed while there is atleast 1 machine
    # who can make some progress
    while any( [ progress[ 1 ] != 0 for progress in total_progress_map ] ):
        # Get every progress from our progress map
        for progress in total_progress_map:
            # Get the associated machine for this computation
            curr_machine = scheduler.machines[ progress[ 0 ] ]
            
            # If the machine can't make progress or if the machine has
            # no job, indicate that the machine is waiting
            if progress[ 1 ] == 0 or curr_machine.get_curr_job() is None:
                curr_machine.add_waiting_time( progress[ 1 ] )
                progress[ 1 ] = 0
                continue
            
            # Progress the machine for progression time units
            ret = curr_machine.progress( scheduler._current_timestamp, progress[ 1 ] )

            # Get the current job on the machine
            curr_job = curr_machine.get_curr_job()
            

            if curr_job.is_job_complete():
                # If the job is complete, compute all the relevant statistics and send
                # the job to the finished jobs queue
                curr_job.set_completion_time( scheduler._current_timestamp + ret + curr_job.get_waiting_time() )
                curr_job.add_waiting_time( scheduler._current_timestamp + ret - curr_job.get_release_time() - curr_job.get_job_active_running_time())
                scheduler.finished_tasks.append( curr_job )
                curr_machine.set_curr_job( None )

            else:
                if curr_job.is_in_error():
                    # If this job is in error, add lock for recovery
                    curr_machine.add_lock_time( RECOVERY_OVERHEAD )
                    curr_job.restart_job()

                    # Indicate when the job was last checkpointed
                    curr_job.set_last_checkpoint_time( scheduler._current_timestamp + ret )

            # Edit the overall progression for this machine
            progress[ 1 ] -= ret
        
        # Sort the machines so that we prioritize machines that
        # still have more progressions to make
        total_progress_map.sort( key=lambda x : x[ 1 ], reverse=True )