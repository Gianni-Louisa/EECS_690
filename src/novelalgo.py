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

# Used for statistics surrounding killing/checkpoint
total_preempts = 0
total_kills = 0

# This function defines the liklehood of an error
# within the context of the job and the current
# timestamp
def job_error_func( job : Job, current_timestamp ):
    # Define our threshold for error to be uniformly
    # distributed between 0 and 1
    ret = random.uniform( 0, 1 )

    # The probality of an error on this job can be represented
    # by this CDF
    p = 1 - ( math.e ) ** ( -LAMBDA * ( current_timestamp - job.get_last_checkpoint_time() ) )

    # Determine whether the probablity of error exceeds the
    # threshold of errors 
    return p > ret
    
# This function determines the comparison behavior between two jobs
def job_comparison_func( job1 : Job, job2 : Job ):
    # First check to see if two jobs have the same
    # priority
    if job1._job_priority == job2._job_priority:

        # If the priority is equal on both sides, then
        # check to see if the runtimes of the jobs are
        # equal
        if job1._runtime == job2._runtime:

            # If the runtimes is equal on both sides, then
            # check to see if the release times of the jobs are
            # equal
            if job1._release_time == job2._release_time:
                # If all the metrics are the same, that means
                # both jobs are equal
                return 0
            
            elif job1._release_time > job2._release_time:
                # Indicate that the job on the left hand side of the
                # operator is greater than the right hand side if the
                # release time is greater on the right hand side
                return -1
            
            else:
                # Indicate that the job on the right hand side of the
                # operator is greater than the left hand side if the
                # release time is greater on the left hand side
                return 1
        
        elif job1._runtime > job2._runtime:
            # Indicate that the job on the left hand side of the
            # operator is greater than the right hand side if the
            # runtime is greater on the left hand side
            return 1
        
        else:
            # Indicate that the job on the right hand side of the
            # operator is greater than the left hand side if the
            # runtime is greater on the right hand side
            return -1

    elif job1._job_priority > job2._job_priority:
        # Indicate that the job on the left hand side of the
        # operator is greater than the right hand side if the
        # priority is greater on the left hand side
        return 1
    
    else:
        # Indicate that the job on the right hand side of the
        # operator is greater than the left hand side if the
        # priority is greater on the right hand side
        return -1

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

            # Lock the machine for the overhead's units of time
            machine.add_lock_time( CHECKPOINTING_OVERHEAD )

            # Trigger a checkpoint on the machine
            machine.trigger_checkpoint()

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
        
# This function defines the behavior of the how
# the machine triggers checkpoints
def machine_checkpointing_func( machine : Machine ):
    # Get the current job on the machine
    curr_job = machine.get_curr_job()

    # If a job exists on the machine, store the
    # current runtime of the job
    if curr_job is not None:
        machine._stored_checkpoints[ curr_job.get_id() ] = curr_job.get_runtime()

# This function determines the scheduler's logic in
# determining the priority of the queue
def new_job_func( scheduler : GlobalScheduler, job : Job ):
    # Add the new job to the queue
    scheduler.task_queue.append( job )

    # Sort the scheduler
    scheduler.task_queue.sort( reverse=True )

# This functoin determines how the scheduler reallocates
# jobs on every timestep
def reschedule_func( scheduler : GlobalScheduler ):
    # While there are jobs to schedule, do the following
    while len( scheduler.task_queue ) > 0:
        # Create variables to store the machine to replace
        # the job to replace and whether we found an open
        # machine
        machine_to_replace = None
        job_to_replace = None
        found_open_machine = False

        # For every machine in the scheduler, do the following
        for machine in scheduler.machines:
            # Only do the following lines of code if there is
            # no lock on the machine
            if machine._lock_time == 0:
                # Indicate that we found an open machine
                found_open_machine = True

                # If this condition is true, that means that we
                # can stop the machine because we found a free machine
                if machine.get_curr_job() is None:
                    machine_to_replace = machine.get_id()
                    job_to_replace = None
                    break

                # If this condition is true, that means that machine
                # is a candidate to be preempted
                elif job_to_replace is None or machine.get_curr_job() < job_to_replace:
                    machine_to_replace = machine.get_id()
                    job_to_replace = machine.get_curr_job()
        
        else:
            # If there is no open machine, just exit this loop
            # because we cannot reschedule
            if not found_open_machine:
                break
        
        # Check to see if there is no job to replace
        if job_to_replace is None:
            # If a machine is free, set the highest priority job in the machine
            scheduler.machines[ machine_to_replace ].set_curr_job( scheduler.task_queue.pop( 0 ) )
            scheduler.machines[ machine_to_replace ].get_curr_job().set_first_schedule_time( scheduler._current_timestamp )
            scheduler.machines[ machine_to_replace ].get_curr_job().set_last_run_machine( machine_to_replace )

        # Check to see if the highest priority waiting job
        # is higher than the lowest priority active job
        elif scheduler.task_queue[ 0 ] > job_to_replace:
            # Set the current job back into the task queue
            curr_job = scheduler.machines[ machine_to_replace ].get_curr_job()
            curr_job.set_last_run_machine( machine_to_replace )
            scheduler.task_queue.append( curr_job )

            # Get the new job to put into the machine
            new_job = scheduler.task_queue.pop( 0 )
            scheduler.machines[ machine_to_replace ].set_curr_job( new_job )

            # If the job has never been scheduled, schedule it
            # onto the current timestamp
            if new_job.get_first_schedule_time() == -1:
                new_job.set_first_schedule_time( scheduler._current_timestamp )

            # Compute the cost to kill this job
            kill_cost = new_job.get_orig_runtime() - new_job.get_runtime()

            # Compute the cost to checkpoint
            checkpoint_cost = scheduler._current_timestamp - new_job.get_last_checkpoint_time() + ( MIGRATION_OVERHEAD * ( len( scheduler.machines ) - 1 ) / len( scheduler.machines ) )

            global total_preempts
            total_preempts += 1

            # If the cost to kill is less than the cost to checkpoint restart the job
            if( kill_cost <= checkpoint_cost ):
                global total_kills
                total_kills += 1
                new_job.restart_job()
                new_job.set_last_checkpoint_time( scheduler._current_timestamp )

            else:
                # First determine if we can get the checkpoint information migrated
                success = scheduler.machines[ new_job.get_last_run_machine() ].migrate_checkpoint( scheduler.machines[ machine_to_replace ], new_job )

                # If we can migrate the job and the machines are not the same, add migration overhead
                if success and new_job.get_last_run_machine() != scheduler.machines[ machine_to_replace ].get_id():
                    scheduler.machines[ new_job.get_last_run_machine() ].add_lock_time( MIGRATION_OVERHEAD )
                    new_job.revert_to_checkpoint(scheduler.machines[ machine_to_replace ]._stored_checkpoints[new_job.get_id()])
                    new_job.set_last_checkpoint_time( scheduler._current_timestamp )
            
            # Update the machine that the job last ran on
            new_job.set_last_run_machine( scheduler.machines[ machine_to_replace ].get_id() )

        else:
            break

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
                    # time
                    curr_machine.add_lock_time( RECOVERY_OVERHEAD )
                    
                    if curr_job.get_id() in curr_machine._stored_checkpoints.keys():
                        # If the machine has a checkpoint for the job, restart the
                        # job to the checkpoint
                        curr_job.revert_to_checkpoint( curr_machine._stored_checkpoints[ curr_job.get_id() ] )  
                    else:
                        # Otherwise just restart the job
                        curr_job.restart_job()

                    # Indicate when the job was last checkpointed
                    curr_job.set_last_checkpoint_time( scheduler._current_timestamp + ret )

            # Edit the overall progression for this machine
            progress[ 1 ] -= ret
        
        # Sort the machines so that we prioritize machines that
        # still have more progressions to make
        total_progress_map.sort( key=lambda x : x[ 1 ], reverse=True )

