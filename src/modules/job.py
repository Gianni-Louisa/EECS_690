from typing import Callable, Type

class Job:
    def __init__( self, job_id : int, job_priority : int, runtime : float, 
                    release_time : float, error_func,
                    error_loc_func, 
                    comparison_func ):
        
        ################################################
        # The following are meta in formation about jobs 
        ################################################
        # This the unique identifier for a job
        self._job_id : int = job_id

        # This is the priority of the job
        self._job_priority : int = job_priority

        ################################################
        # The following are functions that define how 
        # jobs operate
        ################################################
        # This function defines how jobs should be compared
        # against eachother
        self._comparison_func = comparison_func

        # This function defines how likely an error is to
        # occur within a job
        self._error_func = error_func

        # This function determines where an error is likely
        # to occur
        self._error_loc_func = error_loc_func
        
        ################################################
        # The following is general information about the
        # jobs current state and behavior
        ################################################
        # This is the remaining time a job needs to complete
        self._runtime : float = runtime

        # This is the original runtime of the job
        self._orig_runtime : float = runtime

        # This is when the job was released
        self._release_time : float = release_time

        # This is time that it was first scheduled
        self._first_schedule_time : float = -1

        # This is the last time a checkpoint was triggered
        # on this job
        self._last_checkpoint_time : float = -1

        # This is the location of the last machine that this
        # job was running on
        self._last_run_machine : int = -1
        
        # This indicates whether this job is in error or not
        self._in_error : bool = False
        
        ################################################
        # These are statistics about the job
        ################################################
        # This is the time that the job was actively running
        self._active_running_time : float = 0

        # This is the job's completion time
        self._completion_time : float = 0

        # This is the time that the job spend waiting
        self._waiting_time : float = 0

    # This function returns the first scheduled time 
    def get_first_schedule_time( self : type[ "Job" ] ):
        return self._first_schedule_time
    
    # This function returns the priority of the job
    def get_priority(self):
        return self._job_priority

    # This function sets the first scheduled time
    def set_first_schedule_time( self : type[ "Job" ], time : float ):
        self._first_schedule_time = time
        self._last_checkpoint_time = time
    
    # This function will get the release time
    def get_release_time( self : type[ "Job" ] ):
        return self._release_time

    # This function will get the jobs id
    def get_id( self : type[ "Job" ] ):
        return self._job_id
    
    # This function will get the jobs current remaining runtime
    def get_runtime( self : type[ "Job" ] ):
        return self._runtime
    
    # This function will restart the job
    def restart_job( self : type[ "Job" ] ):
        self._runtime = self._orig_runtime
        self._in_error = False

    # This function will get the job's original runtime
    def get_orig_runtime( self : type[ "Job" ] ):
        return self._orig_runtime
    
    # This function returns the jobs waiting time
    def get_waiting_time(self):
        return self._waiting_time
    
    # This function sets the jobs completion time
    def set_completion_time( self, completion_time ):
        self._completion_time = completion_time
        
    # This function returns the jobs completion time
    def get_completion_time( self ):
        return self._completion_time

    # This function will define the progression behavior
    # of the job
    def progress( self : type[ "Job" ], current_timestamp : float, inc : float = 1 ) -> float:
        # First determine if the job is in error
        self._in_error = self._error_func( self, current_timestamp )
        
        if not self._in_error:
            # If the job is not in error, determine
            # maximum continuous unit of progressions we
            # can take
            inc = min( self._runtime, inc )

            # Reduce the runtime of the job
            self._runtime -= inc

            # Increment the active runtime 
            self._active_running_time += inc
            return inc
        else:
            # Determine where the location of the error could
            # be if there is an error
            loc = self._error_loc_func( self, current_timestamp, inc )
            
            # Increment the active runtime but don't increment the
            # job state because it is an invalid progression
            self._active_running_time += loc
            return loc

    # This function adds waiting time to the job    
    def add_waiting_time( self, waiting_time ):
        if waiting_time < 0:
            x = 1
        self._waiting_time += waiting_time
        #print('HERE')
        #print(waiting_time)

    # This function determines if the job is complete
    def is_job_complete( self ):
        return self._runtime == 0
    
    # This function determines a jobs active running time
    def get_job_active_running_time( self ):
        return self._active_running_time
    
    # This function reverts a job to a previous state
    def revert_to_checkpoint( self, restart_point : int ):
        # Removing the floating point error if it exists
        if restart_point < 0:
            restart_point = 0 
        
        # Reset the runtime
        self._runtime = restart_point

        # Remove the error regardeless of whether it exists
        # or not
        self._in_error = False

    # This function determines if a job is in error
    def is_in_error( self ):
        return self._in_error

    # This function updates the last running machine
    def set_last_run_machine( self, machine_id : int ):
        self._last_run_machine = machine_id

    # This function returns the machine that the job last
    # ran on 
    def get_last_run_machine( self ):
        return self._last_run_machine
    
    # This function sets the last time this job was
    # checkpointed
    def set_last_checkpoint_time( self, time ):
        self._last_checkpoint_time = time

    # This function gets the last checkpointing time
    def get_last_checkpoint_time( self ):
        return self._last_checkpoint_time
    
    # Overload the equality behavior
    def __eq__( self, other ):
        return self._comparison_func( self, other ) == 0

    # Overload the less than behavior
    def __lt__( self, other ):
        return self._comparison_func( self, other ) < 0
    
    # Overload the greater than behavior
    def __gt__( self, other ):
        return not ( self < other ) and not ( self == other )
    
    # Overload the greater than or equal to behavior
    def __ge__( self, other ):
        return self > other or self == other
    
    # Overload the less than or equal to behavior
    def __le__( self, other ):
        return self < other or self == 0
    
    # Overload the not equal to behavior
    def __ne__( self, other ):
        return not ( self == other )
    
