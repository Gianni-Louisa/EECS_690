from typing import Callable, Type

class Job:
    def __init__( self, job_id : int, job_priority : int, runtime : int, 
                    release_time : int, error_func : Callable[ [ Type[ "Job" ] ], bool ],
                    error_loc_func : Callable[ [ Type[ "Job" ] ], bool ], 
                    comparison_func : Callable[ [ Type[ "Job" ], Type[ "Job" ] ], int ] ):
        # The following are meta in formation about jobs 
        self._job_id : int = job_id
        self._job_priority : int = job_priority

        # The following are functions that define how jobs operate
        self._comparison_func = comparison_func
        self._error_func = error_func
        self._error_loc_func = error_loc_func
        
        # The following are information about the job
        self._runtime : int = runtime
        self._orig_runtime : int = runtime
        self._release_time : int = release_time
        self._first_schedule_time = -1
        self._last_checkpoint_time : int = -1
        self._active_running_time : int = 0
        self._waiting_time : int = 0
        self._completion_time : float = 0
        self._in_error = False
        self._last_run_machine = -1

    def get_first_schedule_time( self ):
        return self._first_schedule_time

    def get_priority(self):
        return self._job_priority
    
    def get_release_time( self ):
        return self._release_time

    def set_first_schedule_time( self, time ):
        self._first_schedule_time = time
        self._last_checkpoint_time = time

    def get_id( self ):
        return self._job_id
    
    def get_runtime( self ):
        return self._runtime
    
    def restart_job( self ):
        self._runtime = self._orig_runtime
        self._in_error = False

    def get_orig_runtime( self ):
        return self._orig_runtime

    def get_waiting_time(self):
        return self._waiting_time
    
    def set_completion_time( self, completion_time ):
        self._completion_time = completion_time
        
    def get_completion_time( self ):
        return self._completion_time
    
    def progress( self, current_timestamp, inc = 1 ) -> float:
        self._in_error = self._error_func( self, current_timestamp )
        
        if not self._in_error:
            inc = min( self._runtime, inc )
            self._runtime -= inc
            self._active_running_time += inc
            return inc
        else:
            loc = self._error_loc_func( self, current_timestamp, inc )
            self._active_running_time += loc
            return loc
        
    def add_waiting_time( self, waiting_time ):
        if waiting_time < 0:
            x = 1
        self._waiting_time += waiting_time
        #print('HERE')
        #print(waiting_time)

    def is_job_complete( self ):
        return self._runtime == 0
    
    def get_job_active_running_time( self ):
        return self._active_running_time
    
    def revert_to_checkpoint( self, restart_point : int ):
        if restart_point < 0:
            restart_point = 0 
        self._runtime = restart_point
        self._in_error = False

    def is_in_error( self ):
        return self._in_error

    def set_last_run_machine( self, machine_id : int ):
        self._last_run_machine = machine_id

    def get_last_run_machine( self ):
        return self._last_run_machine
    
    def set_last_checkpoint_time( self, time ):
        self._last_checkpoint_time = time

    def get_last_checkpoint_time( self ):
        return self._last_checkpoint_time
    
    def __eq__( self, other ):
        return self._comparison_func( self, other ) == 0

    def __lt__( self, other ):
        return self._comparison_func( self, other ) < 0
    
    def __gt__( self, other ):
        return not ( self < other ) and not ( self == other )
    
    def __ge__( self, other ):
        return self > other or self == other
    
    def __le__( self, other ):
        return self < other or self == 0
    
    def __ne__( self, other ):
        return not ( self == other )
    
