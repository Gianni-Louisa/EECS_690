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
        self._active_running_time : int = 0
        self._waiting_time : int = 0
        self._in_error = False
        self._last_run_machine = -1

    def get_first_schedule_time( self ):
        return self._first_schedule_time
    
    def get_release_time( self ):
        return self._release_time

    def set_first_schedule_time( self, time ):
        self._first_schedule_time = time

    def get_id( self ):
        return self._job_id
    
    def get_runtime( self ):
        return self._runtime
    
    def restart_job( self ):
        self._runtime = self._orig_runtime
    
    def progress( self, inc = 1 ) -> float:
        self._in_error = self._error_func( self )
        
        if not self._in_error:
            inc = min( self._runtime, inc )
            self._runtime -= inc
            self._active_running_time += inc
            return inc
        else:
            loc = self._error_loc_func( inc )
            self._active_running_time += loc
            return loc
        
    def add_waiting_time( self, waiting_time ):
        self._waiting_time += waiting_time

    def is_job_complete( self ):
        return self._runtime == 0
    
    def get_job_active_running_time( self ):
        return self._active_running_time
    
    def revert_to_checkpoint( self, restart_point : int ):
        self._runtime = restart_point
        self._in_error = False

    def is_in_error( self ):
        return self._in_error

    def set_last_run_machine( self, machine_id : int ):
        self._last_run_machine = machine_id

    def get_last_run_machine( self ):
        return self._last_run_machine
    
    def __eq__( self, other ):
        return self._comparison_func( other ) == 0

    def __lt__( self, other ):
        return self._comparison_func( other ) < 0
    
    def __gt__( self, other ):
        return not ( self < other ) and not ( self == other )
    
    def __ge__( self, other ):
        return self > other or self == other
    
    def __le__( self, other ):
        return self < other or self == 0
    
    def __ne__( self, other ):
        return not ( self == other )
    