from typing import Callable, Type

class Job:
    def __init__( self, job_id : int, job_priority : int, runtime : int, 
                    release_time : int, error_func : Callable[ [ Type[ "Job" ] ], bool ],
                    error_loc_func : Callable[ [ Type[ "Job" ] ], bool ], 
                    comparison_func : Callable[ [ Type[ "Job" ], Type[ "Job" ] ], int ] ):
        self._job_id : int = job_id
        self._job_priority : int = job_priority
        self._runtime : int = runtime
        self._release_time : int = release_time
        self._completion_time : int = -1
        self._in_error = False
        self._error_func = error_func
        self._last_run_machine = -1
        self._comparison_func = comparison_func
        self._error_loc_func = error_loc_func

    def get_id( self ):
        return self._job_id
    
    def get_runtime( self ):
        return self._runtime
    
    def progress( self, inc = 1 ) -> float:
        self._in_error = self._error_func( self )
        
        if not self._in_error:
            inc = min( self._runtime, inc )
            self._runtime -= inc
            self._completion_time += inc
            return inc
        else:
            loc = self._error_loc_func( inc )
            self._completion_time += loc
            return loc

    def is_job_complete( self ):
        return self._runtime == 0
    
    def set_job_completion_time( self, completion_time ):
        self._completion_time = completion_time
    
    def get_job_completion_time( self ):
        return self._completion_time
    
    def revert_to_checkpoint( self, restart_point : int ):
        self._runtime = restart_point
        self._in_error = False

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
    