from job import Job
from typing import Callable, Type
import simpy

class Machine:
    def __init__( self, id : int, progression_algo : Callable[ [ Type[ "Machine" ] ], None ], checkpoint_algo : Callable[ [ Type[ "Machine" ] ], None ] ) -> None:        
        self._id : int = id
        self._curr_job : Job = None
        self._stored_checkpoints : dict[ int : int ] = {}
        self._progression_algo = progression_algo
        self._checkpoint_algo = checkpoint_algo
        self._active_time = 0
        self._waiting_time = 0

    def get_id( self ):
        return self._id

    def progress( self, progression_amount ):
        active_time = self._progression_algo( self, progression_amount )
        self._active_time += active_time

        return active_time
    
    def add_waiting_time( self, waiting_time ):
        self._waiting_time += waiting_time

    def is_machine_free( self ): 
        return self._curr_job is None

    def set_curr_job( self, new_job : Job ) -> None:
        self._curr_job = new_job

    def get_curr_job( self ):
        return self._curr_job

    def trigger_checkpoint( self ) -> None:
        self._checkpoint_algo( self )

    def migrate_checkpoint( self, target_machine : Type[ "Machine" ], job : Job ) -> bool:
        if job.get_id() in self._stored_checkpoints.keys():
            target_machine._stored_checkpoints[ job.get_id() ] = self._stored_checkpoints.pop( job.get_id() )
            return True
        
        else:
            return False
        