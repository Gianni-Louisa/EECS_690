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

    def progress( self, progression_amount ):
        return self._progression_algo( self, progression_amount )

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
        