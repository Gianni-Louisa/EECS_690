from modules.job import Job
from typing import Callable, Type

class Machine:
    def __init__( self, id : int, checkpointing_progression, progression_algo : Callable[ [ Type[ "Machine" ] ], None ], checkpoint_algo : Callable[ [ Type[ "Machine" ] ], None ] ) -> None:        
        self._id : int = id
        self._curr_job : Job = None
        self._stored_checkpoints : dict[ int : int ] = {}

        self._checkpoint_time = checkpointing_progression
        self._checkpoint_progression = checkpointing_progression

        self._progression_algo = progression_algo
        self._checkpoint_algo = checkpoint_algo
        
        self._active_time = 0
        self._waiting_time = 0
        self._lock_time = 0

    def add_lock_time( self, lock_time ):
        self._lock_time += lock_time

    def get_lock_time( self ):
        return self._lock_time

    def get_id( self ):
        return self._id

    def get_active_time(self):
        return self._active_time

    def get_waiting_time(self):
        return self._waiting_time

    def progress( self, current_timestamp, progression_amount ):
        is_active, progression_time = self._progression_algo( self, current_timestamp, progression_amount )

        if is_active:
            self._active_time += progression_time

        else:
            self._waiting_time += progression_time

        return progression_time
    
    def add_waiting_time( self, waiting_time ):
        self._waiting_time += waiting_time

    def is_machine_free( self ): 
        return self._curr_job is None and self._lock_time == 0

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
        
    def get_checkpoint_time( self ):
        return self._checkpoint_time
    
    def progress_checkpoint_time( self ):
        self._checkpoint_time += self._checkpoint_progression
