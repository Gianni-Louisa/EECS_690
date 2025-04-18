from modules.job import Job
from typing import Callable, Type

class Machine:
    def __init__( self, id : int, checkpointing_progression : float, progression_algo , checkpoint_algo ) -> None:        
        ################################################
        # The following are meta in formation about machines 
        ################################################
        # This is the ID of the machine
        self._id : int = id

        # This is the instance of the job that the machine is
        # currently running
        self._curr_job : Job = None
        
        ################################################
        # The following are checkpointing information
        # for the machine 
        ################################################
        # This is a local machine storage for all the
        # checkpoints that a machine has
        self._stored_checkpoints : dict[ int : float ] = {}
        
        # This is the time that the job should checkpoint
        # next
        self._checkpoint_time = checkpointing_progression
        
        # This is how much the checkpoint time should increase
        # by
        self._checkpoint_progression = checkpointing_progression
        
        ################################################
        # The following are functions to define how a
        # machine should behave 
        ################################################
        # This defines how a machine should handle
        # progressions
        self._progression_algo = progression_algo
        
        # This function determins how a machine should
        # handle checkpoints
        self._checkpoint_algo = checkpoint_algo
        
        # This determines how long a machine is locked for.
        # Locks are when a machine needs to handle recovery
        # and overhead
        self._lock_time = 0
        
        ################################################
        # The following are statistics about the machine
        ################################################
        # This stores how long the machine was active
        self._active_time = 0

        # This stores how long the machine was waiting
        self._waiting_time = 0

    # This function add locks time to the machine
    def add_lock_time( self, lock_time ):
        self._lock_time += lock_time

    # This functions gets the locktime for a machine
    def get_lock_time( self ):
        return self._lock_time

    # This function gets the id of the machine
    def get_id( self ):
        return self._id

    # This function gets the active time of the machine
    def get_active_time(self):
        return self._active_time

    # This function gets the waiting time of the machine
    def get_waiting_time(self):
        return self._waiting_time

    # This function defines the progression behavior of the
    # machine. It pprogression by the given amount and updates
    # the relevant statistics
    def progress( self, current_timestamp, progression_amount ):
        # Call the specific progression function defined for this
        # machine
        is_active, progression_time = self._progression_algo( self, current_timestamp, progression_amount )

        if is_active:
            # If the progression amount is determined to be
            # active, update the machine's activate time
            self._active_time += progression_time

        else:
            # Otherwise, update the machine's waiting
            # time
            self._waiting_time += progression_time

        return progression_time
    
    # This function adds waiting time to the machine
    def add_waiting_time( self, waiting_time ):
        self._waiting_time += waiting_time

    # This function determines if a machine is free. This
    # requires that there is no job currently scheduled
    # on the machine and the machine's lock_time is 0
    def is_machine_free( self ): 
        return self._curr_job is None and self._lock_time < 0.000001

    # This function sets the current job on the machine
    def set_curr_job( self, new_job : Job ) -> None:
        self._curr_job = new_job

    # This function gets the current job on the machine
    def get_curr_job( self ):
        return self._curr_job

    # This function triggers a checkpoint on the machine
    def trigger_checkpoint( self ) -> None:
        # Call the specific function that is associated
        # with this machine
        self._checkpoint_algo( self )

    # This function tries to migrate a checkpoint from one
    # machine to another
    def migrate_checkpoint( self, target_machine, job ) -> bool:
        if job.get_id() in self._stored_checkpoints.keys():
            # If the job exists in this machine's checkpoint storage
            # send it to the target machine and indicate that
            # the checkpointing was successful
            target_machine._stored_checkpoints[ job.get_id() ] = self._stored_checkpoints.pop( job.get_id() )
            return True
        
        else:
            # Indicate that the migration failed
            return False
    
    # This function returns the current checkpointing time
    def get_checkpoint_time( self ):
        return self._checkpoint_time
    
    # This function progresses the current checkpointing
    # time
    def progress_checkpoint_time( self ):
        self._checkpoint_time += self._checkpoint_progression
