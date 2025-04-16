from modules.machine import Machine

class GlobalScheduler:
    def __init__( self, num_of_machines, machine_progression_func, machine_checkpointing_func, new_job_func, reschedule_func, curr_timestamp_func, checkpointing_progression = 0):
        
        self.machines = [ Machine(i, checkpointing_progression, machine_progression_func, machine_checkpointing_func) for i in range( num_of_machines ) ]
        
        self.task_queue = []
        self.finished_tasks = []

        self._current_timestamp = 0
        self._current_timestamp_func = curr_timestamp_func
        self._new_job_func = new_job_func
        self._reschedule_func = reschedule_func

    def get_current_timestamp(self):
        return self._current_timestamp

    def run_schedule( self, list_of_jobs ):
        self.task_queue = []
        self.finished_tasks = []

        self._current_timestamp = 0
        
        while True:
            if self._current_timestamp in list_of_jobs.keys():
                for job in list_of_jobs[ self._current_timestamp ]:
                    self._new_job_func( self, job )

            self._reschedule_func( self )
            self._current_timestamp_func( self )

            if all( machine.is_machine_free() for machine in self.machines ) and len( self.task_queue ) == 0 and self._current_timestamp > max( list_of_jobs.keys() ):
                return

            self._current_timestamp += 1

