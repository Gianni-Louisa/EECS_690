from modules.machine import Machine

class GlobalScheduler:
    def __init__( self, num_of_machines, machine_progression_func, machine_checkpointing_func, new_job_func, reschedule_func, curr_timestamp_func, checkpointing_progression = 0):
        # This is a list of machines that exist within the scheduler
        self.machines = [ Machine(i, checkpointing_progression, machine_progression_func, machine_checkpointing_func) for i in range( num_of_machines ) ]
        
        # These are all the tasks that are waiting to be scheduled
        self.task_queue = []

        # These are all the tasks that have been completed
        self.finished_tasks = []

        # This is the current timestep of the scheduler
        self._current_timestamp = 0

        ################################################
        # The following are functions that tell the
        # scheduler how to operate
        ################################################
        # This behavior determines how the global scheduler
        # operates in the current job
        self._current_timestamp_func = curr_timestamp_func
        
        # This function determines how the scheduler handles
        # adding new jobs
        self._new_job_func = new_job_func

        # This function determines how the scheduler
        # reschedules jobs on every timestep
        self._reschedule_func = reschedule_func

    # This function gets the current timestep of the
    # scheduler
    def get_current_timestamp(self):
        return self._current_timestamp

    def run_schedule( self, list_of_jobs ):
        # Reset the member variables of the scheduler
        self.task_queue = []
        self.finished_tasks = []
        self._current_timestamp = 0
        
        # While the scheduler is running, do the following:
        while True:
            if self._current_timestamp in list_of_jobs.keys():
                # If the current timestamp has assoicated jobs,
                # add the job to the queue
                for job in list_of_jobs[ self._current_timestamp ]:
                    self._new_job_func( self, job )

            # Resechedule the jobs after adding all the
            # jobs to the scheduler
            self._reschedule_func( self )

            # Execute the current timestamp of this scheduler
            self._current_timestamp_func( self )

            if all( machine.is_machine_free() for machine in self.machines ) and len( self.task_queue ) == 0 and self._current_timestamp > max( list_of_jobs.keys() ):
                # If all the machines are free and there is nothing
                # left to schedule and there are no more expected jobs
                # end the loop
                return

            # Increment the timestamp
            self._current_timestamp += 1

