from machine import Machine
from job import Job
import random
import simpy

def null( x ):
    pass

def progress( machine : Machine, progression_amount ):
    if not machine.is_machine_free():
        progress_count = progression_amount

        while progress_count > 0:
            progression = machine.get_curr_job().progress( progress_count )

            if machine.get_curr_job().is_job_complete():
                return progression + 1 - progress_count
            
            else:
                progress_count -= progression

    return progression_amount
        

class GlobalScheduler:
    def __init__( self, env, num_of_machines, new_job_func, reschedule_func, curr_timestamp_func ):
        self.env = env
        
        self.machines = [ Machine(env, i, progress, null) for i in range( num_of_machines ) ]
        
        self.task_queue = []
        self.finished_tasks = []

        self._current_timestamp = env.now
        self._current_timestamp_func = curr_timestamp_func
        self._new_job_func = new_job_func
        self._reschedule_func = reschedule_func


    def run_schedule( self, list_of_jobs ):
        while True:
            if self._current_timestamp in list_of_jobs.keys():
                for job in list_of_jobs[ self._current_timestamp ]:
                    self._new_job_func( self, job )

            self._reschedule_func( self )
            self._current_timestamp_func( self )

            if all( machine.is_machine_free() for machine in self.machines ) and len( self.task_queue ) == 0 and self._current_timestamp > max( list_of_jobs.keys() ):
                return

            self._current_timestamp += 1


def new_job_func( scheduler : GlobalScheduler, job ):
    scheduler.task_queue.append( job )

def reschedule_func( scheduler : GlobalScheduler ):
    for machine in scheduler.machines:
        if machine.is_machine_free() and len( scheduler.task_queue ) > 0:
            machine.set_curr_job( scheduler.task_queue.pop() )

def current_timestamp_func( scheduler : GlobalScheduler ):
    pass

env = simpy.Environment()