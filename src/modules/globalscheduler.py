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
    def __init__( self, num_of_machines, new_job_func, reschedule_func, curr_timestamp_func ):
        
        self.machines = [ Machine(i, progress, null) for i in range( num_of_machines ) ]
        
        self.task_queue = []
        self.finished_tasks = []

        self._current_timestamp = 0
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
    stored_rets = [ [ i, 1 ] for i in range( len( scheduler.machines ) ) ]

    while any( [ i[ 1 ] != 0 for i in stored_rets ] ):
        for progress_map in stored_rets:
            if progress_map[ 1 ] == 0:
                continue

            progress_map[ 1 ] -= scheduler.machines[ progress_map[ 0 ] ].progress( progress_map[ 1 ] )


        stored_rets.sort( key=lambda x : x[ 1 ] )

        for progress_map in stored_rets:
            if scheduler.machines[ progress_map[ 0 ] ].get_curr_job() is None:
                continue
            
            if scheduler.machines[ progress_map[ 0 ] ].get_curr_job().is_job_complete():
                scheduler.finished_tasks.append( scheduler.machines[ progress_map[ 0 ] ].get_curr_job() )
                scheduler.machines[ progress_map[ 0 ] ].set_curr_job( None )

                if len( scheduler.task_queue ) > 0:
                    scheduler.machines[ progress_map[ 0 ] ].set_curr_job( scheduler.task_queue.pop( 0 ) )
                    

temp_jobs = { 0 : [
    Job(0, 1, 2, 0, lambda x : random.choice( [True, False] ), lambda x : random.uniform( 0, x ), lambda x, y : True),
    Job(0, 1, 2, 0, lambda x : random.choice( [True, False] ), lambda x : random.uniform( 0, x ), lambda x, y : True),
    Job(0, 1, 2, 0, lambda x : random.choice( [True, False] ), lambda x : random.uniform( 0, x ), lambda x, y : True),
    Job(0, 1, 2, 0, lambda x : random.choice( [True, False] ), lambda x : random.uniform( 0, x ), lambda x, y : True),
] }

scheduler = GlobalScheduler( 2, new_job_func, reschedule_func, current_timestamp_func )
scheduler.run_schedule( temp_jobs )