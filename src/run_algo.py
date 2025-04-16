import random

from modules.job import Job
from modules.globalscheduler import GlobalScheduler
from modules.machine import Machine

import novelalgo

# Define a set of parameters for machine based on algorithm
novelalgo_machine_params = [novelalgo.machine_progression_func,
                            novelalgo.machine_checkpointing_func,
                            novelalgo.new_job_func,
                            novelalgo.reschedule_func,
                            novelalgo.curr_timestamp_func]

# Print a list of jobs
def print_jobs(list_jobs):
    release_times = list(list_jobs.keys())
    release_times.sort()
    print('Initial Jobs')
    for release_time in release_times:
        print(f'Release Time t = {release_time}')

        for job in list_jobs[release_time]:
            print(f'Job ({job.get_id()}) p = {job.get_priority()}, T = {job.get_runtime()}')

        print()

    print()

    
# Run a set of jobs once and returns various statistics
def run_single_set_of_jobs(algorithm, dict_jobs, num_machines):
    # Variable decls
    list_jobs = [item for vals in dict_jobs.values() for item in vals]
    list_jobs.sort(key=(lambda x : x.get_id()))
    
    # Make a scheduler based on algorithm
    scheduler = None
    if (algorithm == 'novelalgo'):
        scheduler = GlobalScheduler(num_machines,
                                    *novelalgo_machine_params)
    else:
        raise ValueError('Algorithm Not Found')

    # Print jobs
    print_jobs(dict_jobs)

    # Run jobs
    scheduler.run_schedule(dict_jobs)

    # Get and compute machine statistics
    total_runtime = scheduler.get_current_timestamp() + 1
    
    # Print total statistics from running
    print(f'Resulting Scheduler Statistics:')
    print(f'Time At Completion of All Tasks: {total_runtime}')
    print('\n')

    # Print machine stats
    print('Resulting Machine Statistics:')
    for machine in scheduler.machines:
        machine_id = machine.get_id()
        active_time = machine.get_active_time()
        waiting_time = machine.get_waiting_time()

        print(f'Machine {machine_id}: active_time = {active_time}, waiting_time = {waiting_time}, active_time / total_time = {active_time / total_runtime}')

    print()

    # Print job stats
    print('Resulting Job Statistics: ')
    for job in list_jobs:
        job_id = job.get_id()
        priority = job.get_priority()
        release_time = job.get_release_time()
        runtime = job.get_job_active_running_time()
        orig_runtime = job.get_orig_runtime()
        waiting_time = job.get_waiting_time()

        print(f'Job ({job_id}) p = {priority}, T = {orig_runtime}, r = {release_time}, w = {waiting_time}, s = {runtime / orig_runtime}')

    
if __name__ == '__main__':
    jobs = { 0 : [
        Job(0, 1, 2, 0, novelalgo.job_error_func, lambda x, y, z : z, novelalgo.job_comparison_func),
        Job(1, 2, 2, 0, novelalgo.job_error_func, lambda x, y, z : z, novelalgo.job_comparison_func),
        Job(2, 3, 2, 0, novelalgo.job_error_func, lambda x, y, z : z, novelalgo.job_comparison_func),
        Job(3, 4, 2, 0, novelalgo.job_error_func, lambda x, y, z : z, novelalgo.job_comparison_func),
        Job(4, 5, 2, 0, novelalgo.job_error_func, lambda x, y, z : z, novelalgo.job_comparison_func),
        Job(5, 6, 2, 0, novelalgo.job_error_func, lambda x, y, z : z, novelalgo.job_comparison_func),
        Job(6, 7, 2, 0, novelalgo.job_error_func, lambda x, y, z : z, novelalgo.job_comparison_func),
        Job(7, 8, 2, 0, novelalgo.job_error_func, lambda x, y, z : z, novelalgo.job_comparison_func),
    ] }

    run_single_set_of_jobs('novelalgo', jobs, 3)
