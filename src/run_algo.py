import random
from copy import deepcopy
import matplotlib.pyplot as plt

from modules.job import Job
from modules.globalscheduler import GlobalScheduler
from modules.machine import Machine

import novelalgo

# Hightest Priority
HIGHEST_PRIORITY = 10

# Define a set of parameters for machine based on algorithm
novelalgo_machine_params = [novelalgo.machine_progression_func,
                            novelalgo.machine_checkpointing_func,
                            novelalgo.new_job_func,
                            novelalgo.reschedule_func,
                            novelalgo.curr_timestamp_func,
                            novelalgo.PERIOD]

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

    
# Make bar graph of different average statistics to compare algorithms
def graph_averages(algorithm_list, stat_list):
    stat_lable_list = ['Average Total Runtime over Area', 'Average Weighted Stretch', 'Average Wait Time']

    # Make a bar graph for each stat in the stat list
    for i, stat_label in enumerate(stat_lable_list):
        stat = [stat[i] for stat in stat_list]

        plt.figure()
        plt.bar(algorithm_list, stat)
        plt.title('Comparison of ' + stat_label)
        plt.xlabel('Algorithm')
        plt.ylabel(stat_label)

    #plt.show()

    
# Make overlapping normal distributions for statistics
def graph_distributions(algorithm_list, complete_stat_lists, n_trials):
    stat_label_list = ['Average Total Runtime over Area', 'Average Weighted Stretch', 'Average Wait Time']

    # Make a histogram for each stat
    for i, stat_label in enumerate(stat_label_list):
        plt.figure()
        
        for j, algo in enumerate(algorithm_list):
            stat_list = [stat_list[i] for stat_list in complete_stat_lists[j]]

            if stat_label == 'Average Total Runtime over Area':
                plt.hist(stat_list, 4, label=algo)
            else:
                plt.hist(stat_list, 10, label=algo)

        plt.title('Comparison of ' + str(n_trials) + ' of ' + stat_label)
        plt.xlabel(stat_label)
        plt.ylabel('Number of Occurrences')
                         

# Run a set of jobs once and returns various statistics
def run_single_set_of_jobs(algorithm, dict_jobs, num_machines, suppress_printing=False):
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
    if not suppress_printing:
        print_jobs(dict_jobs)

    # Run jobs
    scheduler.run_schedule(dict_jobs)

    # Reset time to checkpoint for next 
    #novelalgo.time_to_checkpoint = novelalgo.PERIOD


    # Get and compute machine statistics
    total_schedule_runtime = scheduler.get_current_timestamp() + 1
    
    # Print total statistics from running
    if not suppress_printing:
        print(f'Resulting Scheduler Statistics:')
        print(f'Time At Completion of All Tasks: {total_schedule_runtime}')
        print('\n')

    # Print machine stats
    if not suppress_printing:
        print('Resulting Machine Statistics:')
        
    for machine in scheduler.machines:
        machine_id = machine.get_id()
        active_time = machine.get_active_time()
        waiting_time = machine.get_waiting_time()

        if not suppress_printing:
            print(f'Machine {machine_id}: active_time = {active_time}, waiting_time = {waiting_time}, active_time / total_time = {active_time / total_schedule_runtime}')

    # Print job stats
    if not suppress_printing:
        print('\nResulting Job Statistics: ')

    sum_job_length = 0
    sum_wait_time = 0
    sum_weighted_stretch = 0
    sum_ideal_job_length = 0
    
    for job in list_jobs:
        job_id = job.get_id()
        priority = job.get_priority()
        release_time = job.get_release_time()
        runtime = job.get_job_active_running_time()
        orig_runtime = job.get_orig_runtime()
        waiting_time = job.get_waiting_time()
        total_runtime = job.get_completion_time() - release_time

        sum_job_length += total_runtime
        sum_ideal_job_length += orig_runtime
        sum_wait_time += waiting_time
        sum_weighted_stretch += priority * total_runtime / orig_runtime

        if not suppress_printing:
            print(f'Job ({job_id}) p = {priority}, T = {orig_runtime}, r = {release_time}, w = {waiting_time}, rT = { runtime }, r_T = {total_runtime}, s = {runtime / orig_runtime}')

    # Final Statistics
    num_jobs = len(list_jobs)
    avg_wait_time = sum_wait_time / num_jobs
    avg_weighted_stretch = sum_weighted_stretch / num_jobs
    area = sum_ideal_job_length / len(scheduler.machines)
    total_runtime_over_area = total_schedule_runtime / area

    if not suppress_printing:
        print(f'\nTotal Time: {total_schedule_runtime}, Total Time / Area: {total_runtime_over_area}')
        print(f'Average Weighted Stretch: {avg_weighted_stretch}, Average Wait Time: {avg_wait_time}')
        print()

    return total_runtime_over_area, avg_weighted_stretch, avg_wait_time


# Run job scheduler each time on a list of job dicts and print and graph statistics
def run_set_of_jobs(algorithm_list, job_lists, num_machines, suppress_graphing=True):
    n = len(job_lists)
    stat_dict = { }
    complete_stat_list = []
    stat_every_run = []

    # Run suite of jobs n times for each algorithm
    for algorithm in algorithm_list:
        stats = (0, 0, 0)
        total_stats = []
        
        for job_list in job_lists:
            sing_stats = run_single_set_of_jobs(algorithm, job_list, num_machines, True)

            stats = tuple(x + y for x, y in zip(sing_stats, stats))
            total_stats.append(sing_stats)

        stats = [x / n for x in stats]
        complete_stat_list.append(stats)
        stat_every_run.append(total_stats)

        # Print Results
        print(f'{algorithm}:')
        print(f'Average Time / Area: {stats[0]}')
        print(f'Average Weighted Stretch: {stats[1]}')
        print(f'Average Average Wait Time: {stats[2]}')
        print()

    # Make graph
    if not suppress_graphing:
        graph_averages(['Novel Algorithm'], complete_stat_list)
        graph_distributions(['Novel Algorithm'], stat_every_run, n)

        plt.show()

    
if __name__ == '__main__':
    jobs = [{ 0 : [
        Job(0, 1, 2, 0, novelalgo.job_error_func, lambda x, y, z : z, novelalgo.job_comparison_func), 
    ],
    1: [
        Job(1, 2, 2, 1, novelalgo.job_error_func, lambda x, y, z : z if z < 0.000001 else random.uniform( 0, z ), novelalgo.job_comparison_func),
        Job(2, 3, 2, 1, novelalgo.job_error_func, lambda x, y, z : z if z < 0.000001 else random.uniform( 0, z ), novelalgo.job_comparison_func),
        Job(3, 4, 2, 1, novelalgo.job_error_func, lambda x, y, z : z if z < 0.000001 else random.uniform( 0, z ), novelalgo.job_comparison_func),
        Job(4, 5, 2, 1, novelalgo.job_error_func, lambda x, y, z : z if z < 0.000001 else random.uniform( 0, z ), novelalgo.job_comparison_func),
        Job(5, 6, 2, 1, novelalgo.job_error_func, lambda x, y, z : z if z < 0.000001 else random.uniform( 0, z ), novelalgo.job_comparison_func),
        Job(6, 7, 2, 1, novelalgo.job_error_func, lambda x, y, z : z if z < 0.000001 else random.uniform( 0, z ), novelalgo.job_comparison_func),
        Job(7, 8, 2, 1, novelalgo.job_error_func, lambda x, y, z : z if z < 0.000001 else random.uniform( 0, z ), novelalgo.job_comparison_func),
    ] }]

    job2 = [{ 0 : [
        #Job(5, 6, 2, 0, novelalgo.job_error_func, lambda x, y, z : z if z < 0.000001 else random.uniform( 0, z ), novelalgo.job_comparison_func),
        Job(6, 7, 2, 0, novelalgo.job_error_func, lambda x, y, z : z if z < 0.000001 else random.uniform( 0, z ), novelalgo.job_comparison_func),
        Job(7, 8, 2, 0, novelalgo.job_error_func, lambda x, y, z : z if z < 0.000001 else random.uniform( 0, z ), novelalgo.job_comparison_func),
    ] }]

    #run_single_set_of_jobs('novelalgo', jobs[0], 3)
    run_set_of_jobs(['novelalgo'], [ deepcopy( jobs[ 0 ] ) for i in range( 100 )  ], 3, False)
