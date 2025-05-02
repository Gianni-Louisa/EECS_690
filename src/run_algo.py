import random
from copy import deepcopy
import statistics

import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
#matplotlib.use('TkAgg')

from modules.job import Job
from modules.globalscheduler import GlobalScheduler
from modules.machine import Machine

import novelalgo
import LISTorg
import randomafscheduler
from generate_random_jobs import generate_random_jobs, LISTLambdaParams, NovelLambdaParams, RandomAlgoParams

# Hightest Priority
HIGHEST_PRIORITY = 10

# Define a set of parameters for machine based on algorithm
novelalgo_machine_params = [novelalgo.machine_progression_func,
                            novelalgo.machine_checkpointing_func,
                            novelalgo.new_job_func,
                            novelalgo.reschedule_func,
                            novelalgo.curr_timestamp_func,
                            novelalgo.PERIOD]

list_machine_params = [LISTorg.machine_progression_func,
                      LISTorg.machine_checkpointing_func,
                      LISTorg.new_job_func,
                      LISTorg.reschedule_func,
                      LISTorg.curr_timestamp_func,
                      LISTorg.PERIOD]

random_machine_params = [randomafscheduler.machine_progression_func,
                      randomafscheduler.machine_checkpointing_func,
                      randomafscheduler.new_job_func,
                      randomafscheduler.reschedule_func,
                      randomafscheduler.curr_timestamp_func,
                      randomafscheduler.PERIOD]


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
    plots = []

    # Make a bar graph for each stat in the stat list
    for i, stat_label in enumerate(stat_lable_list):
        stat = [stat[i] for stat in stat_list]

        fig = plt.figure()
        plt.bar(algorithm_list, stat)
        plt.title('Comparison of ' + stat_label)
        plt.xlabel('Algorithm')
        plt.ylabel(stat_label)

        plots.append(fig)

    return plots

    
# Make overlapping normal distributions for statistics
def graph_distributions(algorithm_list, complete_stat_lists, n_trials):
    stat_label_list = ['Average Total Runtime over Area', 'Average Weighted Stretch', 'Average Wait Time']
    plots = []

    # Make a histogram for each stat
    for i, stat_label in enumerate(stat_label_list):
        fig = plt.figure()
        stat_list = []
        
        for j, algo in enumerate(algorithm_list):
            stat_list.append([stat_list[i] for stat_list in complete_stat_lists[j]])

        if stat_label == 'Average Total Runtime over Area':
            plt.hist(stat_list, 10, label=algorithm_list)
        elif stat_label == 'Average Weighted Stretch':
            plt.hist(stat_list, 15, label=algorithm_list)
        else:
            plt.hist(stat_list, 10, label=algorithm_list)
            
        plt.title('Comparison of ' + str(n_trials) + ' of ' + stat_label)
        plt.xlabel(stat_label)
        plt.ylabel('Number of Occurrences')
        plt.legend()

        plots.append(fig)

    return plots
                         

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
        
    elif (algorithm == 'listalgo'):
        scheduler = GlobalScheduler(num_machines,
                                    *list_machine_params)
    elif (algorithm == 'randomalgo' ):
        scheduler = GlobalScheduler(num_machines,
                                    *random_machine_params)
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
def run_set_of_jobs(algorithm_list, job_list_list, func_arg_list, num_machines, suppress_graphing=True):
    n = len(job_list_list)
    stat_dict = { }
    complete_stat_list = []
    stat_every_run = []

    # Run suite of jobs n times for each algorithm
    for i, algorithm in enumerate(algorithm_list):
        stats = (0, 0, 0)
        total_stats = []
        
        for job_list in job_list_list:
            job_list = { release_time : [job_lambda(*func_arg_list[i]) for job_lambda in job_lambda_list] for release_time, job_lambda_list in job_list.items() }
            sing_stats = run_single_set_of_jobs(algorithm, job_list, num_machines, True)

            stats = tuple(x + y for x, y in zip(sing_stats, stats))
            total_stats.append(sing_stats)

        stats = [x / n for x in stats]
        complete_stat_list.append(stats)
        stat_every_run.append(total_stats)

        variance_list = list(map(statistics.variance, [total_stats[i] for i in range(len(stats))]))

        # Print Results
        print(f'{algorithm}:')
        print(f'Average Time / Area: {stats[0]}')
        print(f'\tVariance: {variance_list[0]}')
        print(f'Average Weighted Stretch: {stats[1]}')
        print(f'\tVariance: {variance_list[1]}')
        print(f'Average Average Wait Time: {stats[2]}')
        print(f'\tVariance: {variance_list[2]}')
        print()

    # Make graph
    if not suppress_graphing:
        plots = []
        algorithm_title_dictionary = { 'novelalgo' : 'Novel Algorithm',
                                       'listalgo' : 'LIST Algorithm',
                                       'randomalgo' : 'Random Algorithm' }
        algorithm_titles = [algorithm_title_dictionary[algo] for algo in algorithm_list]

        # Plot Everything
        plots += graph_averages(algorithm_titles, complete_stat_list)
        plots += graph_distributions(algorithm_titles, stat_every_run, n)

        # Plot only LIST and Novel
        plots += graph_averages(algorithm_titles[:-1], complete_stat_list[:-1])
        plots += graph_distributions(algorithm_titles[:-1], stat_every_run[:-1], n)

        # Show graph or save to pdf
        show = True # Change if you want a pdf
        if show:
            plt.show()
        else:
            pp = PdfPages('graphs.pdf')

            for plot in plots:
                pp.savefig(plot)

            pp.close()

    
if __name__ == '__main__':

    '''
    Delete later before pushing to production
    # ------------------------------ TEST JOBS ------------------------------
    jobs = lambda error_func, job_compare_func : { 0 : [
        Job(0, 1, 10, 0, error_func, lambda x, y, z : z, job_compare_func), 
    ],
    1: [
        Job(1, 2, 10, 1, error_func, lambda x, y, z : z if z < 0.000001 else random.uniform( 0, z ), job_compare_func),
        Job(2, 3, 10, 1, error_func, lambda x, y, z : z if z < 0.000001 else random.uniform( 0, z ), job_compare_func),
        Job(3, 4, 10, 1, error_func, lambda x, y, z : z if z < 0.000001 else random.uniform( 0, z ), job_compare_func),
        Job(4, 5, 10, 1, error_func, lambda x, y, z : z if z < 0.000001 else random.uniform( 0, z ), job_compare_func),
        Job(5, 6, 10, 1, error_func, lambda x, y, z : z if z < 0.000001 else random.uniform( 0, z ), job_compare_func),
        Job(6, 7, 10, 1, error_func, lambda x, y, z : z if z < 0.000001 else random.uniform( 0, z ), job_compare_func),
        Job(7, 8, 10, 1, error_func, lambda x, y, z : z if z < 0.000001 else random.uniform( 0, z ), job_compare_func),
    ] }

    job2 = lambda error_func, job_compare_func : { 0 : [
        #Job(5, 6, 2, 0, error_func, lambda x, y, z : z if z < 0.000001 else random.uniform( 0, z ), job_compare_func),
        Job(6, 7, 2, 0, error_func, lambda x, y, z : z if z < 0.000001 else random.uniform( 0, z ), job_compare_func),
        Job(7, 8, 2, 0, error_func, lambda x, y, z : z if z < 0.000001 else random.uniform( 0, z ), job_compare_func),
    ] }
    '''


    #run_single_set_of_jobs('listalgo', jobs(*LISTLambdaParams), 3)
    #run_set_of_jobs(['novelalgo', 'listalgo'], [ jobs for _ in range( 1000 ) ], [NovelLambdaParams, LISTLambdaParams], 3)

    jobs = [generate_random_jobs(100, HIGHEST_PRIORITY, 10, 50) for _ in range(100)]
    run_set_of_jobs(['novelalgo', 'listalgo', 'randomalgo'], jobs, [NovelLambdaParams, LISTLambdaParams, RandomAlgoParams], 4, False)
