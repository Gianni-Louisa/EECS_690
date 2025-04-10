import simpy
import random
import statistics
"""
Pi = priority (0-10 highest priority 10)
T = time    
Rt = release time
Ji = job ID

Cc = Cost of checkpointing
Cm = Cost of migration
Cr = Cost of recovery

This study is trying to find:
 - weighted Slowdown and stretch of jobs
 - weighted Utilization

Fill processors if they are empty. If not, fill with highest priority job. You will find the lowest priority job that is running on a processor to replace. That job will be preempted and moved to the new processor.
There is a global scheduler that will schedule jobs to machines and when a job is replaced it will be moved to the globoal scheduler until it is scheduled again.
Essentially, the global scheduler is the job scheduler and is a priority queue and will make all higher priority jobs move to the front, the local machines do not have queues and only hold enough jobs to fill the machine. So if a job is preempted, which will be terminated or checkpointed, it will be moved to the global scheduler until it is scheduled again.
Each machine will just store enough to storage checkpoint and recovery information. 
We will either terminate or use the latest checkpoint to recover the job. Taking the loss of the job into account.
Make sure to include the machine that the job was running on for the previous checkpoint and if its kicked back to the global scheduler, make sure to try to schedule it on the same machine if possible.

"""
NUM_MACHINES = 8
CORES_PER_MACHINE = 18
TOTAL_CPUS = NUM_MACHINES * CORES_PER_MACHINE
SIM_TIME = 10000
TASK_ARRIVAL_RATE = 0.5  
MEAN_TASK_DURATION = 20
CHECKPOINT_OVERHEAD = 2
MIGRATION_COST = 3  
SEED = 42

random.seed(SEED)

class Task:
    def __init__(self, env, id, duration, priority, release_time=None):
        self.env = env
        self.id = id
        self.total_duration = duration
        self.remaining = duration
        self.start_time = None
        self.end_time = None
        self.wait_time = 0
        self.preemptions = 0
        self.checkpoints = 0
        self.priority = priority
        self.machine_id = None
        self.last_machine_id = None
        self.release_time = release_time if release_time is not None else env.now
        self.checkpoint_times = []  
        self.last_checkpoint_remaining = duration 

    def run(self, machine, stats):
        with machine.cpu.request(priority=self.priority) as req:
            try:
                start_wait = self.env.now
                yield req
                
                self.wait_time += self.env.now - start_wait
                if self.start_time is None:
                    self.start_time = self.env.now
                
                self.machine_id = machine.id
                self.last_machine_id = machine.id
                machine.running_tasks.append(self)
                
                while self.remaining > 0:
                    start_exec = self.env.now
                    yield self.env.timeout(1)  # Process in small increments to allow preemption
                    self.remaining -= 1
                
                # Task completed successfully
                self.end_time = self.env.now
                machine.running_tasks.remove(self)
                stats['completed'].append(self)
                
            except simpy.Interrupt:
                # Task was preempted
                self.preemptions += 1
                
                # Create checkpoint
                self.checkpoints += 1
                self.last_checkpoint_remaining = self.remaining
                self.checkpoint_times.append(self.env.now)
                
                # Apply checkpoint overhead
                yield self.env.timeout(CHECKPOINT_OVERHEAD)
                
                # Remove from current machine if it was actually running
                if self in machine.running_tasks:
                    machine.running_tasks.remove(self)
                
                # Return to global scheduler
                stats['global_scheduler'].add_task(self)

class Machine:
    def __init__(self, env, id, cores):
        self.env = env
        self.id = id
        self.cores = cores
        self.cpu = simpy.PreemptiveResource(env, capacity=cores)
        self.running_tasks = []
    
    def get_lowest_priority_task(self):
        if not self.running_tasks:
            return None
        return min(self.running_tasks, key=lambda t: t.priority)
    
    def is_full(self):
        return len(self.running_tasks) >= self.cores
    
    def preempt_lowest_priority(self, new_task):
        lowest_task = self.get_lowest_priority_task()
        if lowest_task and lowest_task.priority < new_task.priority:
            for req in self.cpu.users:
                if req.key == -lowest_task.priority: 
                    self.cpu.preempt(req)
                    return True
        return False

class GlobalScheduler:
    def __init__(self, env, machines, stats):
        self.env = env
        self.machines = machines
        self.task_queue = []
        self.stats = stats
        self.env.process(self.scheduler_process())
    
    def add_task(self, task):
        self.task_queue.append(task)
        self.task_queue.sort(key=lambda t: (-t.priority, t.release_time))
    
    def scheduler_process(self):
        while True:
            if self.task_queue:
                self.schedule_tasks()
            yield self.env.timeout(1) 
    
    def schedule_tasks(self):
        tasks_to_remove = []
        
        for task in self.task_queue:
            scheduled = False
            
            if task.last_machine_id is not None:
                preferred_machine = self.machines[task.last_machine_id]
                if not preferred_machine.is_full():
                    self.env.process(task.run(preferred_machine, self.stats))
                    scheduled = True
                    tasks_to_remove.append(task)
                elif self.try_preemption(preferred_machine, task):
                    scheduled = True
                    tasks_to_remove.append(task)
            
            if not scheduled:
                for machine in self.machines:
                    if not machine.is_full():
                        self.env.process(task.run(machine, self.stats))
                        scheduled = True
                        tasks_to_remove.append(task)
                        break
                
                if not scheduled:
                    for machine in self.machines:
                        if self.try_preemption(machine, task):
                            scheduled = True
                            tasks_to_remove.append(task)
                            break
        
        for task in tasks_to_remove:
            self.task_queue.remove(task)
    
    def try_preemption(self, machine, new_task):
        lowest_task = machine.get_lowest_priority_task()
        if lowest_task and lowest_task.priority < new_task.priority:
            machine.preempt_lowest_priority(new_task)
            self.env.process(new_task.run(machine, self.stats))
            return True
        return False

def task_generator(env, stats, scheduler):
    task_id = 0
    while True:
        yield env.timeout(random.expovariate(1.0 / TASK_ARRIVAL_RATE))
        duration = random.expovariate(1.0 / MEAN_TASK_DURATION)
        priority = random.randint(1, 10)  
        task = Task(env, task_id, duration, priority)
        stats['generated'] += 1
        scheduler.add_task(task)
        task_id += 1

def run_simulation():
    env = simpy.Environment()
    
    # Initialize statistics
    stats = {
        'completed': [],
        'generated': 0,
        'global_scheduler': None 
    }
    
    machines = [Machine(env, i, CORES_PER_MACHINE) for i in range(NUM_MACHINES)]
    
    scheduler = GlobalScheduler(env, machines, stats)
    stats['global_scheduler'] = scheduler
    
    env.process(task_generator(env, stats, scheduler))
    
    env.run(until=SIM_TIME)
    
    if stats['completed']:
        response_times = [t.end_time - t.release_time for t in stats['completed']]
        wait_times = [t.wait_time for t in stats['completed']]
        slowdowns = [rt / t.total_duration for rt, t in zip(response_times, stats['completed'])]
        weighted_slowdowns = [sd * t.priority for sd, t in zip(slowdowns, stats['completed'])]
        
        stretches = slowdowns 
        weighted_stretches = [s * t.priority for s, t in zip(stretches, stats['completed'])]
        
        total_processing_time = sum(t.total_duration - t.remaining for t in stats['completed'])
        utilization = (total_processing_time / (TOTAL_CPUS * SIM_TIME)) * 100
        
        weighted_processing_time = sum((t.total_duration - t.remaining) * t.priority for t in stats['completed'])
        weighted_utilization = (weighted_processing_time / (TOTAL_CPUS * SIM_TIME * 10)) * 100  

        total_preemptions = sum(t.preemptions for t in stats['completed'])
        total_checkpoints = sum(t.checkpoints for t in stats['completed'])
        
        print(f"Total Tasks Generated: {stats['generated']}")
        print(f"Total Tasks Completed: {len(stats['completed'])}")
        print(f"Average Response Time: {statistics.mean(response_times):.2f}s")
        print(f"Average Wait Time: {statistics.mean(wait_times):.2f}s")
        print(f"Average Slowdown: {statistics.mean(slowdowns):.2f}")
        print(f"Average Weighted Slowdown: {statistics.mean(weighted_slowdowns):.2f}")
        print(f"Average Stretch: {statistics.mean(stretches):.2f}")
        print(f"Average Weighted Stretch: {statistics.mean(weighted_stretches):.2f}")
        print(f"CPU Utilization: {utilization:.2f}%")
        print(f"Weighted CPU Utilization: {weighted_utilization:.2f}%")
        print(f"Total Preemptions: {total_preemptions}")
        print(f"Total Checkpoints: {total_checkpoints}")
        print(f"Tasks in Queue at End: {len(stats['global_scheduler'].task_queue)}")
    else:
        print("No tasks were completed during the simulation.")

if __name__ == '__main__':
    run_simulation()