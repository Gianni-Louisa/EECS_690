import simpy
import random
import statistics

NUM_MACHINES = 2
CORES_PER_MACHINE = 4
TOTAL_CPUS = NUM_MACHINES * CORES_PER_MACHINE
SIM_TIME = 1000
TASK_ARRIVAL_RATE = 0.5  
MEAN_TASK_DURATION = 20
CHECKPOINT_OVERHEAD = 2
MIGRATION_COST = 3  
SEED = 42

random.seed(SEED)

class Task:
    def __init__(self, env, id, duration, priority):
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
        self.processor = None

    def run(self, cpu, stats, scheduler):
        with cpu.request(priority=self.priority) as req:
            self.start_time = self.env.now if self.start_time is None else self.start_time
            yield req

            self.processor = cpu
            try:
                while self.remaining > 0:
                    quantum = min(5, self.remaining)
                    start = self.env.now
                    yield self.env.timeout(quantum)
                    self.remaining -= quantum

                self.end_time = self.env.now
                stats['completed'].append(self)
            except simpy.Interrupt:
                self.preemptions += 1
                self.checkpoints += 1
                yield self.env.timeout(CHECKPOINT_OVERHEAD)
                cpu.release(req)
                self.wait_time += self.env.now - start

                yield self.env.timeout(MIGRATION_COST)
                scheduler.notify_migration(self)
                self.env.process(self.run(cpu, stats, scheduler))


class Scheduler:
    def __init__(self):
        self.task_queue = []

    def notify_migration(self, task):
        pass


def task_generator(env, cpu, stats, scheduler):
    task_id = 0
    while True:
        yield env.timeout(random.expovariate(1.0 / TASK_ARRIVAL_RATE))
        duration = random.expovariate(1.0 / MEAN_TASK_DURATION)
        priority = random.randint(1, 10) 
        task = Task(env, task_id, duration, priority)
        env.process(task.run(cpu, stats, scheduler))
        task_id += 1


def run_simulation():
    env = simpy.Environment()
    cpu = simpy.PreemptiveResource(env, capacity=TOTAL_CPUS)
    stats = {'completed': []}
    scheduler = Scheduler()

    env.process(task_generator(env, cpu, stats, scheduler))
    env.run(until=SIM_TIME)

    response_times = [t.end_time - t.start_time for t in stats['completed']]
    wait_times = [t.wait_time for t in stats['completed']]
    utilization = (sum(t.total_duration for t in stats['completed']) / (TOTAL_CPUS * SIM_TIME)) * 100
    total_preemptions = sum(t.preemptions for t in stats['completed'])
    total_checkpoints = sum(t.checkpoints for t in stats['completed'])

    print(f"Total Tasks Completed: {len(stats['completed'])}")
    print(f"Average Response Time: {statistics.mean(response_times):.2f}s")
    print(f"Average Wait Time: {statistics.mean(wait_times):.2f}s")
    print(f"CPU Utilization: {utilization:.2f}%")
    print(f"Total Preemptions: {total_preemptions}")
    print(f"Total Checkpoints: {total_checkpoints}")

if __name__ == '__main__':
    run_simulation()