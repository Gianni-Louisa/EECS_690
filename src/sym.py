import numpy as np
from sympy import *
from scipy.optimize import minimize
import matplotlib.pyplot as plt

import novelalgo
import run_algo
from generate_random_jobs import generate_random_jobs\

# Debug stuff
IS_USE_UNICODE = False

# Symbol Declataration
## Error bits
## T := E(T*),
T_star, mu, C, lam, T, R = symbols('T_star mu C lam T R')

## Preemption bits
## E_p := E'(T), O := O_{migrate}
E_p, gamma, a, O, m, P = symbols('E_p gamma a O m P')

# Get equations for E(T*)
T_equ = Eq(T_star, sqrt(2 * mu * C))
E_equ = Eq(T, (E ** (lam * T_star) - 1) * (1 / lam + R) + C)

# Get equation for E'(T)
E_prime_equ = Eq(E_p, (E ** (-1 * gamma * a * T)) * a * T + (1 - E ** (-1 * gamma * a * T)) * (P * (a * T * Rational(1, 2) + E_p) + (1 - P) * (R + O * (m - 1) / m + E_p - a * T * Rational(1, 2))))
E_prime_equ = Eq(E_p, solve(E_prime_equ, E_p)[0])

# Substitute in equations
E_equ = E_equ.subs(T_star, solve(T_equ, T_star)[0])
E_prime_equ = E_prime_equ.subs(T, solve(E_equ, T)[0])

# Substitute in constant values
constant_bindings = [(mu, novelalgo.MEW),
                     (lam, novelalgo.LAMBDA),
                     (C, novelalgo.CHECKPOINTING_OVERHEAD),
                     (R, novelalgo.RECOVERY_OVERHEAD),
                     (O, novelalgo.MIGRATION_OVERHEAD),
                     (m, 4)]

E_prime_equ = E_prime_equ.subs(constant_bindings)

# Run set of test jobs
jobs = [generate_random_jobs(100, run_algo.HIGHEST_PRIORITY, 25, 50) for _ in range(1000)]
run_algo.run_set_of_jobs(['novelalgo'], jobs, [run_algo.NovelLambdaParams], 4, suppress_printing=True)

# Substitue P into equation
P_val = novelalgo.total_kills / novelalgo.total_preempts
print(f'P: {P_val}\n')
E_prime_equ = E_prime_equ.subs(P, P_val)
E_prime_expr = solve(E_prime_equ, E_p)[0]
pprint(E_prime_expr, use_unicode=IS_USE_UNICODE)
print()

# Lambdify expected time function
E_prime_fn = lambdify([a, gamma], E_prime_expr)

# Generate stats to fit data
a_list = list(set([stat[0] for stat in run_algo.job_stats_a_to_T]))
job_stats = [(a_val, [stat[1] for stat in run_algo.job_stats_a_to_T if stat[0] == a_val]) for a_val in a_list]
job_stats.sort(key=lambda x : x[0])

job_stats = [(stat[0], sum(stat[1]) / len(stat[1])) for stat in job_stats]

# Fit data to one gamma

# Least squares function for minimization
def least_squares(gamma):
    return sum([(stat[1] - E_prime_fn(stat[0], gamma[0])) ** 2 for stat in job_stats])

# Calculate best gamma
best_gamma = minimize(least_squares, [0.001]).x[0]
print(f'Best gamma: {best_gamma}')

# Print job statistics
for stat in job_stats:
    gamma = best_gamma
    print(f'{stat}, {E_prime_fn(stat[0], gamma)} , {abs(stat[1] - E_prime_fn(stat[0], gamma))}')
print(f'Least squares: {least_squares([best_gamma])}')
print()

# Fit gamma based on priority

# Generate priority stats

p_list = list(set([stat[1] for stat in run_algo.job_stats_with_p]))
job_stats_p = []

for p in p_list:
    stat_list_total = []
    for a in a_list:
        stat_list = [stat[2] for stat in run_algo.job_stats_with_p if stat[0] == a and stat[1] == p]
        if len(stat_list) != 0:
            stat_list_total.append((a, sum(stat_list) / len(stat_list)))

    stat_list_total.sort(key=lambda x : x[0])

    job_stats_p.append((p, stat_list_total))

job_stats_p.sort(key=lambda x : x[0])

# Generate best gamma for each p
best_gamma_list = []
reg_list = []

for job_stat in job_stats_p:
    best_gamma = minimize(least_squares, [0.001]).x[0]
    best_gamma_list.append(best_gamma)
    reg_list.append(least_squares([best_gamma]))
    
    print(f'{job_stat[0]}: {best_gamma} with reg. {least_squares([best_gamma])}')

print(f'Average reg. {sum(reg_list) / len(reg_list)}')

# Graph results

plt.figure()

x = [novelalgo.PERIOD * stat[0] for stat in job_stats]
y = [stat[1] for stat in job_stats]

x_2 = np.linspace(x[0], x[-1], 1000)
y_2 = [E_prime_fn(x / novelalgo.PERIOD, best_gamma) for x in x_2]

plt.plot(x_2, y_2, label='Calculated Expected Average Runtime', color='red')
plt.scatter(x, y, label='Actual Average Runtime')

plt.title('Average Runtime vs Job Length')
plt.xlabel('Job Length (ticks)')
plt.ylabel('Actual Runtime (ticks)')
plt.legend()

plt.show()
