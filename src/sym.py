from sympy import *
import novelalgo
import run_algo
from generate_random_jobs import generate_random_jobs

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
E_prime_equ = Eq(E_p, (1 - gamma * a * T) * a * T + (gamma * a * T) * (P * (a * T * Rational(1, 2) + E_p) + (1 - P) * (R + O * (m - 1) / m + E_p - a * T * Rational(1, 2))))
E_prime_equ = solveset(E_prime_equ, E_p)

# Substitute in equations
E_equ = E_equ.subs(T_star, solve(T_equ, T_star)[0])
E_prime_equ = E_prime_equ.subs(T, solve(E_equ, T)[0])

# Substitute in constant values
E_prime_equ = E_prime_equ.subs([(mu, novelalgo.MEW),
                                (lam, novelalgo.LAMBDA),
                                (C, novelalgo.CHECKPOINTING_OVERHEAD),
                                (R, novelalgo.RECOVERY_OVERHEAD),
                                (O, novelalgo.MIGRATION_OVERHEAD)])

# Substitute in per test values
E_prime_equ = E_prime_equ.subs([(m, 4)])

#pprint(E_prime_equ, use_unicode=IS_USE_UNICODE)

# Run set of test jobs
jobs = [generate_random_jobs(100, run_algo.HIGHEST_PRIORITY, 10, 50) for _ in range(100)]
run_algo.run_set_of_jobs(['novelalgo'], jobs, [run_algo.NovelLambdaParams], 4, suppress_printing=True)

# Substitue P into equation
P_val = novelalgo.total_kills / novelalgo.total_preempts
print(f'P: {P_val}\n')
E_prime_equ = E_prime_equ.subs(P, P_val)
#pprint(solve(E_prime_equ, E_p), use_unicode=IS_USE_UNICODE)
pprint(E_prime_equ, use_unicode=IS_USE_UNICODE)
print(type(E_prime_equ))

# Lambdify around gamma
