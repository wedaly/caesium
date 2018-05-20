#!/usr/bin/env python
import sys
import os.path
import numpy as np

DIR = os.path.dirname(os.path.abspath(__file__))
MAX_VAL = 0xFFFFFFFFFFFFFFFF
SIZES = [10 ** p for p in range(4, 9)]


def main():
    for size in SIZES:
        write_data("uniform", size, sample_uniform)
        write_data("normal", size, sample_normal)
        write_data("pareto", size, sample_pareto)


def write_data(distribution, size, sample_gen):
    filename = "{}_{}.txt".format(distribution, size)
    path = os.path.join(DIR, filename)
    with open(path, "w") as f:
        for val in sample_gen(size):
            f.write("{}\n".format(val))


def sample_uniform(size):
    for s in np.random.uniform(low=0, high=MAX_VAL, size=size):
        yield int(s)


def sample_normal(size):
    for s in np.random.normal(size=size):
        yield abs(int(s * MAX_VAL))


def sample_pareto(size):
    for s in np.random.pareto(3, size):
        yield int((s + 1) * (MAX_VAL / 2))


if __name__ == "__main__":
    main()
