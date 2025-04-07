"""
Part 2: Performance Comparisons

**Released: Wednesday, October 16**

In this part, we will explore comparing the performance
of different pipelines.
First, we will set up some helper classes.
Then we will do a few comparisons
between two or more versions of a pipeline
to report which one is faster.
"""

import part1

"""
=== Questions 1-5: Throughput and Latency Helpers ===

We will design and fill out two helper classes.

The first is a helper class for throughput (Q1).
The class is created by adding a series of pipelines
(via .add_pipeline(name, size, func))
where name is a title describing the pipeline,
size is the number of elements in the input dataset for the pipeline,
and func is a function that can be run on zero arguments
which runs the pipeline (like def f()).

The second is a similar helper class for latency (Q3).

1. Throughput helper class

Fill in the add_pipeline, eval_throughput, and generate_plot functions below.
"""

# Number of times to run each pipeline in the following results.
# You may modify this if any of your tests are running particularly slow
# or fast (though it should be at least 10).
NUM_RUNS = 10

import timeit
import matplotlib.pyplot as plt

class ThroughputHelper:
    def __init__(self):
        # Initialize the object.
        # Pipelines: a list of functions, where each function
        # can be run on no arguments.
        # (like: def f(): ... )
        self.pipelines = []

        # Pipeline names
        # A list of names for each pipeline
        self.names = []

        # Pipeline input sizes
        self.sizes = []

        # Pipeline throughputs
        # This is set to None, but will be set to a list after throughputs
        # are calculated.
        self.throughputs = None

    def add_pipeline(self, name, size, func):
        self.names.append(name)
        self.sizes.append(size)
        self.pipelines.append(func)

    def compare_throughput(self):
        # Measure the throughput of all pipelines
        # and store it in a list in self.throughputs.
        # Make sure to use the NUM_RUNS variable.
        # Also, return the resulting list of throughputs,
        # in **number of items per second.**
        self.throughputs = []

        for i in range(len(self.pipelines)):
            pipeline = self.pipelines[i]
            size = self.sizes[i]

            execution_time = timeit.timeit(pipeline, number=NUM_RUNS)
            throughput = (NUM_RUNS * size)/ execution_time
            self.throughputs.append(throughput)

    def generate_plot(self, filename):
        # Generate a plot for throughput using matplotlib.
        # You can use any plot you like, but a bar chart probably makes
        # the most sense.
        # Make sure you include a legend.
        # Save the result in the filename provided.
        plt.figure(figsize=(10, 6))
        plt.bar(self.names, self.throughputs)
        plt.xlabel("Pipeline")
        plt.ylabel("Throughput (items/second)")
        plt.title("Throughput Comparison")
        plt.legend()
        output_file = filename
        plt.savefig(output_file)
        plt.close()


"""
As your answer to this part,
return the name of the method you decided to use in
matplotlib.

(Example: "boxplot" or "scatter")
"""

def q1():
    # Return plot method (as a string) from matplotlib
    return "bar chart"

"""
2. A simple test case

To make sure your monitor is working, test it on a very simple
pipeline that adds up the total of all elements in a list. 

We will compare three versions of the pipeline depending on the
input size.
"""

LIST_SMALL = [10] * 100
LIST_MEDIUM = [10] * 100_000
LIST_LARGE = [10] * 100_000_000

def add_list(l):
    # Please use a for loop (not a built-in)
    total = 0
    for i in l:
        total += i
    return total

def q2a():
    # Create a ThroughputHelper object
    h = ThroughputHelper()
    # Add the 3 pipelines.
    # (You will need to create a pipeline for each one.)
    # Pipeline names: small, medium, large
    h.add_pipeline("small", 100, lambda: add_list(LIST_SMALL))
    h.add_pipeline("medium", 100_000, lambda: add_list(LIST_MEDIUM))
    h.add_pipeline("large", 100_000_000, lambda: add_list(LIST_LARGE))

    # Generate a plot.
    # Save the plot as 'output/q2a.png'.
    h.compare_throughput()
    h.generate_plot('output/q2a.png')

    # Finally, return the throughputs as a list.
    return h.throughputs 

"""
2b.
Which pipeline has the highest throughput?
Is this what you expected?

=== ANSWER Q2b BELOW ===
The pipeline with the highest throughput is the medium pipeline. 
I was not expecting this, I thought that the smallest data set would have the highest throughput
since it would be easier to process, so it would be much quicker. 

=== END OF Q2b ANSWER ===
"""

"""
3. Latency helper class.

Now we will create a similar helper class for latency.

The helper should assume a pipeline that only has *one* element
in the input dataset.

It should use the NUM_RUNS variable as with throughput.
"""

class LatencyHelper:
    def __init__(self):
        # Initialize the object.
        # Pipelines: a list of functions, where each function
        # can be run on no arguments.
        # (like: def f(): ... )
        self.pipelines = []

        # Pipeline names
        # A list of names for each pipeline
        self.names = []

        # Pipeline latencies
        # This is set to None, but will be set to a list after latencies
        # are calculated.
        self.latencies = None

    def add_pipeline(self, name, func):
        self.names.append(name)
        self.pipelines.append(func)

    def compare_latency(self):
        # Measure the latency of all pipelines
        # and store it in a list in self.latencies.
        # Also, return the resulting list of latencies,
        # in **milliseconds.**
        self.latencies = []

        for i in range(len(self.pipelines)):
            pipeline = self.pipelines[i]
            
            execution_time = timeit.timeit(pipeline, number=NUM_RUNS)
            latency = execution_time / NUM_RUNS * 1000

            self.latencies.append(latency)

    def generate_plot(self, filename):
        # Generate a plot for latency using matplotlib.
        # You can use any plot you like, but a bar chart probably makes
        # the most sense.
        # Make sure you include a legend.
        # Save the result in the filename provided.
        plt.figure(figsize=(10, 6))
        plt.bar(self.names, self.latencies)
        plt.xlabel("Pipeline")
        plt.ylabel("Latencies (milliseconds)")
        plt.title("Latencies Comparison")
        plt.legend()
        output_file = filename
        plt.savefig(output_file)
        plt.close()

"""
As your answer to this part,
return the number of input items that each pipeline should
process if the class is used correctly.
"""

def q3():
    # Return the number of input items in each dataset,
    # for the latency helper to run correctly.
    return 1

"""
4. To make sure your monitor is working, test it on
the simple pipeline from Q2.

For latency, all three pipelines would only process
one item. Therefore instead of using
LIST_SMALL, LIST_MEDIUM, and LIST_LARGE,
for this question run the same pipeline three times
on a single list item.
"""

LIST_SINGLE_ITEM = [10] # Note: a list with only 1 item

def q4a():
    # Create a LatencyHelper object
    h = LatencyHelper()

    # Add the single pipeline three times.
    h.add_pipeline("pipeline 1", lambda: add_list(LIST_SINGLE_ITEM))
    h.add_pipeline("pipeline 2", lambda: add_list(LIST_SINGLE_ITEM))
    h.add_pipeline("pipeline 3", lambda: add_list(LIST_SINGLE_ITEM))

    h.compare_latency()

    # Generate a plot.
    # Save the plot as 'output/q4a.png'.
    h.generate_plot('output/q4a.png')
    # Finally, return the latencies as a list.
    return h.latencies

"""
4b.
How much did the latency vary between the three copies of the pipeline?
Is this more or less than what you expected?

=== ANSWER Q4b BELOW ===
I expected to see some differences in latency because of variations in 
processing or memory access times between the pipeline copies. 
However, the latency barely changed at all. This is probably because each pipeline only 
processes a single item, which keeps things simple and so there would be a
 similar latency across all the copies.


=== END OF Q4b ANSWER ===
"""

"""
Now that we have our helpers, let's do a simple comparison.

NOTE: you may add other helper functions that you may find useful
as you go through this file.

5. Comparison on Part 1

Finally, use the helpers above to calculate the throughput and latency
of the pipeline in part 1.
"""

# You will need these:
#part1.load_input
#part1.PART_1_PIPELINE

def q5a():
    
    # Return the throughput of the pipeline in part 1.
    h = ThroughputHelper()

    h.add_pipeline("PART_1_PIPELINE", 300, lambda: part1.PART_1_PIPELINE()) # 3 dfs with 100 rows each, so input is 300

    h.compare_throughput()

    return h.throughputs


def q5b():
    # Return the latency of the pipeline in part 1.
    
    h = LatencyHelper()

    h.add_pipeline("PART_1_PIPELINE", lambda: part1.PART_1_PIPELINE())

    h.compare_latency()

    return h.latencies


"""
===== Questions 6-10: Performance Comparison 1 =====

For our first performance comparison,
let's look at the cost of getting input from a file, vs. in an existing DataFrame.

6. We will use the same population dataset
that we used in lecture 3.

Load the data using load_input() given the file name.

- Make sure that you clean the data by removing
  continents and world data!
  (World data is listed under OWID_WRL)

Then, set up a simple pipeline that computes summary statistics
for the following:

- *Year over year increase* in population, per country

    (min, median, max, mean, and standard deviation)

How you should compute this:

- For each country, we need the maximum year and the minimum year
in the data. We should divide the population difference
over this time by the length of the time period.

- Make sure you throw out the cases where there is only one year
(if any).

- We should at this point have one data point per country.

- Finally, as your answer, return a list of the:
    min, median, max, mean, and standard deviation
  of the data.

Hints:
You can use the describe() function in Pandas to get these statistics.
You should be able to do something like
df.describe().loc["min"]["colum_name"]

to get a specific value from the describe() function.

You shouldn't use any for loops.
See if you can compute this using Pandas functions only.
"""
import pandas as pd

def load_input(filename):
    # Return a dataframe containing the population data
    # **Clean the data here**

    pop_data = pd.read_csv(filename, encoding='latin-1')
    pop_data = pop_data[pop_data['Code'] != 'OWID_WRL'] 
    pop_data = pop_data.dropna(subset=['Code'])
    return pop_data

def population_pipeline(df):
    # Input: the dataframe from load_input()
    # Return a list of min, median, max, mean, and standard deviation
    pop_stats = df.groupby('Entity').agg(
        min_year=('Year', 'min'),
        max_year=('Year', 'max'),
        min_pop=('Population (historical)', 'first'),
        max_pop=('Population (historical)', 'last')
    )

    pop_stats = pop_stats[pop_stats['max_year'] > pop_stats['min_year']]
    pop_stats['pop_inc'] = (pop_stats['max_pop'] - pop_stats['min_pop']) / (pop_stats['max_year'] - pop_stats['min_year'])

    return [
        pop_stats['pop_inc'].min(),
        pop_stats['pop_inc'].median(), 
        pop_stats['pop_inc'].max(),
        pop_stats['pop_inc'].mean(),
        pop_stats['pop_inc'].std()
    ]


def q6():
    # As your answer to this part,
    # call load_input() and then population_pipeline()
    # Return a list of min, median, max, mean, and standard deviation
    return population_pipeline(load_input("data/population.csv"))


"""
7. Varying the input size

Next we want to set up three different datasets of different sizes.

Create three new files,
    - data/population-small.csv
      with the first 600 rows
    - data/population-medium.csv
      with the first 6000 rows
    - data/population-single-row.csv
      with only the first row
      (for calculating latency)

You can edit the csv file directly to extract the first rows
(remember to also include the header row)
and save a new file.

Make four versions of load input that load your datasets.
(The _large one should use the full population dataset.)
"""

def load_input_small():
    pop_data = pd.read_csv("data/population-small.csv", encoding='latin-1')
    pop_data = pop_data[pop_data['Code'] != 'OWID_WRL'] 
    pop_data = pop_data.dropna(subset=['Code'])
    return pop_data

def load_input_medium():
    pop_data = pd.read_csv("data/population-medium.csv", encoding='latin-1')
    pop_data = pop_data[pop_data['Code'] != 'OWID_WRL'] 
    pop_data = pop_data.dropna(subset=['Code'])
    return pop_data

def load_input_large():
    pop_data = pd.read_csv("data/population.csv", encoding='latin-1')
    pop_data = pop_data[pop_data['Code'] != 'OWID_WRL'] 
    pop_data = pop_data.dropna(subset=['Code'])
    return pop_data

def load_input_single_row():
    # This is the pipeline we will use for latency.
    pop_data = pd.read_csv("data/population-single-row.csv", encoding='latin-1')
    pop_data = pop_data[pop_data['Code'] != 'OWID_WRL'] 
    pop_data = pop_data.dropna(subset=['Code'])
    return pop_data


def q7():
    # Don't modify this part
    s = load_input_small()
    m = load_input_medium()
    l = load_input_large()
    x = load_input_single_row()
    return [len(s), len(m), len(l), len(x)]

"""
8.
Create baseline pipelines

First let's create our baseline pipelines.
Create four pipelines,
    baseline_small
    baseline_medium
    baseline_large
    baseline_latency

based on the three datasets above.
Each should call your population_pipeline from Q6.
"""

def baseline_small():
    return population_pipeline(load_input_small())

def baseline_medium():
    return population_pipeline(load_input_medium())


def baseline_large():
    return population_pipeline(load_input_large())

def baseline_latency():
    return population_pipeline(load_input_single_row())

def q8():
    # Don't modify this part
    _ = baseline_medium()
    return ["baseline_small", "baseline_medium", "baseline_large", "baseline_latency"]

"""
9.
Finally, let's compare whether loading an input from file is faster or slower
than getting it from an existing Pandas dataframe variable.

Create four new dataframes (constant global variables)
directly in the script.
Then use these to write 3 new pipelines:
    fromvar_small
    fromvar_medium
    fromvar_large
    fromvar_latency

As your answer to this part;
a. Generate a plot in output/q9a.png of the throughputs
    Return the list of 6 throughputs in this order:
    baseline_small, baseline_medium, baseline_large, fromvar_small, fromvar_medium, fromvar_large
b. Generate a plot in output/q9b.png of the latencies
    Return the list of 2 latencies in this order:
    baseline_latency, fromvar_latency
"""

pop = load_input("data/population.csv")
POPULATION_SMALL = pop.head(600)
POPULATION_MEDIUM = pop.head(6000)
POPULATION_LARGE = pop
POPULATION_SINGLE_ROW = pop.head(1)

def fromvar_small():
    return population_pipeline(POPULATION_SMALL)

def fromvar_medium():
    return population_pipeline(POPULATION_MEDIUM)

def fromvar_large():
    return population_pipeline(POPULATION_LARGE)

def fromvar_latency():
    return population_pipeline(POPULATION_SINGLE_ROW)

def q9a():
    # Add all 6 pipelines for a throughput comparison
    # Generate plot in ouptut/q9a.png
    # Return list of 6 throughputs
    h = ThroughputHelper()

    h.add_pipeline("baseline_small", 261, lambda: baseline_small())
    h.add_pipeline("baseline_medium", 5122, lambda: baseline_medium())
    h.add_pipeline("baseline_large", 55240, lambda: baseline_large())

    h.add_pipeline("fromvar_small", 600, lambda: fromvar_small())
    h.add_pipeline("fromvar_medium", 6000, lambda: fromvar_medium())
    h.add_pipeline("fromvar_large", 55240, lambda: fromvar_large())

    h.compare_throughput()
    
    h.generate_plot('output/q9a.png')

    return h.throughputs

def q9b():
    # Add 2 pipelines for a latency comparison
    # Generate plot in ouptut/q9b.png
    # Return list of 2 latencies
    h = LatencyHelper()

    h.add_pipeline("baseline_latency()", lambda: baseline_latency())

    h.add_pipeline("fromvar_latency", lambda: fromvar_latency())

    h.compare_latency()

    h.generate_plot('output/q9b.png')
   
    return h.latencies


"""
10.
Comment on the plots above!
How dramatic is the difference between the two pipelines?
Which differs more, throughput or latency?
What does this experiment show?

===== ANSWER Q10 BELOW =====
The throughput plot highlights that fromvar_large performs far better than the other pipelines, 
whereas the latency plot shows a slight speed advantage for fromvar_latency compared to baseline_latency. 
This experiment suggests that loading data from variables is generally faster than from files, 
especially in terms of throughput, indicating that file I/O may create slowdowns,
particularly with large datasets.


===== END OF Q10 ANSWER =====
"""

"""
===== Questions 11-14: Performance Comparison 2 =====

Our second performance comparison will explore vectorization.

Operations in Pandas use Numpy arrays and vectorization to enable
fast operations.
In particular, they are often much faster than using for loops.

Let's explore whether this is true!

11.
First, we need to set up our pipelines for comparison as before.

We already have the baseline pipelines from Q8,
so let's just set up a comparison pipeline
which uses a for loop to calculate the same statistics.

Create a new pipeline:
- Iterate through the dataframe entries. You can assume they are sorted.
- Manually compute the minimum and maximum year for each country.
- Add all of these to a Python list. Then manually compute the summary
  statistics for the list (min, median, max, mean, and standard deviation).
"""

def for_loop_pipeline(df):

    # Input: the dataframe from load_input()
    # Return a list of min, median, max, mean, and standard deviation
    pop_stats = []

    for entity in df['Entity'].unique():
        countries = df[df['Entity'] == entity]
        min_year = countries['Year'].min()
        max_year = countries['Year'].max()

        if max_year > min_year:
            min_pop = countries.loc[countries['Year'] == min_year, 'Population (historical)'].values[0]
            max_pop = countries.loc[countries['Year'] == max_year, 'Population (historical)'].values[0]
            pop_inc = (max_pop - min_pop) / (max_year - min_year)
            pop_stats.append(pop_inc)

    pop_stats_df = pd.DataFrame(pop_stats, columns=['pop_inc'])

    return [
        pop_stats_df['pop_inc'].min(),
        pop_stats_df['pop_inc'].median(), 
        pop_stats_df['pop_inc'].max(),
        pop_stats_df['pop_inc'].mean(),
        pop_stats_df['pop_inc'].std()
    ]


def q11():
    # As your answer to this part, call load_input() and then
    # for_loop_pipeline() to return the 5 numbers.
    # (these should match the numbers you got in Q6.)
    return for_loop_pipeline(load_input("data/population.csv"))

"""
12.
Now, let's create our pipelines for comparison.

As before, write 4 pipelines based on the datasets from Q7.
"""

def for_loop_small():
    return for_loop_pipeline(POPULATION_SMALL)

def for_loop_medium():
    return for_loop_pipeline(POPULATION_MEDIUM)

def for_loop_large():
    return for_loop_pipeline(POPULATION_LARGE)

def for_loop_latency():
    return for_loop_pipeline(POPULATION_SINGLE_ROW)

def q12():
    # Don't modify this part
    _ = for_loop_medium()
    return ["for_loop_small", "for_loop_medium", "for_loop_large", "for_loop_latency"]

"""
13.
Finally, let's compare our two pipelines,
as we did in Q9.

a. Generate a plot in output/q13a.png of the throughputs
    Return the list of 6 throughputs in this order:
    baseline_small, baseline_medium, baseline_large, for_loop_small, for_loop_medium, for_loop_large

b. Generate a plot in output/q13b.png of the latencies
    Return the list of 2 latencies in this order:
    baseline_latency, for_loop_latency
"""

def q13a():
    # Add all 6 pipelines for a throughput comparison
    # Generate plot in ouptut/q13a.png
    # Return list of 6 throughputs
    h = ThroughputHelper()

    h.add_pipeline("baseline_small", 600, lambda: baseline_small())
    h.add_pipeline("baseline_medium", 6000, lambda: baseline_medium())
    h.add_pipeline("baseline_large", 59051, lambda: baseline_large())

    h.add_pipeline("for_loop_small", 600, lambda: for_loop_small())
    h.add_pipeline("for_loop_medium", 6000, lambda: for_loop_medium())
    h.add_pipeline("for_loop_large", 59051, lambda: for_loop_large())

    h.compare_throughput()
    
    h.generate_plot('output/13a.png')

    return h.throughputs

def q13b():
    # Add 2 pipelines for a latency comparison
    # Generate plot in ouptut/q13b.png
    # Return list of 2 latencies
    h = LatencyHelper()

    h.add_pipeline("baseline_latency()", lambda: baseline_latency())

    h.add_pipeline("fromvar_latency", lambda: for_loop_latency())

    h.compare_latency()

    h.generate_plot('output/13b.png')
   
    return h.latencies


"""
14.
Comment on the results you got!

14a. Which pipelines is faster in terms of throughput?

===== ANSWER Q14a BELOW =====
For large datasets, the baseline pipeline offers higher throughput, 
while the for loop performs faster on small and medium datasets.

===== END OF Q14a ANSWER =====

14b. Which pipeline is faster in terms of latency?

===== ANSWER Q14b BELOW =====
The for loop pipeline achieves lower latency due to minimal setup costs 
and overhead, along with more efficient cache utilization.


===== END OF Q14b ANSWER =====

14c. Do you notice any other interesting observations?
What does this experiment show?

===== ANSWER Q14c BELOW =====
This experiment shows that, for smaller datasets, a for loop is typically more efficient than directly loading input from a file. 
However, for the largest dataset, the baseline pipeline proved to be much faster.

===== END OF Q14c ANSWER =====
"""

"""
===== Questions 15-17: Reflection Questions =====
15.

Take a look at all your pipelines above.
Which factor that we tested (file vs. variable, vectorized vs. for loop)
had the biggest impact on performance?

===== ANSWER Q15 BELOW =====
It seems that loading the data into a pandas variable improves performance compared to loading the data from a csv file directly. 
For a for loop vs. vectorized it is a bit different since the throughputs for the baseline_large was the highest, but the latency for from_var was the lowest. 
All the throughputs for the for loops pipelines were pretty low though, so it seems that the pandas variable improved performance the most.


===== END OF Q15 ANSWER =====

16.
Based on all of your plots, form a hypothesis as to how throughput
varies with the size of the input dataset.

(Any hypothesis is OK as long as it is supported by your data!
This is an open ended question.)

===== ANSWER Q16 BELOW =====
It seems that throughput tends to increase when the size gets larger.
 We can see this from the baseline and from_var pipelines. 
 However, this is not the case for the for loop pipelines, which is interesting.


===== END OF Q16 ANSWER =====

17.
Based on all of your plots, form a hypothesis as to how
throughput is related to latency.

(Any hypothesis is OK as long as it is supported by your data!
This is an open ended question.)

===== ANSWER Q17 BELOW =====
The throughput and latency seem to be inversely correlated. When the throughput is higher, the latency for that pipeline mostly is lower. 
This is mostly because if more items per second are processed it takes less time to run the pipeline, so fewer milliseconds (latency).

===== END OF Q17 ANSWER =====
"""


"""
===== Extra Credit =====

This part is optional.

Use your pipeline to compare something else!

Here are some ideas for what to try:
- the cost of random sampling vs. the cost of getting rows from the
  DataFrame manually
- the cost of cloning a DataFrame
- the cost of sorting a DataFrame prior to doing a computation
- the cost of using different encodings (like one-hot encoding)
  and encodings for null values
- the cost of querying via Pandas methods vs querying via SQL
  For this part: you would want to use something like
  pandasql that can run SQL queries on Pandas data frames. See:
  https://stackoverflow.com/a/45866311/2038713

As your answer to this part,
as before, return
a. the list of 6 throughputs
and
b. the list of 2 latencies.

and generate plots for each of these in the following files:
    output/extra_credit_a.png
    output/extra_credit_b.png
"""

# Extra credit (optional)

def extra_credit_a():
    return "N/A"

def extra_credit_b():
    return "N/A"

"""
===== Wrapping things up =====

**Don't modify this part.**

To wrap things up, we have collected
your answers and saved them to a file below.
This will be run when you run the code.
"""

ANSWER_FILE = "output/part2-answers.txt"
UNFINISHED = 0

def log_answer(name, func, *args):
    try:
        answer = func(*args)
        print(f"{name} answer: {answer}")
        with open(ANSWER_FILE, 'a') as f:
            f.write(f'{name},{answer}\n')
            print(f"Answer saved to {ANSWER_FILE}")
    except NotImplementedError:
        print(f"Warning: {name} not implemented.")
        with open(ANSWER_FILE, 'a') as f:
            f.write(f'{name},Not Implemented\n')
        global UNFINISHED
        UNFINISHED += 1

def PART_2_PIPELINE():
    open(ANSWER_FILE, 'w').close()

    # Q1-5
    log_answer("q1", q1)
    log_answer("q2a", q2a)
    # 2b: commentary
    log_answer("q3", q3)
    log_answer("q4a", q4a)
    # 4b: commentary
    log_answer("q5a", q5a)
    log_answer("q5b", q5b)

    # Q6-10
    log_answer("q6", q6)
    log_answer("q7", q7)
    log_answer("q8", q8)
    log_answer("q9a", q9a)
    log_answer("q9b", q9b)
    # 10: commentary

    # Q11-14
    log_answer("q11", q11)
    log_answer("q12", q12)
    log_answer("q13a", q13a)
    log_answer("q13b", q13b)
    # 14: commentary

    # 15-17: reflection
    # 15: commentary
    # 16: commentary
    # 17: commentary

    # Extra credit
    log_answer("extra credit (a)", extra_credit_a)
    log_answer("extra credit (b)", extra_credit_b)

    # Answer: return the number of questions that are not implemented
    if UNFINISHED > 0:
        print("Warning: there are unfinished questions.")

    return UNFINISHED

"""
=== END OF PART 2 ===

Main function
"""

if __name__ == '__main__':
    log_answer("PART 2", PART_2_PIPELINE)
