"""
Part 3: Measuring Performance

Now that you have drawn the dataflow graph in part 2,
this part will explore how performance of real pipelines
can differ from the theoretical model of dataflow graphs.

We will measure the performance of your pipeline
using your ThroughputHelper and LatencyHelper from HW1.

=== Coding part 1: making the input size and partitioning configurable ===

We would like to measure the throughput and latency of your PART1_PIPELINE,
but first, we need to make it configurable in:
(i) the input size
(ii) the level of parallelism.

Currently, your pipeline should have two inputs, load_input() and load_input_bigger().
You will need to change part1 by making the following additions:

- Make load_input and load_input_bigger take arguments that can be None, like this:

    def load_input(N=None, P=None)

    def load_input_bigger(N=None, P=None)

You will also need to do the same thing to q8_a and q8_b:

    def q8_a(N=None, P=None)

    def q8_b(N=None, P=None)

Here, the argument N = None is an optional parameter that, if specified, gives the size of the input
to be considered, and P = None is an optional parameter that, if specifed, gives the level of parallelism
(number of partitions) in the RDD.

You will need to make both functions work with the new signatures.
Be careful to check that the above changes should preserve the existing functionality of part1
(so python3 part1.py should still give the same output as before!)

Don't make any other changes to the function sigatures.

Once this is done, define a *new* version of the PART_1_PIPELINE, below,
that takes as input the parameters N and P.
(This time, you don't have to consider the None case.)
You should not modify the existing PART_1_PIPELINE.

You may either delete the parts of the code that save the output file, or change these to a different output file like part1-answers-temp.txt.
"""

# Spark boilerplate (remember to always add this at the top of any Spark file)
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("DataflowGraphExample").getOrCreate()
sc = spark.sparkContext

import part1 as p1

def num_to_english(n):
    # Dictionaries for numbers to words
    ones = [
        "", "one", "two", "three", "four", "five", "six", "seven", "eight", "nine", 
        "ten", "eleven", "twelve", "thirteen", "fourteen", "fifteen", "sixteen", 
        "seventeen", "eighteen", "nineteen"
    ]
    tens = ["", "", "twenty", "thirty", "forty", "fifty", "sixty", "seventy", "eighty", "ninety"]
    thousands = ["", "thousand", "million"]

    # Convert numbers below 100 to English
    def hundred_and(digits):
        if digits == 0:
            return ""
        elif digits < 20:
            return ones[digits]
        elif digits < 100:
            return tens[digits // 10] + (" " + ones[digits % 10] if digits % 10 != 0 else "")
        else:
            return ones[digits // 100] + " hundred" + (" and " + hundred_and(digits % 100) if digits % 100 != 0 else "")
    
    # Returns 0 for 0
    if n == 0:
        return "zero"
    
    result = ""
    # Process thousands, million, and higher
    for i, word in enumerate(thousands):
        if n % 1000 != 0:
            result = hundred_and(n % 1000) + (" " + word if word else "") + (" " + result if result else "")
        n //= 1000
    return result.strip() # Remove any leading/trailing spaces.


def q7(rdd):
    # Input: the RDD from Q4
    # Output: a tulpe (most common char, most common frequency, least common char, least common frequency)
    rdd_1 = rdd.flatMap(lambda number: list(num_to_english(number).replace(" ", "").replace("-", "").lower()))
    rdd_2 = rdd_1.map(lambda char: (char, 1))

    # Aggregate counts by character
    rdd_3 = p1.general_reduce(rdd_2, lambda a, b: a + b)

    # Collect and find most/least common characters
    count_of_chars = rdd_3.collect()
    most_common = max(count_of_chars, key=lambda x: x[1])
    least_common = min(count_of_chars, key=lambda x: x[1])

    return (most_common[0], most_common[1], least_common[0], least_common[1])


def q8_b(N=None, P=None):
    # version of Q7
    # It should call into q7() with the new RDD!
    # Don't re-implemented the q7 logic.
    # Output: a tulpe (most common char, most common frequency, least common char, least common frequency)

    # RDD made from load input bigger function
    rdd = p1.load_input_bigger(N, P)

    # Calls q6 funciton
    return q7(rdd)

def log_answer(name, func, *args):
    try:
        answer = func(*args)
        print(f"{name} answer: {answer}")
    except NotImplementedError:
        print(f"Warning: {name} not implemented.")
        
def PART_1_PIPELINE_PARAMETRIC(N, P):
    """
    TODO: Follow the same logic as PART_1_PIPELINE
    N = number of inputs
    P = parallelism (number of partitions)
    (You can copy the code here), but make the following changes:
    - load_input should use an input of size N.
    - load_input_bigger (including q8_a and q8_b) should use an input of size N.
    - both of these should return an RDD with level of parallelism P (number of partitions = P).
    """
        
    dfs = p1.load_input(N, P)

    # Questions 1-3
    log_answer("q1", p1.q1)
    log_answer("q2", p1.q2)
    # 3: commentary

    # Questions 4-10
    log_answer("q4", p1.q4, dfs)
    log_answer("q5", p1.q5, dfs)
    log_answer("q6", p1.q6, dfs)
    log_answer("q7", q7, dfs)
    log_answer("q8a", p1.q8_a, N, P)
    log_answer("q8b", q8_b, N, P)
    # 9: commentary
    # 10: commentary

    # Questions 11-18
    log_answer("q11", p1.q11, dfs)
    # 12: commentary
    # 13: commentary
    log_answer("q14", p1.q14, dfs)
    # 15: commentary
    log_answer("q16a", p1.q16_a)
    log_answer("q16b", p1.q16_b)
    log_answer("q16c", p1.q16_c)
    # 17: commentary
    # 18: commentary

    # Questions 19-20
    # 19: commentary
    log_answer("q20", p1.q20)

"""
=== Coding part 2: measuring the throughput and latency ===

Now we are ready to measure the throughput and latency.

To start, copy the code for ThroughputHelper and LatencyHelper from HW1 into this file.

Then, please measure the performance of PART1_PIPELINE as a whole
using five levels of parallelism:
- parallelism 1
- parallelism 2
- parallelism 4
- parallelism 8
- parallelism 16

For each level of parallelism, you should measure the throughput and latency as the number of input
items increases, using the following input sizes:
- N = 1, 10, 100, 1000, 10_000, 100_000, 1_000_000.

- Note that the larger sizes may take a while to run (for example, up to 30 minutes). You can try with smaller sizes to test your code first.

You can generate any plots you like (for example, a bar chart or an x-y plot on a log scale,)
but store them in the following 10 files,
where the file name corresponds to the level of parallelism:

output/part3-throughput-1.png
output/part3-throughput-2.png
output/part3-throughput-4.png
output/part3-throughput-8.png
output/part3-throughput-16.png
output/part3-latency-1.png
output/part3-latency-2.png
output/part3-latency-4.png
output/part3-latency-8.png
output/part3-latency-16.png

Clarifying notes:

- To control the level of parallelism, use the N, P parameters in your PART_1_PIPELINE_PARAMETRIC above.

- Make sure you sanity check the output to see if it matches what you would expect! The pipeline should run slower
  for larger input sizes N (in general) and for fewer number of partitions P (in general).

- For throughput, the "number of input items" should be 2 * N -- that is, N input items for load_input, and N for load_input_bigger.

- For latency, please measure the performance of the code on the entire input dataset
(rather than a single input row as we did on HW1).
MapReduce is batch processing, so all input rows are processed as a batch
and the latency of any individual input row is the same as the latency of the entire dataset.
That is why we are assuming the latency will just be the running time of the entire dataset.

- Set `NUM_RUNS` to `1` if you haven't already. Note that this will make the values for low numbers (like `N=1`, `N=10`, and `N=100`) vary quite unpredictably.
"""

import timeit
import matplotlib.pyplot as plt

NUM_RUNS = 1

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
        plt.yscale("log")
        plt.title("Throughput Comparison (Log Scale)")
        plt.legend()
        output_file = filename
        plt.savefig(output_file)
        plt.close()


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
=== Reflection part ===

Once this is done, write a reflection and save it in
a text file, output/part3-reflection.txt.

I would like you to think about and answer the following questions:

1. What would we expect from the throughput and latency
of the pipeline, given only the dataflow graph?

Use the information we have seen in class. In particular,
how should throughput and latency change when we double the amount of parallelism?

Please ignore pipeline and task parallelism for this question.
The parallelism we care about here is data parallelism.

2. In practice, does the expectation from question 1
match the performance you see on the actual measurements? Why or why not?

State specific numbers! What was the expected throughput and what was the observed?
What was the expected latency and what was the observed?

3. Finally, use your answers to Q1-Q2 to form a conjecture
as to what differences there are (large or small) between
the theoretical model and the actual runtime.
Name some overheads that might be present in the pipeline
that are not accounted for by our theoretical model of
dataflow graphs that could affect performance.

You should list an explicit conjecture in your reflection, like this:

    Conjecture: I conjecture that ....

You may have different conjectures for different parallelism cases.
For example, for the parallelism=4 case vs the parallelism=16 case,
if you believe that different overheads are relevant for these different scenarios.

=== Grading notes ===

Don't forget to fill out the entrypoint below before submitting your code!
Running python3 part3.py should work and should re-generate all of your plots in output/.

In the reflection, please write at least a paragraph for each question. (5 sentences each)

Please include specific numbers in your reflection (particularly for Q2).

=== Entrypoint ===
"""

if __name__ == '__main__':
    print("Complete part 3. Please use the main function below to generate your plots so that they are regenerated whenever the code is run:")
    
    # List of values for N (input size) and P (# of partitions)
    N_values = [1, 10, 100, 1000, 10000, 100000, 1000000]
    P_values = [1, 2, 4, 8, 16]

    for P in P_values:
        # Initialize throughput and latency helpers
        h = ThroughputHelper()
        l = LatencyHelper()

        # Loop over each data size (N).
        for N in N_values:
            # Add pipelines for throughput and latency with parameters (N, P)
            pipeline_name = f"{N}_{P}"
            h.add_pipeline(pipeline_name, N * 2, lambda: PART_1_PIPELINE_PARAMETRIC(N, P))
            l.add_pipeline(pipeline_name, lambda: PART_1_PIPELINE_PARAMETRIC(N, P))

        # Calculate throughputs and latencies across all pipelines and generate plots
        h.compare_throughput()
        print(h.throughputs)
        h.generate_plot(f'output/part3-throughput-{P}.png')

        l.compare_latency()
        print(l.latencies)
        l.generate_plot(f'output/part3-latency-{P}.png')




