"""
Part 1: MapReduce

In our first part, we will practice using MapReduce
to create several pipelines.
This part has 20 questions.

As you complete your code, you can run the code with

    python3 part1.py
    pytest part1.py

and you can view the output so far in:

    output/part1-answers.txt

In general, follow the same guidelines as in HW1!
Make sure that the output in part1-answers.txt looks correct.
See "Grading notes" here:
https://github.com/DavisPL-Teaching/119-hw1/blob/main/part1.py

For Q5-Q7, make sure your answer uses general_map and general_reduce as much as possible.
You will still need a single .map call at the beginning (to convert the RDD into key, value pairs), but after that point, you should only use general_map and general_reduce.

If you aren't sure of the type of the output, please post a question on Piazza.
"""

# Spark boilerplate (remember to always add this at the top of any Spark file)
import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("DataflowGraphExample").getOrCreate()
sc = spark.sparkContext

# Additional imports
import pytest

"""
===== Questions 1-3: Generalized Map and Reduce =====

We will first implement the generalized version of MapReduce.
It works on (key, value) pairs:

- During the map stage, for each (key1, value1) pairs we
  create a list of (key2, value2) pairs.
  All of the values are output as the result of the map stage.

- During the reduce stage, we will apply a reduce_by_key
  function (value2, value2) -> value2
  that describes how to combine two values.
  The values (key2, value2) will be grouped
  by key, then values of each key key2
  will be combined (in some order) until there
  are no values of that key left. It should end up with a single
  (key2, value2) pair for each key.

1. Fill in the general_map function
using operations on RDDs.

If you have done it correctly, the following test should pass.
(pytest part1.py)

Don't change the q1() answer. It should fill out automatically.
"""

def general_map(rdd, f):
    """
    rdd: an RDD with values of type (k1, v1)
    f: a function (k1, v1) -> List[(k2, v2)]
    output: an RDD with values of type (k2, v2)
    """
    return rdd.flatMap(lambda pair: f(pair[0], pair[1])) # Use flatMap to change and flatten the output of the function


# Remove skip when implemented!
def test_general_map():
    rdd = sc.parallelize(["cat", "dog", "cow", "zebra"])

    # Use first character as key
    rdd1 = rdd.map(lambda x: (x[0], x))

    # Map returning no values
    rdd2 = general_map(rdd1, lambda k, v: [])

    # Map returning length
    rdd3 = general_map(rdd1, lambda k, v: [(k, len(v))])
    rdd4 = rdd3.map(lambda pair: pair[1])

    # Map returnning odd or even length
    rdd5 = general_map(rdd1, lambda k, v: [(len(v) % 2, ())])

    assert rdd2.collect() == []
    assert sum(rdd4.collect()) == 14
    assert set(rdd5.collect()) == set([(1, ())])

def q1():
    # Answer to this part: don't change this
    rdd = sc.parallelize(["cat", "dog", "cow", "zebra"])
    rdd1 = rdd.map(lambda x: (x[0], x))
    rdd2 = general_map(rdd1, lambda k, v: [(1, v[-1])])
    return sorted(rdd2.collect())

"""
2. Fill in the reduce function using operations on RDDs.

If you have done it correctly, the following test should pass.
(pytest part1.py)

Don't change the q2() answer. It should fill out automatically.
"""

def general_reduce(rdd, f):
    """
    rdd: an RDD with values of type (k2, v2)
    f: a function (v2, v2) -> v2
    output: an RDD with values of type (k2, v2),
        and just one single value per key
    """
    return rdd.reduceByKey(f) # Combine all values for each key using the function given


# Remove skip when implemented!
def test_general_reduce():
    rdd = sc.parallelize(["cat", "dog", "cow", "zebra"])

    # Use first character as key
    rdd1 = rdd.map(lambda x: (x[0], x))

    # Reduce, concatenating strings of the same key
    rdd2 = general_reduce(rdd1, lambda x, y: x + y)
    res2 = set(rdd2.collect())

    # Reduce, adding lengths
    rdd3 = general_map(rdd1, lambda k, v: [(k, len(v))])
    rdd4 = general_reduce(rdd3, lambda x, y: x + y)
    res4 = sorted(rdd4.collect())

    assert (
        res2 == set([('c', "catcow"), ('d', "dog"), ('z', "zebra")])
        or res2 == set([('c', "cowcat"), ('d', "dog"), ('z', "zebra")])
    )
    assert res4 == [('c', 6), ('d', 3), ('z', 5)]

def q2():
    # Answer to this part: don't change this
    rdd = sc.parallelize(["cat", "dog", "cow", "zebra"])
    rdd1 = rdd.map(lambda x: (x[0], x))
    rdd2 = general_reduce(rdd1, lambda x, y: "hello")
    return sorted(rdd2.collect())


"""
3. Name one scenario where having the keys for Map
and keys for Reduce be different might be useful.

=== ANSWER Q3 BELOW ===

Having different keys for Map versus Reduce might be helpful for when, for example, you use map
to take emails from each sender and then you use reduce to group the emails by which of the recipeint's 
email adresses they were sent to. The stages use different keys in this scenario.

=== END OF Q3 ANSWER ===
"""

"""
===== Questions 4-10: MapReduce Pipelines =====

Now that we have our generalized MapReduce function,
let's do a few exercises.
For the first set of exercises, we will use a simple dataset that is the
set of integers between 1 and 1 million (inclusive).

4. First, we need a function that loads the input.
"""

def load_input(N=None, P=None):
    if N is None:
        N = 1_000_000 # Default is 1,000,000 if N is not given
    
    data = range(1, N+1) # Create a range of numbers from 1 to N (inclusive)
    
    if P is None:
        P = 8  # Default to 8 partitions if P is not given since # of cores is 8
    
    return sc.parallelize(data, P)  # Make an RDD with the specific # of partitions


def q4(rdd):
    # Input: the RDD from load_input
    # Output: the length of the dataset.
    # You may use general_map or general_reduce here if you like (but you don't have to) to get the total count.
    return rdd.count() # Count items in the RDD

"""
Now use the general_map and general_reduce functions to answer the following questions.

5. Among the numbers from 1 to 1 million, what is the average value?
"""

def q5(rdd):
    # Input: the RDD from Q4
    # Output: the average value

    # Map each number in the RDD to a key-value pair with the key as None.
    rdd_1 = rdd.map(lambda num: (None, num))

    # Change the RDD into key-value pairs where the key is 1, and the value is (num, 1).
    rdd_2 = general_map(rdd_1, lambda _, num: [(1, (num, 1))])

    # Reduce the RDD by adding both the numbers and their counts.
    rdd_3 = general_reduce(rdd_2, lambda a, b: (a[0] + b[0], a[1] + b[1]))

    # Get total sum and total count from the RDD.
    total_sum, total_count = rdd_3.collect()[0][1]
    
    # Avg calculation
    return total_sum / total_count

"""
6. Among the numbers from 1 to 1 million, when written out,
which digit is most common, with what frequency?
And which is the least common, with what frequency?

(If there are ties, you may answer any of the tied digits.)

The digit should be either an integer 0-9 or a character '0'-'9'.
Frequency is the number of occurences of each value.

Your answer should use the general_map and general_reduce functions as much as possible.
"""

def q6(rdd):
    # Input: the RDD from Q4
    # Output: a tuple (most common digit, most common frequency, least common digit, least common frequency)
    rdd_1 = rdd.flatMap(lambda num: [(digit, 1) for digit in str(num)])

    # Aggregate counts by digit
    rdd_2 = general_reduce(rdd_1, lambda a, b: a + b)

    # Collect and find most/least common digits
    digit_counts = rdd_2.collect()
    most_common = max(digit_counts, key=lambda x: x[1])
    least_common = min(digit_counts, key=lambda x: x[1])

    return (most_common[0], most_common[1], least_common[0], least_common[1])


"""
7. Among the numbers from 1 to 1 million, written out in English, which letter is most common?
With what frequency?
The least common?
With what frequency?

(If there are ties, you may answer any of the tied characters.)

For this part, you will need a helper function that computes
the English name for a number.
Examples:

    0 = zero
    71 = seventy one
    513 = five hundred and thirteen
    801 = eight hundred and one
    999 = nine hundred and ninety nine
    1001 = one thousand one
    500,501 = five hundred thousand five hundred and one
    555,555 = five hundred and fifty five thousand five hundred and fifty five
    1,000,000 = one million

Notes:
- For "least frequent", count only letters which occur,
  not letters which don't occur.
- Please ignore spaces and hyphens.
- Use lowercase letters.
- The word "and" should always appear after the "hundred" part (where present),
  but nowhere else.
  (Note the 1001 case above which differs from some other implementations.)
- Please implement this without using an external library such as `inflect`.
"""

# *** Define helper function(s) here ***

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
    rdd_3 = general_reduce(rdd_2, lambda a, b: a + b)

    # Collect and find most/least common characters
    count_of_chars = rdd_3.collect()
    most_common = max(count_of_chars, key=lambda x: x[1])
    least_common = min(count_of_chars, key=lambda x: x[1])

    return (most_common[0], most_common[1], least_common[0], least_common[1])


"""
8. Does the answer change if we have the numbers from 1 to 100,000,000?

Make a version of both pipelines from Q6 and Q7 for this case.
You will need a new load_input function.
"""

def load_input_bigger(N=None, P=None):
    if N is None:
        N = 10_000_000 # Default is 10,000,000 if N is not given
    
    data = range(1, N+1) # Create a range of numbers from 1 to N (inclusive)
    
    if P is None:
        P = 8  # Default to 8 partitions if P is not given since # of cores is 8
    
    return sc.parallelize(data, P)  # Make an RDD with the specific # of partitions


def q8_a(N=None, P=None):
    # version of Q6
    # It should call into q6() with the new RDD!
    # Don't re-implemented the q6 logic.
    # Output: a tuple (most common digit, most common frequency, least common digit, least common frequency)

    # RDD made from load input bigger function
    rdd = load_input_bigger(N, P) 

    # Calls q6 funciton
    return q6(rdd)

def q8_b(N=None, P=None):
    # version of Q7
    # It should call into q7() with the new RDD!
    # Don't re-implemented the q7 logic.
    # Output: a tulpe (most common char, most common frequency, least common char, least common frequency)

    # RDD made from load input bigger function
    rdd = load_input_bigger(N, P)

    # Calls q6 funciton
    return q7(rdd)

"""
Discussion questions

9. State the values of k1, v1, k2, and v2 for your Q6 and Q7 pipelines.

=== ANSWER Q9 BELOW ===

For Q6 k1 is the specific digit and for Q7 k1 is the specific English character, v1 for both questions is 1 to indicate
that the digit or character occurs once. Then in Q6 k2 is still the specific digit and for Q7 k2 is still the 
specific English character. However, for both Q6 and Q7 v2 is the count of how many times the digit or character occurs.

=== END OF Q9 ANSWER ===

10. Do you think it would be possible to compute the above using only the
"simplified" MapReduce we saw in class? Why or why not?

=== ANSWER Q10 BELOW ===

No, it would not be possible. Since we would not use functions necessary to partition by key and
to count the occurences of the digits and characters, we would need that.

=== END OF Q10 ANSWER ===
"""

"""
===== Questions 11-18: MapReduce Edge Cases =====

For the remaining questions, we will explore two interesting edge cases in MapReduce.

11. One edge case occurs when there is no output for the reduce stage.
This can happen if the map stage returns an empty list (for all keys).

Demonstrate this edge case by creating a specific pipeline which uses
our data set from Q4. It should use the general_map and general_reduce functions.

Output a set of (key, value) pairs after the reduce stage.
"""

def q11(rdd):
    # Input: the RDD from Q4
    # Output: the result of the pipeline, a set of (key, value) pairs

    # Map each number in the RDD to a key-value pair with key as None.
    rdd_1 = rdd.map(lambda num: (None, num))

    # Change RDD into key-value pairs (empty list).
    rdd_2 = general_map(rdd_1, lambda _, num: [])

    # Reduce RDD by combining values for the key.
    rdd_3 = general_reduce(rdd_2, lambda a, b: a + b)

    # Collect RDD and change to a set of (key, value) pairs.
    return set(rdd_3.collect())
"""
12. What happened? Explain below.
Does this depend on anything specific about how
we chose to define general_reduce?

=== ANSWER Q12 BELOW ===

For Q11 we get an empty set. Since all the keys are None there is no data tp
reduce so we get an empty set. This does not have to do with the general_reduce function but rather 
the output of the general_map function since it returned an empty output.

=== END OF Q12 ANSWER ===

13. Lastly, we will explore a second edge case, where the reduce stage can
output different values depending on the order of the input.
This leads to something called "nondeterminism", where the output of the
pipeline can even change between runs!

First, take a look at the definition of your general_reduce function.
Why do you imagine it could be the case that the output of the reduce stage
is different depending on the order of the input?

=== ANSWER Q13 BELOW ===

The function reduces by key and applies the function given to each item. If the function is non-commutative
then the order of the inputs can affect the way they are reduced.

=== END OF Q13 ANSWER ===

14.
Now demonstrate this edge case concretely by writing a specific example below.
As before, you should use the same dataset from Q4.

Important: Please create an example where the output of the reduce stage is a set of (integer, integer) pairs.
(So k2 and v2 are both integers.)
"""

def q14(rdd):
    # Input: the RDD from Q4
    # Output: the result of the pipeline, a set of (key, value) pairs

    # Map each number in the RDD to a key-value pair with key as 1.
    rdd_1 = rdd.map(lambda num: (1, num))

    # Reduce RDD by using subtraction on values with the same key.
    rdd_2 = general_reduce(rdd_1, lambda num, a: num - a)
    
    # Collect RDD and change to a set of (key, value) pairs.
    return set(rdd_2.collect())


"""
15.
Run your pipeline. What happens?
Does it exhibit nondeterministic behavior on different runs?
(It may or may not! This depends on the Spark scheduler and implementation,
including partitioning.

=== ANSWER Q15 BELOW ===
The pipeline gives the same answer on different runs. It does not seem to exhibit nondeterministic behavior
this could order it is being reduced in stays the same.
=== END OF Q15 ANSWER ===

16.
Lastly, try the same pipeline as in Q14
with at least 3 different levels of parallelism.

Write three functions, a, b, and c that use different levels of parallelism.
"""

def q16_a():
    # For this one, create the RDD yourself. Choose the number of partitions.
    # Same as question 14 function

    # Here we create an RDD with 2 partitions
    rdd = sc.parallelize(range(1, 1_000_001), 2)

    rdd_1 = rdd.map(lambda num: (1, num))
    rdd_2 = general_reduce(rdd_1, lambda num, a: num - a)
    
    return set(rdd_2.collect())


def q16_b():
    # For this one, create the RDD yourself. Choose the number of partitions.

    # Here we create an RDD with 4 partitions
    rdd = sc.parallelize(range(1, 1_000_001), 4)

    rdd_1 = rdd.map(lambda num: (1, num))
    rdd_2 = general_reduce(rdd_1, lambda num, a: num - a)
    
    return set(rdd_2.collect())

def q16_c():
    # For this one, create the RDD yourself. Choose the number of partitions.

    # Here we create an RDD with 8 partitions
    rdd = sc.parallelize(range(1, 1_000_001), 8)
    
    rdd_1 = rdd.map(lambda num: (1, num))
    rdd_2 = general_reduce(rdd_1, lambda num, a: num - a)
    
    return set(rdd_2.collect())

"""
Discussion questions

17. Was the answer different for the different levels of parallelism?

double throughput if everything is parallel 
Latency is running time of whole pipeline
part 3 will enable u to add an argument 
foo(x=None)
foo() foo(3)

=== ANSWER Q17 BELOW ===

Yes, the answer is different for the different levels of parallelism. The number of partitions affects how the results
are combined which causes the outputs to be different.

=== END OF Q17 ANSWER ===

18. Do you think this would be a serious problem if this occured on a real-world pipeline?
Explain why or why not.

=== ANSWER Q18 BELOW ===

I think this could be a potential problem because they will not give the same results every time depending on 
both the parallelism and the nondeterminism. 

=== END OF Q18 ANSWER ===

===== Q19-20: Further reading =====

19.
The following is a very nice paper
which explores this in more detail in the context of real-world MapReduce jobs.
https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/icsecomp14seip-seipid15-p.pdf

Take a look at the paper. What is one sentence you found interesting?

=== ANSWER Q19 BELOW ===

"The second surprising finding in our study is that most 292 non-commutative reducers can be categorized 
into five simple patterns based on the root cause of their non-commutativity, even though there is a 
wide variety of algorithms and coding styles among them."

=== END OF Q19 ANSWER ===

20.
Take one example from the paper, and try to implement it using our
general_map and general_reduce functions.
For this part, just return the answer "True" at the end if you found
it possible to implement the example, and "False" if it was not.
"""

def q20():
    # Create an RDD with key-value pairs.
    rdd = sc.parallelize([
        ('group1', (10, 'a')),
        ('group1', (15, 'b')),
        ('group1', (5, 'c'))
    ])

    # Map function to keep key-value pairs unchanged.
    def map_function(k1, v1):
        return [(k1, v1)]
    
    # Reduce function that gets the max.
    def reduce_function(v2_1, v2_2):
        return v2_1 if v2_1[0] > v2_2[0] else v2_2

    try:
        rdd_2 = general_map(rdd, map_function)

        rdd_3 = general_reduce(rdd_2, reduce_function)

        # Collect RDD as a list of key-value pairs.
        result = rdd_3.collect()

        expected_result = [('group1', (15, 'b'))]
        
        # Return True or False if MaxRow function worked
        return result == expected_result

    except Exception as e:
        print(f"An error occurred: {e}")
        return False
"""
That's it for Part 1!

===== Wrapping things up =====

**Don't modify this part.**

To wrap things up, we have collected
everything together in a pipeline for you below.

Check out the output in output/part1-answers.txt.
"""

ANSWER_FILE = "output/part1-answers.txt"
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

def PART_1_PIPELINE():
    open(ANSWER_FILE, 'w').close()

    try:
        dfs = load_input()
    except NotImplementedError:
        print("Welcome to Part 1! Implement load_input() to get started.")
        dfs = sc.parallelize([])

    # Questions 1-3
    log_answer("q1", q1)
    log_answer("q2", q2)
    # 3: commentary

    # Questions 4-10
    log_answer("q4", q4, dfs)
    log_answer("q5", q5, dfs)
    log_answer("q6", q6, dfs)
    log_answer("q7", q7, dfs)
    log_answer("q8a", q8_a)
    log_answer("q8b", q8_b)
    # 9: commentary
    # 10: commentary

    # Questions 11-18
    log_answer("q11", q11, dfs)
    # 12: commentary
    # 13: commentary
    log_answer("q14", q14, dfs)
    # 15: commentary
    log_answer("q16a", q16_a)
    log_answer("q16b", q16_b)
    log_answer("q16c", q16_c)
    # 17: commentary
    # 18: commentary

    # Questions 19-20
    # 19: commentary
    log_answer("q20", q20)

    # Answer: return the number of questions that are not implemented
    if UNFINISHED > 0:
        print("Warning: there are unfinished questions.")

    return f"{UNFINISHED} unfinished questions"

if __name__ == '__main__':
    log_answer("PART 1", PART_1_PIPELINE)

"""
=== END OF PART 1 ===
"""
