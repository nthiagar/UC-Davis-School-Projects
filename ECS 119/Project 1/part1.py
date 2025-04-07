"""
Part 1: Data Processing in Pandas

**Released: Monday, October 14**

=== Instructions ===

There are 22 questions in this part.
For each part you will implement a function (q1, q2, etc.)
Each function will take as input a data frame
or a list of data frames and return the answer
to the given question.

To run your code, you can run `python3 part1.py`.
This will run all the questions that you have implemented so far.
It will also save the answers to part1-answers.txt.

=== Dataset ===

In this part, we will use a dataset of world university rankings
called the "QS University Rankings".

The ranking data was taken 2019--2021 from the following website:
https://www.topuniversities.com/university-rankings/world-university-rankings/2021

=== Grading notes ===

- Once you have completed this part, make sure that
  your code runs, that part1-answers.txt is being re-generated
  every time the code is run, and that the answers look
  correct to you.

- Be careful about output types. For example if the question asks
  for a list of DataFrames, don't return a numpy array or a single
  DataFrame. When in doubt, ask on Piazza!

- Make sure that you remove any NotImplementedError exceptions;
  you won't get credit for any part that raises this exception
  (but you will still get credit for future parts that do not raise it
  if they don't depend on the previous parts).

- Make sure that you fill in answers for the parts
  marked "ANSWER ___ BELOW" and that you don't modify
  the lines above and below the answer space.

- Q6 has a few unit tests to help you check your work.
  Make sure that you removed the `@pytest.mark.skip` decorators
  and that all tests pass (show up in green, no red text!)
  when you run `pytest part3.py`.

- For plots: There are no specific requirements on which
  plotting methods you use; if not specified, use whichever
  plot you think might be most appropriate for the data
  at hand.
  Please ensure your plots are labeled and human-readable.
  For example, call .legend() on the plot before saving it!

===== Questions 1-6: Getting Started =====

To begin, let's load the Pandas library.

"""

import pandas as pd

#I collaborated with Ruba Thekkath and Meena Iyer for HW #1.


"""
1. Load the dataset into Pandas

Our first step is to load the data into a Pandas DataFrame.
We will also change the column names
to lowercase and reorder to get only the columns we are interested in.

Implement the rest of the function load_input()
by filling in the parts marked TODO below.

Return as your answer to q1 the number of dataframes loaded.
(This part is implemented for you.)
"""

NEW_COLUMNS = ['rank', 'university', 'region', 'academic reputation', 'employer reputation', 'faculty student', 'citations per faculty', 'overall score']

def load_input():
    """
    Input: None
    Return: a list of 3 dataframes, one for each year.
    """

    # Load the input files and return them as a list of 3 dataframes.
    df_2019 = pd.read_csv('data/2019.csv', encoding='latin-1')
    df_2020 = pd.read_csv('data/2020.csv', encoding='latin-1')
    df_2021 = pd.read_csv('data/2021.csv', encoding='latin-1')

    # Standardizing the column names
    df_2019.columns = df_2019.columns.str.lower()
    df_2020.columns = df_2019.columns.str.lower()
    df_2021.columns = df_2019.columns.str.lower()

    # Restructuring the column indexes
    # Fill out this part. You can use column access to get only the
    # columns we are interested in using the NEW_COLUMNS variable above.
    # Make sure you return the columns in the new order.
    df_2019 = df_2019[NEW_COLUMNS]
    df_2020 = df_2020[NEW_COLUMNS]
    df_2021 = df_2021[NEW_COLUMNS]

    return [df_2019, df_2020, df_2021]

def q1(dfs):
    # As the "answer" to this part, let's just return the number of dataframes.
    # Check that your answer shows up in part1-answers.txt.
    return len(dfs)

"""
2. Input validation

Let's do some basic sanity checks on the data for Q1.

Check that all three data frames have the same shape,
and the correct number of rows and columns in the correct order.

As your answer to q2, return True if all validation checks pass,
and False otherwise.
"""

def q2(dfs):
    """
    Input: Assume the input is provided by load_input()

    Return: True if all validation checks pass, False otherwise.

    Make sure you return a Boolean!
    From this part onward, we will not provide the return
    statement for you.
    You can check that the "answers" to each part look
    correct by inspecting the file part1-answers.txt.
    """
    # Check:
    # - that all three dataframes have the same shape
    # - the number of rows
    # - the number of columns
    # - the columns are listed in the correct order
    shape1=dfs[0].shape
    columns1=dfs[0].columns
    for i in dfs[1:]:
        if shape1 != i.shape or not columns1.equals(i.columns):
            return False
    return True
           



"""
===== Interlude: Checking your output so far =====

Run your code with `python3 part1.py` and open up the file
output/part1-answers.txt to see if the output looks correct so far!

You should check your answers in this file after each part.

You are welcome to also print out stuff to the console
in each question if you find it helpful.
"""

ANSWER_FILE = "output/part1-answers.txt"

def interlude():
    print("Answers so far:")
    with open(f"{ANSWER_FILE}") as fh:
        print(fh.read())

"""
===== End of interlude =====

3a. Input validation, continued

Now write a validate another property: that the set of university names
in each year is the same.
As in part 2, return a Boolean value.
(True if they are the same, and False otherwise)

Once you implement and run your code,
remember to check the output in part1-answers.txt.
(True if the checks pass, and False otherwise)
"""

def q3(dfs):
    # Check:
    # - that the set of university names in each year is the same
    # Return:
    # - True if they are the same, and False otherwise.
    university1=dfs[0]['university']
    for i in dfs[1:]:
        if not university1.equals(i['university']):
            return False
    return True

"""
3b (commentary).
Did the checks pass or fail?
Comment below and explain why.

=== ANSWER Q3b BELOW ===
The checks failed because the universties were ranked differently, so the order of the university names would be different.
For this reason, the check failed since they are not in the same order.

=== END OF Q3b ANSWER ===
"""

"""
4. Random sampling

Now that we have the input data validated, let's get a feel for
the dataset we are working with by taking a random sample of 5 rows
at a time.

Implement q4() below to sample 5 points from each year's data.

As your answer to this part, return the *university name*
of the 5 samples *for 2021 only* as a list.
(That is: ["University 1", "University 2", "University 3", "University 4", "University 5"])

Code design: You can use a for for loop to iterate over the dataframes.
If df is a DataFrame, df.sample(5) returns a random sample of 5 rows.

Hint:
    to get the university name:
    try .iloc on the sample, then ["university"].
"""

def q4(dfs):
    # Sample 5 rows from each dataframe
    # Print out the samples
    for i in dfs:
        print(i.sample(5))

    # Answer as a list of 5 university names
    return list(dfs[2].sample(5).iloc[:,1])

"""
Once you have implemented this part,
you can run a few times to see different samples.

4b (commentary).
Based on the data, write down at least 2 strengths
and 3 weaknesses of this dataset.

=== ANSWER Q4b BELOW ===
Strengths:
1. A strength for this data set is that you can see the overall ranking for each university with all of the individual
scores which is helpful to see what contributed to the overall ranking.
2. Another strength is that each university has the region so that we can group them together and look
at university data for each region.

Weaknesses:
1. A weakness of the data is that there is no other information about the university itself to possibly explain the 
scores that each university recieved.
2. Another weakness is that there is no explanation on how the scores were assigned and what they are out of. It 
is unclear how these universities achieved their rank.
3. One last weakness is that there is no column for the year so there is no way to compare over the years about how
the rankings changed unless you used multiple data sets, like we are doing.
=== END OF Q4b ANSWER ===
"""

"""
5. Data cleaning

Let's see where we stand in terms of null values.
We can do this in two different ways.

a. Use .info() to see the number of non-null values in each column
displayed in the console.

b. Write a version using .count() to return the number of
non-null values in each column as a dictionary.

In both 5a and 5b: return as your answer
*for the 2021 data only*
as a list of the number of non-null values in each column.

Example: if there are 5 non-null values in the first column, 3 in the second, 4 in the third, and so on, you would return
    [5, 3, 4, ...]
"""

def q5a(dfs):
    # Remember to return the list here
    # (Since .info() does not return any values,
    # for this part, you will need to copy and paste
    # the output as a hardcoded list.)

    print(dfs[2].info())
    return[100,100,100,100,100,100,100,100]

    
# Q5b: As in part a: return as your answer for the 2021 data only as a list of the number of non-null values in each column.

def q5b(dfs):
    # Remember to return the list here
    return list(dfs[2].count())

"""
5c.
One other thing:
Also fill this in with how many non-null values are expected.
We will use this in the unit tests below.
"""

def q5c():
    # fill this in with the expected number
    num_non_null = 100
    return num_non_null

"""
===== Interlude again: Unit tests =====

Unit tests

Now that we have completed a few parts,
let's talk about unit tests.
We won't be using them for the entire assignment
(in fact we won't be using them after this),
but they can be a good way to ensure your work is correct
without having to manually inspect the output.

We need to import pytest first.
"""

import pytest

"""
The following are a few unit tests for Q1-5.

To run the unit tests,
first, remove (or comment out) the `@pytest.mark.skip` decorator
from each unit test (function beginning with `test_`).
Then, run `pytest part1.py` in the terminal.
"""

#@pytest.mark.skip
def test_q1():
    dfs = load_input()
    assert len(dfs) == 3
    assert all([isinstance(df, pd.DataFrame) for df in dfs])

#@pytest.mark.skip
def test_q2():
    dfs = load_input()
    assert q2(dfs)

#@pytest.mark.skip
@pytest.mark.xfail
def test_q3():
    dfs = load_input()
    assert q3(dfs)

#@pytest.mark.skip
def test_q4():
    dfs = load_input()
    samples = q4(dfs)
    assert len(samples) == 5

#@pytest.mark.skip
def test_q5():
    dfs = load_input()
    answers = q5a(dfs) + q5b(dfs)
    assert len(answers) > 0
    num_non_null = q5c()
    for x in answers:
        assert x == num_non_null

"""
6a. Are there any tests which fail?

=== ANSWER Q6a BELOW ===
Yes, 1 test failed. The test that failed was for q3.

=== END OF Q6a ANSWER ===

6b. For each test that fails, is it because your code
is wrong or because the test is wrong?

=== ANSWER Q6b BELOW ===
For q3, the test failed because q3 produced a false boolean value and not a true value. This is because the 
order of the university names are different since they are different ranks. For this reason, the test is wrong,
it should test if the same names exists in each of the data sets, instead of checking by going in the same order.

=== END OF Q6b ANSWER ===

IMPORTANT: for any failing tests, if you think you have
not made any mistakes, mark it with
@pytest.mark.xfail
above the test to indicate that the test is expected to fail.
Run pytest part1.py again to see the new output.

6c. Return the number of tests that failed, even after your
code was fixed as the answer to this part.
(As an integer)
Please include expected failures (@pytest.mark.xfail).
(If you have no failing or xfail tests, return 0.)
"""

def q6c():
    return 1

"""
===== End of interlude =====

===== Questions 7-10: Data Processing =====

7. Adding columns

Notice that there is no 'year' column in any of the dataframe. As your first task, append an appropriate 'year' column in each dataframe.

Append a column 'year' in each dataframe. It must correspond to the year for which the data is represented.

As your answer to this part, return the number of columns in each dataframe after the addition.
"""

def q7(dfs):
    dfs[0]["Year"]=2019
    dfs[1]["Year"]=2020
    dfs[2]["Year"]=2021
    print(dfs[0].head())
    # Remember to return the list here
    return [dfs[0].shape[1],dfs[1].shape[1],dfs[2].shape[1]]

"""
8a.
Next, find the count of universities in each region that made it to the Top 100 each year. Print all of them.

As your answer, return the count for "USA" in 2021.
"""

def q8a(dfs):
    # Enter Code here
    for i in dfs:
        print(i.groupby(["region", "Year"])["university"].count())
    # Remember to return the count here
    return dfs[2][dfs[2]['region'] == 'USA']["university"].count()

"""
8b.
Do you notice some trend? Comment on what you observe and why might that be consistent throughout the years.

=== ANSWER Q8b BELOW ===
About the same number of universties for each region stay the same for each year. This makes sense since these regions
probably would not change to much for their universities so they would meet the same standards over only 3 years.
=== END OF Q8b ANSWER ===
"""

"""
9.
From the data of 2021, find the average score of all attributes for all universities.

As your answer, return the list of averages (for all attributes)
in the order they appear in the dataframe:
academic reputation, employer reputation, faculty student, citations per faculty, overall score.

The list should contain 5 elements.
"""

def q9(dfs):

    # Enter code here
    df_2021=dfs[2].drop(columns=['rank','university','region', 'Year'])
    avg_2021 = df_2021.mean()

    # Return the list here
    return list(avg_2021)

"""
10.
From the same data of 2021, now find the average of *each* region for **all** attributes **excluding** 'rank' and 'year'.

In the function q10_helper, store the results in a variable named **avg_2021**
and return it.

Then in q10, print the first 5 rows of the avg_2021 dataframe.
"""

def q10_helper(dfs):
    # Enter code here
    df_2021=dfs[2].drop(columns=['rank','university', 'Year'])
    avg_2021 = df_2021.groupby('region').mean()
    return avg_2021

def q10(avg_2021):
    """
    Input: the avg_2021 dataframe
    Print: the first 5 rows of the dataframe

    As your answer, simply return the number of rows printed.
    (That is, return the integer 5)
    """
    # Enter code here
    
    print(avg_2021.head())
    return avg_2021.head().shape[0]
    # Return 5

"""
===== Questions 11-14: Exploring the avg_2021 dataframe =====

11.
Sort the avg_2021 dataframe from the previous question based on overall score in a descending fashion (top to bottom).

As your answer to this part, return the first row of the sorted dataframe.
"""

def q11(avg_2021):
    sorted_avg_2021=avg_2021.sort_values(by=['overall score'], ascending=False)
    print(sorted_avg_2021)
    print("\n")
    return sorted_avg_2021.head(1)

"""
12a.
What do you observe from the table above? Which country tops the ranking?

What is one country that went down in the rankings
between 2019 and 2021?

You will need to load the data from 2019 to get the answer to this part.
You may choose to do this
by writing another function like q10_helper and running q11,
or you may just do it separately
(e.g., in a Python shell) and return the name of the university
that you found went down in the rankings.

Errata: please note that the 2021 dataset we provided is flawed
(it is almost identical to the 2020 data).
This is why the question now asks for the difference between 2019 and 2021.
Your answer to which university went down will not be graded.

For the answer to this part return the name of the country that tops the ranking and the name of one country that went down in the rankings.
"""
def q12_helper(dfs):
    df_2019=dfs[0].drop(columns=['rank','university', 'Year'])
    avg_2019 = df_2019.groupby('region').mean()
    return avg_2019

def q12a(avg_2021):
    sorted_avg_2021=avg_2021.sort_values(by=['overall score'], ascending=False)

    print("2021")
    print(sorted_avg_2021)
    return (sorted_avg_2021.head(1).index[0], "South Korea") # South Korea is one country that went down in the rankings from 2019 to 2021

"""
12b.
Comment on why the country above is at the top of the list.
(Note: This is an open-ended question.)

=== ANSWER Q12b BELOW ===
Singapore is at the top of the list since their average overall score is 91.65, this is the highest out of all the 
regions, so they are ranked at the top.
=== END OF Q12b ANSWER ===
"""

"""
13a.
Represent all the attributes in the avg_2021 dataframe using a box and whisker plot.

Store your plot in output/13a.png.

As the answer to this part, return the name of the plot you saved.

**Hint:** You can do this using subplots (and also otherwise)
"""

import matplotlib.pyplot as plt

def q13a(avg_2021):
    # Plot the box and whisker plot
    plt.figure(figsize=(10, 6))
    avg_2021.boxplot()
    plt.title('Box and Whisker Plot of avg_2021')
    plt.ylabel('Scores')
    output_file = 'output/13a.png'
    plt.savefig(output_file)
    plt.close()
    return "output/13a.png"

"""
b. Do you observe any anomalies in the box and whisker
plot?

=== ANSWER Q13b BELOW ===
There is an anomaly for the overall score box and whisker plot. Most scores were lower, so this was a high outlier.
There are pretty larges ranges for the rest of the categories so there don't seem to be any other extreme outleirs.
Other than that there does not seem to be any other anomalies.
=== END OF Q13b ANSWER ===
"""

"""
14a.
Pick two attributes in the avg_2021 dataframe
and represent them using a scatter plot.

Store your plot in output/14a.png.

As the answer to this part, return the name of the plot you saved.
"""

def q14a(avg_2021):
    # Enter code here
    plt.figure(figsize=(10, 6))
    avg_2021.plot.scatter(x="overall score", y="academic reputation")
    plt.title('Scatter plot of avg_2021')
    output_file = 'output/14a.png'
    plt.savefig(output_file)
    plt.close()
    return "output/14a.png"

"""
Do you observe any general trend?

=== ANSWER Q14b BELOW ===
I plotted the overall score versus the academic reputation. A trend I see is that as the overall score increases,
so does the academic reputation. This makes sense since if it is a better ranked university both of these scores should be higher.
=== END OF Q14b ANSWER ===

===== Questions 15-20: Exploring the data further =====

We're more than halfway through!

Let's come to the Top 10 Universities and observe how they performed over the years.

15. Create a smaller dataframe which has the top ten universities from each year, and only their overall scores across the three years.

Hint:

*   There will be four columns in the dataframe you make
*   The top ten universities are same across the three years. Only their rankings differ.
*   Use the merge function. You can read more about how to use it in the documentation: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.merge.html
*   Shape of the resultant dataframe should be (10, 4)

As your answer, return the shape of the new dataframe.
"""


def q15_helper(dfs):
    # Return the new dataframe
    top_2019 = dfs[0].head(10)[['university', 'overall score']]
    top_2020 = dfs[1].head(10)[['university', 'overall score']]
    top_2021 = dfs[2].head(10)[['university', 'overall score']]
    
    top_10 = top_2019.merge(top_2020, how="left", on='university').merge(top_2021, how="left", on='university')
    print(top_10)
    return top_10

def q15(top_10):
    # Enter code here
    return top_10.shape

"""
16.
You should have noticed that when you merged,
Pandas auto-assigned the column names. Let's change them.

For the columns representing scores, rename them such that they describe the data that the column holds.

You should be able to modify the column names directly in the dataframe.
As your answer, return the new column names.
"""

def q16(top_10):
    # Enter code here
    top_10.rename(columns= {'overall score_x': '2019 overall score', 'overall score_y': '2020 overall score','overall score': '2021 overall score'}, inplace=True)
    print(top_10)
    return list(top_10.columns)
   
"""
17a.
Draw a suitable plot to show how the overall scores of the Top 10 universities varied over the three years. Clearly label your graph and attach a legend. Explain why you chose the particular plot.

Save your plot in output/16.png.

As the answer to this part, return the name of the plot you saved.

Note:
*   All universities must be in the same plot.
*   Your graph should be clear and legend should be placed suitably
"""

def q17a(top_10):
    plt.figure(figsize=(10, 6))

    for i, row in top_10.iterrows():
        plt.plot(['2019', '2020', '2021'], 
                 [row['2019 overall score'], row['2020 overall score'], row['2021 overall score']], marker='o',  label=row['university'])

    plt.title('Overall Scores of Top 10 Universities (2019-2021)')
    plt.xlabel('Year')
    plt.ylabel('Overall Score')

    plt.legend(loc='center left', bbox_to_anchor=(1, 0.5))

    # Save the plot
    plt.tight_layout()
    plt.savefig('output/17a.png')
    plt.close()

    
    return "output/17a.png" # 2020 does not show because it has the same values as 2021 data for top 10


"""
17b.
What do you observe from the plot above? Which university has remained consistent in their scores? Which have increased/decreased over the years?

=== ANSWER Q17a BELOW ===
From the plot, it seems that the rankings for the top ten universities have been pretty consistent among the 3 years.
MIT has been the most consistent, their score looks to be 100 for each year. Harvard and University of Chicago have seemed
to decrease the most over the years. UCL's overall score increased the most after 2020. The other university's scores didn't
fluctuate that much.
=== END OF Q17b ANSWER ===
"""

"""
===== Questions 18-19: Correlation matrices =====

We're almost done!

Let's look at another useful tool to get an idea about how different variables are corelated to each other. We call it a **correlation matrix**

A correlation matrix provides a correlation coefficient (a number between -1 and 1) that tells how strongly two variables are correlated. Values closer to -1 mean strong negative correlation whereas values closer to 1 mean strong positve correlation. Values closer to 0 show variables having no or little correlation.

You can learn more about correlation matrices from here: https://www.statology.org/how-to-read-a-correlation-matrix/

18.
Plot a correlation matrix to see how each variable is correlated to another. You can use the data from 2021.

Print your correlation matrix and save it in output/18.png.

As the answer to this part, return the name of the plot you saved.

**Helpful link:** https://datatofish.com/correlation-matrix-pandas/
"""
import seaborn as sns

def q18(dfs):
    # Enter code here
    sns.heatmap(dfs[2].iloc[:, 3:8].corr(), annot=True)
    plt.tight_layout()
    output_file = 'output/18.png'
    plt.savefig(output_file)
    plt.close()
    return "output/18.png"

"""
19. Comment on at least one entry in the matrix you obtained in the previous
part that you found surprising or interesting.

=== ANSWER Q19 BELOW ===
Something that was intersting were how highly the academic reputation and citations per faculty is.
I wondering if the repuatation of the faculty has a lot to do with the academic reputation.

=== END OF Q19 ANSWER ===
"""
"""
===== Questions 20-23: Data manipulation and falsification =====

This is the last section.

20. Exploring data manipulation and falsification

For fun, this part will ask you to come up with a way to alter the
rankings such that your university of choice comes out in 1st place.

The data does not contain UC Davis, so let's pick a different university.
UC Berkeley is a public university nearby and in the same university system,
so let's pick that one.

We will write two functions.
a.
First, write a function that calculates a new column
(that is you should define and insert a new column to the dataframe whose value
depends on the other columns)
and calculates
it in such a way that Berkeley will come out on top in the 2021 rankings.

Note: you can "cheat"; it's OK if your scoring function is picked in some way
that obviously makes Berkeley come on top.
As an extra challenge to make it more interesting, you can try to come up with
a scoring function that is subtle!

b.
Use your new column to sort the data by the new values and return the top 10 universities.
"""

def q20a(dfs):
    dfs[2]["New Ranking"] = dfs[2]["overall score"] * .05 + dfs[2]["academic reputation"]*1.25
    dfs[2].loc[dfs[2]["university"] == "University of California, Berkeley (UCB)", "New Ranking"] = 130
    return dfs[2].loc[dfs[2]['university'] == "University of California, Berkeley (UCB)", "New Ranking"].values[0]

    # For your answer, return the score for Berkeley in the new column.

def q20b(dfs):
    sorted_UCB_2021=dfs[2].sort_values(by=['New Ranking'], ascending=False)
    sorted_UCB_2021_head = sorted_UCB_2021.head(10)
    return sorted_UCB_2021_head["university"].tolist()
    # For your answer, return the top 10 universities.

"""
21. Exploring data manipulation and falsification, continued

This time, let's manipulate the data by changing the source files
instead.
Create a copy of data/2021.csv and name it
data/2021_falsified.csv.
Modify the data in such a way that UC Berkeley comes out on top.

For this part, you will also need to load in the new data
as part of the function.
The function does not take an input; you should get it from the file.

Return the top 10 universities from the falsified data.
"""

def q21():
    df2021_falsified = pd.read_csv('data/2021_falsified.csv', encoding='latin-1')
    df2021_falsified.columns = df2021_falsified.columns.str.lower()
    df2021_falsified = df2021_falsified[NEW_COLUMNS]
    df2021_falsified=df2021_falsified.sort_values(by=["rank"]).head(10)
    return df2021_falsified["university"].tolist()

"""
22. Exploring data manipulation and falsification, continued

Which of the methods above do you think would be the most effective
if you were a "bad actor" trying to manipulate the rankings?

Which do you think would be the most difficult to detect?

=== ANSWER Q22 BELOW ===
I think the most effective way would be to create a new file, to manipulate the rankings. This way it does not
change the entire scoring system and you can easliy put your name at the top so that you're the best actor! I think what would
be hardest to detect is creating a new file also, because if you change the scoring system it might be pretty obvious. However,
if you just put your name at the top, it should be more subtle.

=== END OF Q22 ANSWER ===
"""

"""
===== Wrapping things up =====

To wrap things up, we have collected
everything together in a pipeline for you
below.

**Don't modify this part.**
It will put everything together,
run your pipeline and save all of your answers.

This is run in the main function
and will be used in the first part of Part 2.
"""

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
        dfs = []

    # Questions 1-6
    log_answer("q1", q1, dfs)
    log_answer("q2", q2, dfs)
    log_answer("q3a", q3, dfs)
    # 3b: commentary
    log_answer("q4", q4, dfs)
    # 4b: commentary
    log_answer("q5a", q5a, dfs)
    log_answer("q5b", q5b, dfs)
    log_answer("q5c", q5c)
    # 6a: commentary
    # 6b: commentary
    log_answer("q6c", q6c)

    # Questions 7-10
    log_answer("q7", q7, dfs)
    log_answer("q8a", q8a, dfs)
    # 8b: commentary
    log_answer("q9", q9, dfs)
    # 10: avg_2021
    avg_2021 = q10_helper(dfs)
    log_answer("q10", q10, avg_2021)

    # Questions 11-15
    avg_2019 = q12_helper(dfs)
    print("\n")
    print("2019 sorted average")
    q11(avg_2019)
    print("\n")
    log_answer("q11", q11, avg_2021)
    log_answer("q12", q12a, avg_2021)
    # 12b: commentary
    log_answer("q13", q13a, avg_2021)
    # 13b: commentary
    log_answer("q14a", q14a, avg_2021)
    # 14b: commentary

    # Questions 15-17
    top_10 = q15_helper(dfs)
    log_answer("q15", q15, top_10)
    log_answer("q16", q16, top_10)
    log_answer("q17", q17a, top_10)
    # 17b: commentary

    # Questions 18-20
    log_answer("q18", q18, dfs)
    # 19: commentary

    # Questions 20-22
    log_answer("q20a", q20a, dfs)
    log_answer("q20b", q20b, dfs)
    log_answer("q21", q21)
    # 22: commentary

    # Answer: return the number of questions that are not implemented
    if UNFINISHED > 0:
        print("Warning: there are unfinished questions.")

    return UNFINISHED

"""
That's it for Part 1!

=== END OF PART 1 ===

Main function
"""

if __name__ == '__main__':
    log_answer("PART 1", PART_1_PIPELINE)
