import sys
import json
import csv
import yaml

import datetime;
 
import copy

import os


import pandas as pd
import numpy as np
import math


import matplotlib as mpl

import time
from datetime import datetime

import itertools

import pprint

import psycopg2
from sqlalchemy import create_engine, text as sql_text


# ==============================

# Calculate the difference between times 
def time_diff(time1, time2):
    return (time2-time1).total_seconds()

# Calculate and return a dict with metrics for each year query 
def calc_time_diff_per_year(db_eng, count, q_dict):
    perf_details = {}

    # Iterate through all the queries in q_dict
    for query_name, sql_query in q_dict.items():
        time_list = []
        for i in range(count): 
            time_start = datetime.now()

            with db_eng.connect() as conn:
                df = pd.read_sql(sql_query, con=conn)

            time_end = datetime.now()
            # Calculate the time difference
            diff = time_diff(time_start, time_end)
            time_list.append(diff)
            
        # Calculate the metrics
        perf_profile = {
            'avg': round(sum(time_list) / len(time_list), 4),
            'min': round(min(time_list), 4),
            'max': round(max(time_list), 4),
            'std': round(np.std(time_list), 4),
            'exc_count': count,
            'timestamp': datetime.now().strftime('%Y-%m-%d-%H:%M:%S')
        }
        
        # Add metrics according to the query name
        perf_details[query_name] = perf_profile
    
    return perf_details


#fetches the json
def fetch_perf_data(filename):
    with open('/Users/Nfaith21/Documents/ECS 116 - Misc/' + filename, 'r') as f:
        data = json.load(f)
        sorted_data = dict(sorted(data.items()))
    return sorted_data

# writes the dictionary in dict as a json file into filename
def write_perf_data(dict1, filename):
    with open('/Users/Nfaith21/Documents/ECS 116 - Misc/' + filename, 'w') as fp:
        json.dump(dict1, fp)

# adds/drops index based on parameters
def add_drop_index(db_eng, action, column, table):

    q_show_indexes_for_reviews = f'''
    SELECT *
    FROM pg_indexes
    WHERE tablename = '{table}';
    '''
    
    if action == 'add':
        query = f'''
        BEGIN TRANSACTION;
        CREATE INDEX IF NOT EXISTS {column}_in_{table}
        ON {table}({column});
        END TRANSACTION;
        '''
    else:
        query = f'''
        BEGIN TRANSACTION;
        DROP INDEX IF EXISTS {column}_in_{table};
        END TRANSACTION;
        '''
    
    with db_eng.connect() as conn:
        conn.execute(sql_text(query))
        
        result = conn.execute(sql_text(q_show_indexes_for_reviews))
        return result.all()

# builds index description key based on the indexes given
def build_index_description_key(all_indexes, spec):
    sort = sorted(spec, key=lambda x: all_indexes.index(x))
    indexes = [f"{column}_in_{table}" for column, table in sort]
    return '__' + '__'.join(indexes) + '__'


# step 3a functions

def build_query_listings_join_reviews_datetime(date1, date2):
    q31 = """SELECT DISTINCT l.id, l.name
FROM listings l, reviews r 
WHERE l.id = r.listing_id
  AND r.datetime >= '"""
    q32 = """'
  AND r.datetime <= '"""
    q33 = """'
ORDER BY l.id;"""
    return q31 + date1 + q32 + date2 + q33   


# step 3b functions

def build_text_search_query(query_type, date1, date2, word):
    if query_type == 'tsv':
        q_11 = """select count(*)
    from reviews r
    where comments_tsv @@ to_tsquery('"""
        q_12 = """')
    and datetime >= '"""
        q_13 = """'
    and datetime <= '"""
        q_14 = """';"""
        query = q_11+word+q_12+date1+q_13+date2+q_14
    if query_type == 'like':
        q_21 = """select count(*)
        from reviews r
        where comments ILIKE '%%"""
        q_22 = """%%'
        and datetime >= '"""
        q_23 = """'
        and datetime <= '"""
        q_24 = """';"""
        query = q_21+word+q_22+date1+q_23+date2+q_24
    return query


# Runs function multiple times and gets the performance metrics
def get_perf_metrics(db_eng, query,count):
    time_list = []
    for i in range(0,count):
        time_start = datetime.now()
        # Open new db connection for each execution of the query to avoid multithreading
        with db_eng.connect() as conn:
            df = pd.read_sql(query, con=conn)
        time_end = datetime.now()
        diff = time_diff(time_start, time_end)
        time_list.append(diff)
        
    perf_profile = {
        'avg': round(sum(time_list) / len(time_list), 4),
        'min': round(min(time_list), 4),
        'max': round(max(time_list), 4),
        'std': round(np.std(time_list), 4),
        'exec_count': count,
        'timestamp': datetime.now().strftime('%Y-%m-%d-%H:%M:%S')
    }
    return perf_profile


# Gets performance metrics for specified year, word, and count
# Runs four combinations (two different types of queries, and then without + with the datetime_in_reviews index)
def text_search(db_eng, date1, date2, word, count):
    results = {}
    q_like = build_text_search_query('like',date1,date2,word)
    q_tsv = build_text_search_query('tsv',date1,date2,word)
    
    # Drop index
    mod_index = add_drop_index(db_eng, 'drop', 'datetime', 'reviews')
    
    # LIKE
    perf_profile = get_perf_metrics(db_eng, q_like,count)
    results['__'] = perf_profile
    
    # TSV
    perf_profile = get_perf_metrics(db_eng, q_tsv,count)
    results['__comments_tsv_in_reviews__'] = perf_profile
    
    # Add index
    mod_index = add_drop_index(db_eng, 'add', 'datetime', 'reviews')
    
    # LIKE and Datetime
    perf_profile = get_perf_metrics(db_eng, q_like,count)
    results['__datetime_in_reviews__'] = perf_profile
    
    # TSV and Datetime
    perf_profile = get_perf_metrics(db_eng, q_tsv,count)
    results['__datetime_in_reviews__comments_tsv_in_reviews__'] = perf_profile
    
    return results


# step 3c functions

def build_query_add_index(column, table):
    query = f"""
BEGIN TRANSACTION;
CREATE INDEX IF NOT EXISTS {column}_in_{table}
ON {table}({column});
END TRANSACTION;
"""
    return query


def build_query_drop_index(column, table):
    query = f"""
BEGIN TRANSACTION;
DROP INDEX IF EXISTS {column}_in_{table};
END TRANSACTION;
"""
    return query


def build_query_reviews_datetime_update(interval, operator, neighbourhood_type, neighborhood_val):
    if neighbourhood_type == 'group':
        nbhood_or_group = 'neighbourhood_group'
    else:
        nbhood_or_group = 'neighbourhood'

    query = f"""
UPDATE reviews r
SET datetime = datetime {operator} INTERVAL '{interval} days'
FROM listings l
WHERE l.id = r.listing_id
AND l.{nbhood_or_group} = '{neighborhood_val}'
RETURNING 'done';
"""
    return query


# Add or drop index
def add_drop_index_partc(db_eng, add_drop, column, table):
    with db_eng.connect() as conn:
        if add_drop == 'add':
            conn.execute(sql_text(build_query_add_index(column, table)))
        elif add_drop == 'drop':
            conn.execute(sql_text(build_query_drop_index(column, table)))
    return


# build index description key
def build_index_description_key(all_indexes, spec):
    key = '__'
    for index in sorted(all_indexes, key=lambda x: x[1]):
        if (index in spec) or (index == spec):
            if index[0] in ['neighbourhood', 'neighbourhood_group']:
                col = 'neigh'
            else:
                col = index[0]
            table = index[1]
            key += f'{col}_in_{table}__'
    return key


def calc_perf_per_update_datetime_query(db_eng, count, query):
    exec_time = datetime.now()
    time_list = []

    add_query = query[0]
    sub_query = query[1]

    for _ in range(math.ceil(count/2)):
        # add query
        time_start = datetime.now()

        with db_eng.connect() as conn:
            conn.execute(sql_text(add_query))

        time_end = datetime.now()

        # calculate the time difference
        diff = time_diff(time_start, time_end)
        time_list.append(diff)

        # sub query
        time_start = datetime.now()

        with db_eng.connect() as conn:
            conn.execute(sql_text(sub_query))

        time_end = datetime.now()

        # calculate the time difference
        diff = time_diff(time_start, time_end)
        time_list.append(diff)

    # calculate the metrics
    perf_profile = {
        'avg': round(sum(time_list) / len(time_list), 4),
        'min': round(min(time_list), 4),
        'max': round(max(time_list), 4),
        'std': round(np.std(time_list), 4),
        'count': count,
        'timestamp': exec_time.strftime("%Y-%m-%d-%H:%M:%S")
    }
    return perf_profile


def run_update_datetime_query_neighbourhoods(db_eng, count, q_dict, all_indexes, filename):
    # get all combinations of indexes
    specs = []
    for i in range(0, len(all_indexes)+1):
        specs += (list(itertools.combinations(all_indexes, i)))

    # run the tests
    # fetch the current performance data, if any
    if os.path.exists('/Users/Nfaith21/Documents/ECS 116 - Misc/' + filename):
        perf_summary = fetch_perf_data_partc(filename)
    else:
        perf_summary = {}

    # run each query with each index spec
    for query in q_dict:
        if query in perf_summary:
            perf_dict = perf_summary[query]
        else:
            perf_dict = {}

        for spec in specs:
            print(f'\nRunning test for query "{query}" with indexes {spec}')
            # remove any current indexes not in spec
            for index in all_indexes:
                if (index not in spec) or (index != spec):
                    add_drop_index_partc(db_eng, 'drop', index[0], index[1])

            # add any new indexes in spec
            if spec:
                for index in spec:
                    add_drop_index_partc(db_eng, 'add', index[0], index[1])

            # build the key for the index description
            key = build_index_description_key(all_indexes, spec)

            # run the test
            perf_dict[key] = calc_perf_per_update_datetime_query(
                db_eng, count, q_dict[query])

        # add the results to perf_summary
        perf_summary[query] = perf_dict

    # write the results to the file
    write_perf_data_partc(perf_summary, filename)

    return

# Fetches the json file for step 3 part c
def fetch_perf_data_partc(filename):
    f = open('/Users/Nfaith21/Documents/ECS 116 - Misc/' + filename)
    return json.load(f)


# writes the dictionary in dict as a json file into filename for step 3 part c
def write_perf_data_partc(dict1, filename):
    dict2 = dict(sorted(dict1.items()))
    with open('/Users/Nfaith21/Documents/ECS 116 - Misc/' + filename, 'w') as fp:
        json.dump(dict2, fp, indent=4)


