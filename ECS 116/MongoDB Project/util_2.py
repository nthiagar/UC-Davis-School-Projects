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


def hello_world():
    print("Hello World")

def build_query_left_join_listings_reviewsm():
    query = """
    SELECT *
    FROM listings l
    LEFT JOIN reviewsm r
    ON l.id = r.listing_id;
    """
    return query

# Calculate the difference between times 
def time_diff(time1, time2):
    return (time2-time1).total_seconds()

