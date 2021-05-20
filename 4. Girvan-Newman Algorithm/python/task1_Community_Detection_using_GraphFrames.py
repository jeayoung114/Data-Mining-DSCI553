from functools import reduce
from pyspark.sql.functions import col, lit, when
from pyspark.sql import SQLContext
from graphframes import *


import numpy as np
import pandas as pd
import time
import math
import datetime
import sys
from pyspark import SparkContext
from sklearn import preprocessing
from itertools import combinations
import warnings
import os
warnings.filterwarnings(action='ignore')

os.environ["PYSPARK_SUBMIT_ARGS"] = ("--packages graphframes:graphframes:0.6.0-spark2.3-s_2.11")

if __name__ == '__main__':

    filter_threshold = int(sys.argv[1])
    input_file = sys.argv[2]
    output_file = sys.argv[3]

    t = time.time()
    n = 30

    sc = SparkContext('local[*]', 'task1')
    sc.setLogLevel('ERROR')

    ## RDDs
    textRDD = sc.textFile(input_file, n)
    header = textRDD.first()  ## remove header
    tmp = textRDD.filter(lambda x: x != header)

    ### users who reviewed equal or greater than 7 businesses
    baskets = tmp.map(lambda x: x.split(",")) \
        .map(lambda x: (x[0], [x[1]])) \
        .reduceByKey(lambda a, b: a + b) \
        .filter(lambda x: len(x[1]) >= filter_threshold)

    ## user list
    user_list = baskets.map(lambda x: (1, [x[0]])) \
        .reduceByKey(lambda a, b: a + b) \
        .collect()[0][1]

    ## user : [bus1, bus2, ...]
    user_basket_dict = {}
    for (user, business_list) in baskets.collect():
        user_basket_dict[user] = business_list

    ## candidate edge i.e. candidate pair of users
    candi_edge_list = list(combinations(user_list, 2))

    ## edge list i.e. (user1, user2)
    edge_list = sc.parallelize(candi_edge_list, n) \
        .map(lambda x: (x, len(set(user_basket_dict[x[0]]).intersection(set(user_basket_dict[x[1]]))))).filter(
        lambda x: x[1] >= filter_threshold).cache()

    ## user list of users who are in edge
    user_in_edge_list = list(
        set(edge_list.map(lambda x: (1, list(x[0]))).reduceByKey(lambda a, b: a + b).collect()[0][1]))

    ## define node, edge
    sqlContext = SQLContext(sc)
    vertices = sqlContext.createDataFrame([(user,) for user in user_in_edge_list], ["id"])
    edges = sqlContext.createDataFrame([tup[0] for tup in edge_list.collect()] \
                                       , ["src", "dst"])

    g = GraphFrame(vertices, edges)

    ## Label Propagation Algorithm
    result = g.labelPropagation(maxIter=5)

    import pyspark.sql.functions as fc
    from pyspark.sql.functions import size

    result = result.groupBy("label").agg(fc.sort_array(fc.collect_list("id")).alias("collected"))
    result = result.withColumn("len_collected", size("collected"))
    result = result.orderBy(col("len_collected"), col("collected"))

    answer = result.toPandas()["collected"]

    f = open(output_file, "w")
    for community in answer:
        line = ""
        for node in community:
            line+="'"+str(node) + "', "
        line = line[:-2]
        f.write(line)
        f.write("\n")
    f.close()

    print("Duration : ", time.time() - t)
