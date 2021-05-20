import json
import time
from pyspark import SparkContext
import sys


def main(review_filepath, output_file_path, n_partition):
    sc = SparkContext('local[*]', 'task2')



    ### F
    start_time = time.time()
    textRDD = sc.textFile(review_filepath)

    tmp = textRDD.map(lambda line : (json.loads(line)["business_id"],1)).reduceByKey(lambda a,b : a+b)\
    .map(lambda x : (x[1],x[0])).groupByKey().sortByKey(False).mapValues(lambda v: sorted(v)).flatMap(lambda x : [(x[0], value) for value in x[1]])

    tmp.take(10)
    exe_time_1 = (time.time() - start_time)
    n_partition_1 = tmp.getNumPartitions()
    n_items_1 = tmp.mapPartitions(lambda x: [sum(1 for i in x)]).collect() ## number of items in each partition

    ### Custom
    start_time = time.time()
    def custom_partitioner(key):
        return ord(key[:1])

    textRDD = sc.textFile(review_filepath)
    tmp = textRDD.map(lambda line : (json.loads(line)["business_id"],1)).partitionBy(n_partition, custom_partitioner).reduceByKey(lambda a,b : a+b)\
    .map(lambda x : (x[1],x[0])).groupByKey().sortByKey(False).mapValues(lambda v: sorted(v)).flatMap(lambda x : [(x[0], value) for value in x[1]])

    tmp.take(10)

    exe_time_2 = (time.time() - start_time)
    n_partition_2 = tmp.getNumPartitions()
    n_items_2 = tmp.mapPartitions(lambda x: [sum(1 for i in x)]).collect() ## number of items in each partition


    answer_json = {}
    default_json = {}
    default_json["n_partition"] = n_partition_1
    default_json["n_items"] = n_items_1
    default_json["exe_time"] = exe_time_1
    customized_json = {}
    customized_json["n_partition"] = n_partition_2
    customized_json["n_items"] = n_items_2
    customized_json["exe_time"] = exe_time_2
    answer_json["default"] = default_json
    answer_json["customized"] = customized_json


    with open(output_file_path, 'w') as json_file:
        json.dump(answer_json, json_file)





review_filepath = sys.argv[1]
output_file_path = sys.argv[2]
n_partition = sys.argv[3]
n_partition = int(n_partition)
main(review_filepath, output_file_path, n_partition)