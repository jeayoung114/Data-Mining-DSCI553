
# My prediction: my Python code will run faster then my Spark code on a large dataset on Vocareum:
#
# Reason : The grading dataset is about 5GB and it is smaller than recent main memory size.
# When I run code on python, it will store all the items into the main memory and sort them
# However, when I run code with Spark, as the items are divided into multiple partitions, it has to do reshuffling process to sort the data.
# This will make Python Code faster than Spark code.

import time
import json
from pyspark import SparkContext
import sys

def main(review_filepath, business_filepath, output_filepath_question_a, output_filepath_question_b):


    sc = SparkContext('local[*]', 'task3')
    start_time = time.time()

    sample1RDD = sc.textFile(review_filepath)
    sample2RDD = sc.textFile(business_filepath)

    tmp = sample1RDD.map(lambda line : (json.loads(line)["business_id"],(json.loads(line)["stars"])))
    tmp2 = sample2RDD.map(lambda line : (json.loads(line)["business_id"],(json.loads(line)["city"])))

    joined = tmp.join(tmp2)

    joined = joined.map(lambda line :(line[1][1],(1,line[1][0])))

    def ftn1(first,second):
        return (first[0]+second[0], first[1]+second[1]) ##count, sum
    joined = joined.reduceByKey(ftn1)

    def ftn2(x): ##(avg_star, city)
        return (x[1][1]/x[1][0], x[0])

    question_a_tmp = joined.map(lambda x : ftn2(x)).groupByKey().sortByKey(False).mapValues(lambda v: sorted(v)).flatMap(lambda x : [(x[0], value) for value in x[1]]).collect()
    time_m2 = (time.time() - start_time)

    ### store result of a
    with open(output_filepath_question_a, 'w') as writefile:
        writefile.writelines("city,stars\n")
        for i in question_a_tmp:
            writefile.writelines(str(i[1]) + ',' + str(i[0]) + '\n')



    ######m2
    start_time = time.time()

    sample1RDD = sc.textFile(review_filepath)
    sample2RDD = sc.textFile(business_filepath)

    tmp = sample1RDD.map(lambda line: (json.loads(line)["business_id"], (json.loads(line)["stars"])))
    tmp2 = sample2RDD.map(lambda line: (json.loads(line)["business_id"], (json.loads(line)["city"])))

    joined = tmp.join(tmp2)
    joined = joined.map(lambda line: (line[1][1], (1, line[1][0])))

    def ftn1(first, second):
        return (first[0] + second[0], first[1] + second[1])

    joined = joined.reduceByKey(ftn1)

    def ftn2(x):
        return (x[1][1] / x[1][0], x[0])

    def ftn2(x):
        return (x[1][1] / x[1][0], x[0])

    tmp = joined.map(lambda x: ftn2(x)).collect()

    tmp = sorted(tmp, key=lambda x: (-x[0], x[1]))

    if len(tmp) >= 10:
        tmp = tmp[0:10]

    time_m1 = (time.time() - start_time)

    question_b = {}
    question_b["m1"] = time_m1
    question_b["m2"] = time_m2

    with open(output_filepath_question_b, 'w') as json_file:
        json.dump(question_b, json_file)



review_filepath = sys.argv[1]
business_filepath = sys.argv[2]
output_filepath_question_a = sys.argv[3]
output_filepath_question_b = sys.argv[4]

# main(review_filepath, output_file_path, n_partition)
main(review_filepath, business_filepath, output_filepath_question_a, output_filepath_question_b)
