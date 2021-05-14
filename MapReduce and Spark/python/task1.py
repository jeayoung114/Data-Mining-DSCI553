# input_file = "test_review.json"
#input_file = "ccc_72aa618f64_45625@runweb11:/mnt/data2/students/sub1/ccc_72aa618f64_45625/asn325078_1/asn325119_1/resource/asnlib/publicdata/business.json"
from pyspark import SparkContext
import json
import sys



# def main(review_filepath, output_file_path, n_partition):
def main(review_filepath, output_file_path):
    sc = SparkContext('local[*]', 'task1')
    # textRDD = sc.textFile(review_filepath, int(n_partition))
    textRDD = sc.textFile(review_filepath)
    ## 1a

    tmp = textRDD.map(lambda line : (1,1)).reduceByKey(lambda a, b : a+b).collect()
    n_review = tmp[0][1]

    ## 1b

    def ftn1(line):
        year=int(json.loads(line)["date"][0:4])
        if year == 2018:
            return 1
        else:
            return 0
    tmp = textRDD.map(lambda line : (1,ftn1(line))).reduceByKey(lambda a, b : a+b).collect()
    n_review_2018 = tmp[0][1]

    ## 1c

    tmp = textRDD.map(lambda line : (json.loads(line)["user_id"],1)).reduceByKey(lambda a, b : 1).map(lambda x:(1,1)).reduceByKey(lambda a,b : a+b).collect()
    n_user = tmp[0][1]

    ## 1d
    tmp = textRDD.map(lambda line: (json.loads(line)["user_id"], 1)).reduceByKey(lambda a, b: a + b) \
        .map(lambda x: (x[1], x[0])).groupByKey().sortByKey(False).mapValues(lambda v: sorted(v)).flatMap(
        lambda x: [(x[0], value) for value in x[1]]).take(10)

    tmp_dict = {}
    for i in tmp:
        tmp_dict[i[1]]=i[0]

    tmp_dict = sorted(tmp_dict.items(), key=lambda x: (-x[1], x[0]))
    top10_user = []
    for i in tmp_dict:
        top10_user.append([i[0],i[1]])

    ## 1e

    tmp = textRDD.map(lambda line : (json.loads(line)["business_id"],1)).reduceByKey(lambda a,b : a)\
    .map(lambda x : (1,1)).reduceByKey(lambda a,b : a+b).collect()
    n_business = tmp[0][1]

    ## 1f

    tmp = textRDD.map(lambda line: (json.loads(line)["business_id"], 1)).reduceByKey(lambda a, b: a + b) \
        .map(lambda x: (x[1], x[0])).groupByKey().sortByKey(False).mapValues(lambda v: sorted(v)).flatMap(
        lambda x: [(x[0], value) for value in x[1]]).take(10)

    tmp_dict = {}
    for i in tmp:
        tmp_dict[i[1]]=i[0]

    tmp_dict = sorted(tmp_dict.items(), key=lambda x: (-x[1], x[0]))

    top10_business = []
    for i in tmp_dict:
        top10_business.append([i[0],i[1]])



    ## Final result

    answer_json = {}
    answer_json["n_review"] = n_review
    answer_json["n_review_2018"] = n_review_2018
    answer_json["n_user"] = n_user
    answer_json["top10_user"] = top10_user
    answer_json["n_business"] = n_business
    answer_json["top10_business"] = top10_business

    with open(output_file_path, 'w') as json_file:
        json.dump(answer_json, json_file)

review_filepath = sys.argv[1]
output_file_path = sys.argv[2]
# n_partition = sys.argv[3]

# main(review_filepath, output_file_path, n_partition)
main(review_filepath, output_file_path)
