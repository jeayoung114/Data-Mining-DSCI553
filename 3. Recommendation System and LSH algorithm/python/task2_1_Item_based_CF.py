import time
import math
import csv
import sys

from pyspark import SparkContext


def dict_merge(a, b):
    a.update(b)
    return a


def similarity(business1, business2):
    a = business_basket_dict[business1]
    b = business_basket_dict[business2]

    if len(a) == 0 or len(b) == 0:
        return 0
    else:
        avg_a = sum(a.values()) / len(a)
        avg_b = sum(b.values()) / len(b)

        div_a = sum([(i - avg_a) ** 2 for i in a.values()]) ** 0.5
        div_b = sum([(i - avg_b) ** 2 for i in b.values()]) ** 0.5

        num = 0
        for i in set(a.keys()).intersection(set(b.keys())):
            num += (a[i] - avg_a) * (b[i] - avg_b)

        if div_a == 0 or div_b == 0:
            return 0
        else:
            return num / (div_a * div_b) + 0.1  ## to remove negative similarities


def score(x, user):  ## neighborhood 2
    num = 0
    div = 0
    x = sorted(x, key=lambda a: -a[0])
    count = 0
    while count < 3:
        count += 1
        for a, b in x:  ## w, r
            if a != 0:
                num += a * b
                div += abs(a)
    if div == 0:
        a = user_basket_dict[1].values()
        if len(a) != 0:
            return sum(a) / len(a)
        else:
            return 2.5
    else:
        return num / div


if __name__ == '__main__':

    input_file = sys.argv[1]
    test_file_name = sys.argv[2]
    output_file = sys.argv[3]

    sc = SparkContext('local[*]', 'task1')
    sc.setLogLevel("ERROR")

    t = time.time()
    # input_file = "yelp_train.csv"
    # test_file_name = "yelp_val_in.csv"
    # output_file = "task2.csv"
    n = 30

    ## RDDs
    textRDD = sc.textFile(input_file, n)
    testRDD = sc.textFile(test_file_name, n)

    ## exclue header
    header = textRDD.first()  ## remove header
    header2 = testRDD.first()  ## remove header
    tmp_train = textRDD.filter(lambda x: x != header)
    tmp_test = testRDD.filter(lambda x: x != header2)

    ##make dictionary for user and business
    user_list_train = \
    tmp_train.map(lambda x: (x.split(",")[0], 1)).reduceByKey(lambda a, b: a).map(lambda x: (1, [x[0]])).reduceByKey(
        lambda a, b: a + b).collect()[0][1]
    user_list_test = \
    tmp_test.map(lambda x: (x.split(",")[0], 1)).reduceByKey(lambda a, b: a).map(lambda x: (1, [x[0]])).reduceByKey(
        lambda a, b: a + b).collect()[0][1]
    user_list = list(set(user_list_train + user_list_test))
    user_to_idx = {}
    for idx, user in enumerate(user_list):
        user_to_idx[user] = idx

    business_list_train = \
    tmp_train.map(lambda x: (x.split(",")[1], 1)).reduceByKey(lambda a, b: a).map(lambda x: (1, [x[0]])).reduceByKey(
        lambda a, b: a + b).collect()[0][1]
    business_list_test = \
    tmp_test.map(lambda x: (x.split(",")[1], 1)).reduceByKey(lambda a, b: a).map(lambda x: (1, [x[0]])).reduceByKey(
        lambda a, b: a + b).collect()[0][1]
    business_list = list(set(business_list_train + business_list_test))
    business_to_idx = {}
    for idx, business in enumerate(business_list):
        business_to_idx[business] = idx


    ## user basket

    user_basket_dict = \
    tmp_train.map(lambda x: (user_to_idx[x.split(",")[0]], {business_to_idx[x.split(",")[1]]: float(x.split(",")[2])})) \
        .reduceByKey(lambda a, b: dict_merge(a, b)).map(lambda x: (1, {x[0]: x[1]})).reduceByKey(
        lambda a, b: dict_merge(a, b)).map(lambda x: x[1]).collect()[0]

    for user_id in user_to_idx.values():
        if user_id not in user_basket_dict.keys():
            user_basket_dict[user_id] = {}


    ## business basket

    business_basket_dict = \
    tmp_train.map(lambda x: (business_to_idx[x.split(",")[1]], {user_to_idx[x.split(",")[0]]: float(x.split(",")[2])})) \
        .reduceByKey(lambda a, b: dict_merge(a, b)).map(lambda x: (1, {x[0]: x[1]})).reduceByKey(
        lambda a, b: dict_merge(a, b)).map(lambda x: x[1]).collect()[0]


    for business_id in business_to_idx.values():
        if business_id not in business_basket_dict.keys():
            business_basket_dict[business_id] = {}

    ## id to user, id to business

    idx_to_user = {v: k for k, v in user_to_idx.items()}
    idx_to_business = {v: k for k, v in business_to_idx.items()}



    val = tmp_test.map(lambda x: (user_to_idx[x.split(",")[0]], business_to_idx[x.split(",")[1]])) \
        .map(lambda x: (x[0], x[1], user_basket_dict[x[0]])) \
        .map(lambda x: (x[0], x[1], [(similarity(x[1], k), v) for k, v in x[2].items()])) \
        .map(lambda x: (idx_to_user[x[0]], idx_to_business[x[1]], score(x[2], x[0])))

    answer = val.collect()



    with open(output_file, 'w') as csvfile:
        writer_ = csv.writer(csvfile, delimiter=',')
        writer_.writerow(["user_id", "business_id", "prediction"])
        for line in answer:
            writer_.writerow([line[0], line[1], line[2]])

    print("Duration : ", time.time() - t)
