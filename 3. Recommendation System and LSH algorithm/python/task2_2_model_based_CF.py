import numpy as np
import pandas as pd
import time
import math
import csv
import json
import xgboost as xgb
import datetime
import sys
from pyspark import SparkContext
import warnings
from sklearn import preprocessing
warnings.filterwarnings(action='ignore')


def len_dict(x):
    if x == "None":
        return 0
    elif x == None:
        return 0
    else:
        return len(x)


def date_cal(x):
    #     print(x)
    return (datetime.date(2021, 3, 10) - datetime.date(int(x.split("-")[0]), int(x.split("-")[1]),
                                                       int(x.split("-")[2]))).days


def get_length(x):
    if x == "None":
        return 0
    elif x == None:
        return 0
    else:
        return len(x.split(","))


def get_max(x):
    if x == "None":
        return 0
    elif x == None:
        return 0
    else:
        return int(sorted(x.split(","), reverse=True)[0])


def avg(x):
    a = 0
    for i in x:
        a += i
    return a / len(x)


def summation(x):
    a = 0
    for i in x:
        a += i
    return a


if __name__ == '__main__':
    sc = SparkContext('local[*]', 'task2')
    sc.setLogLevel("ERROR")


    folder_path = sys.argv[1]
    test_file_name = sys.argv[2]
    output_file = sys.argv[3]


    t = time.time()
    n = 30

    ## RDDs
    textRDD = sc.textFile(folder_path + 'yelp_train.csv', n)
    testRDD = sc.textFile(test_file_name, n)

    ## exclue header
    header = textRDD.first()  ## remove header
    header2 = testRDD.first()  ## remove header
    tmp_train = textRDD.filter(lambda x: x != header)
    tmp_test = testRDD.filter(lambda x: x != header2)

    ##make list of user and business
    user_list_train = \
    tmp_train.map(lambda x: (x.split(",")[0], 1)).reduceByKey(lambda a, b: a).map(lambda x: (1, [x[0]])).reduceByKey(
        lambda a, b: a + b).collect()[0][1]
    user_list_test = \
    tmp_test.map(lambda x: (x.split(",")[0], 1)).reduceByKey(lambda a, b: a).map(lambda x: (1, [x[0]])).reduceByKey(
        lambda a, b: a + b).collect()[0][1]
    user_list = list(set(user_list_train + user_list_test))


    business_list_train = \
    tmp_train.map(lambda x: (x.split(",")[1], 1)).reduceByKey(lambda a, b: a).map(lambda x: (1, [x[0]])).reduceByKey(
        lambda a, b: a + b).collect()[0][1]
    business_list_test = \
    tmp_test.map(lambda x: (x.split(",")[1], 1)).reduceByKey(lambda a, b: a).map(lambda x: (1, [x[0]])).reduceByKey(
        lambda a, b: a + b).collect()[0][1]
    business_list = list(set(business_list_train + business_list_test))


    ##create user_dictionary, business_dictionary

    business_dict = {}
    user_dict = {}

    for user in user_list:
        user_dict[user] = {}
    for business in business_list:
        business_dict[business] = {}
    ### fill user_dict and business_dict with json files
    '''
    checkin : checkin sum, avg for business

    business : stars, review_count, latitude, longitude, how many days does it open (len(hours)), attribute length?, 

    photo  : number of photos

    review : number of reviews based on user, number of reviews based on business, avg star, useful (sum avg), funny (sum, avg), cool (sum, avg)

    tip : number of tips based on user, number of tips based on business, number of likes

    user : review_count, daycount of (today -yelpsince), number of friend , useful, funny, cool, fans, most reent "elite" year, number of "elite", avgstars, 

    compliment_hot, more, profile, cute, list, note plain, cool, funny, writer, photos

    '''
    ### Extract Features and then merge it into user_dict and business_dict

    ## checkin
    checkinRDD = sc.textFile(folder_path + "checkin.json", n)
    checkin = checkinRDD.map(lambda line: json.loads(line)).map(lambda x: (x["business_id"], x["time"])).map(
        lambda x: (x[0], list(x[1].values()))).map(lambda x: (x[0], summation(x[1]), avg(x[1]))).collect()
    for i in checkin:
        try:
            business_dict[i[0]]["checkin_sum"] = i[1]
            business_dict[i[0]]["checkin_avg"] = i[2]
        except:
            continue
    del checkin

    # business
    businessRDD = sc.textFile(folder_path + "business.json", n)
    business = businessRDD.map(lambda line: json.loads(line)) \
        .map(lambda x: (
    x["business_id"], x["latitude"], x["longitude"], x["stars"], x["review_count"], x["is_open"],
    len_dict(x["attributes"]), get_length(x["categories"]), len_dict(x["hours"]))).collect()

    for i in business:
        try:
            business_dict[i[0]]["latitude"] = i[1]
            business_dict[i[0]]["longitude"] = i[2]
            business_dict[i[0]]["stars"] = i[3]
            business_dict[i[0]]["review_count"] = i[4]
            business_dict[i[0]]["is_open"] = i[5]
            business_dict[i[0]]["len_attributes"] = i[6]
            business_dict[i[0]]["len_categories"] = i[7]
            business_dict[i[0]]["len_hours"] = i[8]
        except:
            continue
    del business

    # photo
    photoRDD = sc.textFile(folder_path + "photo.json", n)
    photo = photoRDD.map(lambda line: json.loads(line)) \
        .map(lambda x: (x["business_id"], 1)).reduceByKey(lambda a, b: a + b).collect()

    for i in photo:
        try:
            business_dict[i[0]]["n_photo"] = i[1]

        except:
            continue
    del photo

    # review
    reviewRDD = sc.textFile(folder_path + "review_train.json", n)
    review = reviewRDD.map(lambda line: json.loads(line)) \
        .map(lambda x: (x["business_id"], (1, x["stars"], len(x["text"])))).reduceByKey(
        lambda a, b: (a[0] + b[0], a[1] + b[1], a[2] + b[2])) \
        .map(lambda x: (x[0], x[1][0], x[1][1] / x[1][0], x[1][2] / x[1][0])).collect()

    for i in review:
        try:
            business_dict[i[0]]["n_review_review"] = i[1]
            business_dict[i[0]]["avg_review_stars"] = i[2]
            business_dict[i[0]]["avg_review_len"] = i[3]

        except:
            continue

    review = reviewRDD.map(lambda line: json.loads(line)) \
        .map(lambda x: (x["user_id"], (1, x["stars"], len(x["text"])))).reduceByKey(
        lambda a, b: (a[0] + b[0], a[1] + b[1], a[2] + b[2])) \
        .map(lambda x: (x[0], x[1][0], x[1][1] / x[1][0], x[1][2] / x[1][0])).collect()

    for i in review:
        try:
            user_dict[i[0]]["n_review_review"] = i[1]
            user_dict[i[0]]["avg_review_stars"] = i[2]
            user_dict[i[0]]["avg_review_len"] = i[3]

        except:
            continue
    del review

    # tip
    tipRDD = sc.textFile(folder_path + "tip.json", n)
    tip = tipRDD.map(lambda line: json.loads(line)) \
        .map(lambda x: (x["business_id"], (1, x["likes"], len(x["text"])))).reduceByKey(
        lambda a, b: (a[0] + b[0], a[1] + b[1], a[2] + b[2])) \
        .map(lambda x: (x[0], x[1][0], x[1][1] / x[1][0], x[1][2] / x[1][0])).collect()

    for i in tip:
        try:
            business_dict[i[0]]["n_tip_business"] = i[1]
            business_dict[i[0]]["avg_like_business"] = i[2]
            business_dict[i[0]]["avg_tip_len_business"] = i[3]

        except:
            pass

    tip = tipRDD.map(lambda line: json.loads(line)) \
        .map(lambda x: (x["user_id"], (1, x["likes"], len(x["text"])))).reduceByKey(
        lambda a, b: (a[0] + b[0], a[1] + b[1], a[2] + b[2])) \
        .map(lambda x: (x[0], x[1][0], x[1][1] / x[1][0], x[1][2] / x[1][0])).collect()

    for i in tip:
        try:
            user_dict[i[0]]["n_tip_user"] = i[1]
            user_dict[i[0]]["avg_like_user"] = i[2]
            user_dict[i[0]]["avg_tip_len_user"] = i[3]

        except:
            continue
    del tip

    ## user
    userRDD = sc.textFile(folder_path + "user.json", n). \
        map(lambda line: json.loads(line)) \
        .filter(lambda x: x["user_id"] in user_list)
    user_data = userRDD.map(lambda x: (x["user_id"], x["review_count"], \
                                       (datetime.date(2021, 3, 10) - datetime.date(
                                           int(x["yelping_since"].split("-")[0]),
                                           int(x["yelping_since"].split("-")[1]),
                                           int(x["yelping_since"].split("-")[2]))).days, \
                                       get_length(x["friends"]), \
                                       x["useful"], \
                                       x["funny"], \
                                       x["fans"], \
                                       get_length(x["elite"]), \
                                       get_max(x["elite"]), \
                                       x["average_stars"], \
                                       x["compliment_hot"], \
                                       x["compliment_more"], \
                                       x["compliment_cute"], \
                                       x["compliment_list"], \
                                       x["compliment_note"], \
                                       x["compliment_plain"], \
                                       x["compliment_cool"], \
                                       x["compliment_funny"], \
                                       x["compliment_writer"], \
                                       x["compliment_photos"], \
                                       )).collect()

    for i in user_data:
        try:
            user_dict[i[0]]["review_count"] = i[1]
            user_dict[i[0]]["date_since"] = i[2]
            user_dict[i[0]]["n_friends"] = i[3]
            user_dict[i[0]]["useful"] = i[4]
            user_dict[i[0]]["funny"] = i[5]
            user_dict[i[0]]["fans"] = i[6]
            user_dict[i[0]]["n_elite"] = i[7]
            user_dict[i[0]]["max_elite"] = i[8]
            user_dict[i[0]]["avg_stars"] = i[9]
            user_dict[i[0]]["compliment_hot"] = i[10]
            user_dict[i[0]]["compliment_more"] = i[11]
            user_dict[i[0]]["compliment_cute"] = i[12]
            user_dict[i[0]]["compliment_list"] = i[13]
            user_dict[i[0]]["compliment_note"] = i[14]
            user_dict[i[0]]["compliment_plain"] = i[15]
            user_dict[i[0]]["compliment_cool"] = i[16]
            user_dict[i[0]]["compliment_funny"] = i[17]
            user_dict[i[0]]["compliment_writer"] = i[18]
            user_dict[i[0]]["compliment_photos"] = i[19]


        except:
            continue
    del user_data

    print("Duration : ", time.time() - t)

    ## make train_data from user_dict and business_dict
    ## Combine user feature vector and business feature vector and stored as pandas DataFrame

    train_vec = tmp_train.map(lambda x: (x.split(",")[0], x.split(",")[1], x.split(",")[2])) \
        .map(lambda x: (x[0], x[1], user_dict[x[0]], business_dict[x[1]], float(x[2])))
    train_vec = train_vec.collect()
    print("Duration : ", time.time() - t)

    train_data = pd.DataFrame()
    data_row = []
    count = 0
    for train_row in train_vec:
        count += 1
        row = {}
        for i in train_row[2].items():
            row[i[0]] = i[1]
        for i in train_row[3].items():
            row[i[0]] = i[1]
        row["target"] = train_row[4]
        data_row.append(row)

    train_data = pd.DataFrame.from_dict(data_row)
    print("Duration : ", time.time() - t)

    ## minmax scaler
    ## apply minmax scaler for the features
    ## fill na with -1

    scaler = preprocessing.MinMaxScaler()
    train_data_no_label = train_data.loc[:, train_data.columns != 'target']
    train_data_no_label_scaled = pd.DataFrame(scaler.fit_transform(train_data_no_label),
                                              index=train_data_no_label.index, columns=train_data_no_label.columns)
    train_data_no_label_scaled["target"] = train_data["target"]
    train_data = train_data_no_label_scaled
    train_data.fillna(-1)

    ## make test vector
    test_vec = tmp_test.map(lambda x: (x.split(",")[0], x.split(",")[1])) \
        .map(lambda x: (x[0], x[1], user_dict[x[0]], business_dict[x[1]]))
    test_vec = test_vec.collect()  ## user , business

    test_data = pd.DataFrame()
    data_row = []
    count = 0
    for test_row in test_vec:
        count += 1
        row = {}
        for i in test_row[2].items():
            row[i[0]] = i[1]
        for i in test_row[3].items():
            row[i[0]] = i[1]
        #         row["target"] = train_row[4]
        data_row.append(row)

    test_data = pd.DataFrame.from_dict(data_row)
    test_data.fillna(-1)

    ## minmax scaler
    test_data = pd.DataFrame(scaler.transform(test_data), index=test_data.index, columns=test_data.columns)

    ## train xgboost regressor
    xg_reg = xgb.XGBRegressor(objective ='reg:linear', colsample_bytree = 0.3, learning_rate = 0.1,\
                max_depth = 9, alpha = 0, n_estimators = 70, subsample = 0.5, random_state = 0)
    selected_columns = ['stars',
  'avg_review_stars',
  'avg_stars',
  'len_hours',
  'is_open',
  'n_elite',
  'compliment_photos',
  'compliment_hot',
  'compliment_funny',
  'n_friends',
  'max_elite',
  'latitude']
    xg_reg.fit(train_data.loc[:, train_data.columns != 'target'][selected_columns], train_data["target"])
    print("Xgb train Duration : ", time.time() - t)

    ## predict test_data
    predictions = xg_reg.predict(test_data[selected_columns])

    answer = []
    for test, score in zip(test_vec, predictions):
        answer.append((test[0], test[1], score))

    with open(output_file, 'w') as csvfile:
        writer_ = csv.writer(csvfile, delimiter=',')
        writer_.writerow(["user_id", "business_id", "prediction"])
        for line in answer:
            writer_.writerow([line[0], line[1], line[2]])

    print("Duration : ", time.time() - t)


    ##########################
    #### Feature Selection####
    ##########################

    # imp = xg_reg.feature_importances_
    #
    # importance = {}
    # for i, j in zip(list(train_data.columns)[:-1], imp):
    #     importance[i] = j
    #
    # importance_sorted = list(dict(sorted(importance.items(), key=lambda item: item[1], reverse=True)).keys())
    #
    # for idx in range(len(importance_sorted))[1:]:
    #     ## train xgboost regressor
    #     xg_reg = xgb.XGBRegressor(objective='reg:squarederror', colsample_bytree=0.3, learning_rate=0.1,
    #                               max_depth=10, alpha=0, n_estimators=100)
    #
    #     xg_reg.fit(train_data.loc[:, train_data.columns != 'target'][importance_sorted[0:idx]], train_data["target"])
    #     print("Xgb train Duration : ", time.time() - t)
    #
    #     print("Duration : ", time.time() - t)
    #
    #     predictions = xg_reg.predict(test_data[importance_sorted[0:idx]])
    #
    #     answer = []
    #     for test, score in zip(test_vec, predictions):
    #         answer.append((test[0], test[1], score))
    #
    #     with open(output_file, 'w') as csvfile:
    #         writer_ = csv.writer(csvfile, delimiter=',')
    #         writer_.writerow(["user_id", "business_id", "prediction"])
    #         for line in answer:
    #             writer_.writerow([line[0], line[1], line[2]])
    #
    #     print("Duration : ", time.time() - t)
    #
    #     res = pd.read_csv("task2_2.csv")
    #     val = pd.read_csv("yelp_val.csv")
    #
    #     new_df = pd.merge(res, val, how='left', left_on=['user_id', 'business_id'], right_on=['user_id', 'business_id'])
    #
    #     new_df["rmse"] = new_df["prediction"] - new_df["stars"]
    #
    #     new_df["rmse"] = new_df.rmse.apply(lambda x: x ** 2)
    #
    #     print("RMSE : ", (sum(new_df.rmse) / 142043) ** 0.5)
    #     print(importance_sorted[0:idx])



