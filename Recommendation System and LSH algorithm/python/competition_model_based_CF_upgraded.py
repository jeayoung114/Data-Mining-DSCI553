# Method Description:
# In hw3, I used a hybrid recommender system of item based CF and model based CF.
# I used a weighted sum of item based CF score and model based CF score.
# The ratio of weighted sum was selected based on the number of corated items.

# In this final submission, I only used model based CF.
# I could improve the performance of xgboost regressor by adding and subtracting some features and after that item based CF scored worse than model based CF in almost every range.
# I added features of business state and category.
# In case of state, I used one hot encoding to express it as a vector.
# For category, as there were more than a thousand categories, I used pca after one hot encoding.
# In addition, I removed all the other features extracted from tip, photo, review.
# I found that features from those files made the performance of the model worse.

# While fitting the model, I used a high n_estimator and used an early stopping round method.
# By doing so, I could stop training in the appropriate round.

# Error Distribution:
# >=0 and <1: 102353
# >=1 and <2: 32799
# >=2 and <3: 6091
# >=3 and <4: 796
# >=4: 5

# RMSE: 0.9777892711284395


# Execution Time:  1047.774572134018


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
from sklearn.decomposition import PCA
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
        tmp_train.map(lambda x: (x.split(",")[0], 1)).reduceByKey(lambda a, b: a).map(
            lambda x: (1, [x[0]])).reduceByKey(
            lambda a, b: a + b).collect()[0][1]
    user_list_test = \
        tmp_test.map(lambda x: (x.split(",")[0], 1)).reduceByKey(lambda a, b: a).map(lambda x: (1, [x[0]])).reduceByKey(
            lambda a, b: a + b).collect()[0][1]
    user_list = list(set(user_list_train + user_list_test))

    business_list_train = \
        tmp_train.map(lambda x: (x.split(",")[1], 1)).reduceByKey(lambda a, b: a).map(
            lambda x: (1, [x[0]])).reduceByKey(
            lambda a, b: a + b).collect()[0][1]
    business_list_test = \
        tmp_test.map(lambda x: (x.split(",")[1], 1)).reduceByKey(lambda a, b: a).map(lambda x: (1, [x[0]])).reduceByKey(
            lambda a, b: a + b).collect()[0][1]
    business_list = list(set(business_list_train + business_list_test))

    del business_list_train
    del business_list_test
    del user_list_train
    del user_list_test
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

    # business
    businessRDD = sc.textFile(folder_path + "business.json", n)
    business = businessRDD.map(lambda line: json.loads(line)) \
        .map(lambda x: (
        x["business_id"], x["latitude"], x["longitude"], x["stars"], x["review_count"], x["is_open"],
        len_dict(x["attributes"]), get_length(x["categories"]), len_dict(x["hours"]), x["state"])).collect()

    for i in business:
        try:
            business_dict[i[0]]["state"] = i[9]
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

    # business _ category
    businessRDD = sc.textFile(folder_path + "business.json", n)
    business_category = businessRDD.map(lambda line: json.loads(line)) \
        .map(lambda x: (x["business_id"], x["categories"])).collect()

    bus = pd.DataFrame(business_category, columns=["business_id", "category"])

    bus["category"] = bus["category"].fillna("")

    bus["category"] = bus["category"].apply(lambda x: x.split(", "))

    listofitems = []
    for i in bus["category"]:
        listofitems += i

    allitems = set(listofitems)


    def inthelist(x):
        if item in x:
            return 1
        else:
            return 0


    for item in allitems:
        bus[item] = bus["category"].apply(inthelist)


    # business attributes

    def key_val(dictionary):
        key_val_list = dict()
        if dictionary == None:
            return {}
        for k, v in dictionary.items():
            if "{" in v:
                tmp = [l1.split(": ") for l1 in v.replace("'", "").replace("{", "").replace("}", "").split(", ")]
                for i in tmp:
                    if i[1].isdigit() == True:
                        key_val_list[i[0]] = int(i[1])
                    else:
                        key_val_list[i[0]] = i[1]

            else:
                if v.isdigit() == True:
                    key_val_list[k] = int(v)
                else:
                    key_val_list[k] = v
        return key_val_list


    # business attributes
    businessRDD = sc.textFile(folder_path + "business.json", n)
    business_attr = businessRDD.map(lambda line: json.loads(line)) \
        .map(lambda x: (x["business_id"], x["attributes"])) \
        .map(lambda x: (x[0], key_val(x[1]))).collect()

    tmp_dict = dict()
    for business_id, dic in business_attr:
        tmp_dict[business_id] = dic

    attr_df = pd.DataFrame(tmp_dict).T
    attr_df = pd.get_dummies(attr_df, drop_first=True)

    ##############################
    ##### Attribute PCA ##########
    ##############################

    n_components = 5

    x_pca = PCA(n_components=n_components).fit_transform(attr_df)
    attr_df = pd.DataFrame(x_pca, index=attr_df.index, columns=["attr_pca_" + str(i + 1) for i in range(n_components)])

    del business_attr
    del tmp_dict
    del business_category
    #############################
    ##### Category PCA ##########
    #############################

    n_components = 10

    x_pca = PCA(n_components=n_components).fit_transform(bus.iloc[:, 3:])

    business_category = pd.DataFrame(x_pca, columns=["pca_" + str(i + 1) for i in range(n_components)])
    business_category["business_id"] = bus["business_id"]

    for index, row in business_category.iterrows():
        #         print(index )
        #         print(row["pca_1"])
        try:
            for i in range(n_components):
                business_dict[row["business_id"]]["category_pca_" + str(i + 1)] = row["pca_" + str(i + 1)]
        except:
            continue

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
    del bus

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
    print("Duration : ", time.time() - t)

    ## make train_data from user_dict and business_dict
    ## Combine user feature vector and business feature vector and stored as pandas DataFrame

    train_vec = tmp_train.map(lambda x: (x.split(",")[0], x.split(",")[1], x.split(",")[2])) \
        .map(lambda x: (x[0], x[1], user_dict[x[0]], business_dict[x[1]], attr_df.loc[x[1]], float(x[2])))
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
        for idx, i in enumerate(train_row[4]):
            row["bus_attr_" + str(idx)] = i
        row["target"] = train_row[5]
        data_row.append(row)

    train_data = pd.DataFrame.from_dict(data_row)
    del data_row
    print("Duration : ", time.time() - t)

    ## make test vector
    test_vec = tmp_test.map(lambda x: (x.split(",")[0], x.split(",")[1])) \
        .map(lambda x: (x[0], x[1], user_dict[x[0]], business_dict[x[1]], attr_df.loc[x[1]]))
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
        for idx, i in enumerate(test_row[4]):
            row["bus_attr_" + str(idx)] = i
        data_row.append(row)
    test_data = pd.DataFrame.from_dict(data_row)
    del data_row

    X_train = train_data.loc[:, train_data.columns != 'target']
    y_train = train_data["target"]
    X_test = test_data.iloc[:, :]

    train_len = len(X_train)

    dataset = pd.concat(objs=[X_train, X_test], axis=0)

    dataset_one_hot = pd.get_dummies(dataset, drop_first=True)

    X_train = dataset_one_hot[:train_len]
    X_test = dataset_one_hot[train_len:]

    X_train_final = X_train[:]
    X_test_final = X_test[:]

    del X_train
    del X_test
    del dataset_one_hot
    del dataset

    ## train xgboost regressor
    xg_reg = xgb.XGBRegressor(objective='reg:linear', colsample_bytree=0.7, learning_rate=0.07, \
                              max_depth=8, n_estimators=300, subsample=1.0, random_state=0, min_child_weight=4,
                              reg_alpha=0.5, reg_lambda=0.5)
    n_train = len(X_train_final)
    xg_reg.fit(X_train_final, y_train, eval_metric='rmse', eval_set=[(X_train_final, y_train)], early_stopping_rounds=5)
    print("Xgb Training Ended")

    ## predict test_data
    predictions = xg_reg.predict(X_test_final)

    answer = []
    for test, score in zip(test_vec, predictions):
        if score > 5:
            answer.append((test[0], test[1], 5.0))
        elif score < 1:
            answer.append((test[0], test[1], 1.0))
        else:
            answer.append((test[0], test[1], score))

    with open(output_file, 'w') as csvfile:
        writer_ = csv.writer(csvfile, delimiter=',')
        writer_.writerow(["user_id", "business_id", "prediction"])
        for line in answer:
            writer_.writerow([line[0], line[1], line[2]])
    print("File Write Ended")
    print("Duration : ", time.time() - t)

