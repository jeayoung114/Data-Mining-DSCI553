import time
import csv
from itertools import combinations
import sys
from pyspark import SparkContext

def userId_to_idx(list_of_users, idx_dict):
    idx_list = []
    for user in list_of_users:
        idx_list.append(idx_dict[user])
    return idx_list


def hash_idx(x, a, b, p, m):
    return (a * x + b) % p % m


p = 75679


def minHash(userid_list, signature_len, p, m, prime_list):
    signature = []
    for num in range(signature_len):
        tmp = []
        for userid in userid_list:
            tmp.append(hash_idx(userid, num * prime_list[num] + 1, prime_list[-1], p, m))
        signature.append(min(tmp))
    return signature


def lsh(x, prime_list):
    return (prime_list[103] * sum(x) + prime_list[-7]) % prime_list[120]


def Jaccard_sim(pair, idx_baskets_dict):
    list1 = idx_baskets_dict[pair[0]]
    list2 = idx_baskets_dict[pair[1]]

    div = len(set(list1 + list2))
    num = len(set(list1).intersection(set(list2)))

    return num / div

if __name__ == '__main__':
    sc = SparkContext('local[*]', 'task1')
    sc.setLogLevel("ERROR")
    input_file = sys.argv[1]
    output_file = sys.argv[2]

    t = time.time()
    #     input_file = "yelp_train.csv"
    n = 30
    p = 75679
    Signature_len = 50
    b = 10
    r = 5


    textRDD = sc.textFile(input_file, n)
    header = textRDD.first()  ## remove header
    tmp = textRDD.filter(lambda x: x != header)
    prime_list = [17393, 17401, 17417, 17419, 17431, 17443, 17449, 17467, 17471, 17477, 17483, 17489, 17491, 17497,
                  17509, 17519, 17539, 17551, 17569, 17573, 17579, 17581, 17597, 17599, 17609, 17623, 17627, 17657,
                  17659, 17669, 17681, 17683, 17707, 17713, 17729, 17737, 17747, 17749, 17761, 17783, 17789, 17791,
                  17807, 17827, 17837, 17839, 17851, 17863, 17881, 17891, 17903, 17909, 17911, 17921, 17923, 17929,
                  17939, 17957, 17959, 17971, 17977, 17981, 17987, 17989, 18013, 18041, 18043, 18047, 18049, 18059,
                  18061, 18077, 18089, 18097, 18119, 18121, 18127, 18131, 18133, 18143, 18149, 18169, 18181, 18191,
                  18199, 18211, 18217, 18223, 18229, 18233, 18251, 18253, 18257, 18269, 18287, 18289, 18301, 18307,
                  18311, 18313, 18329, 18341, 18353, 18367, 18371, 18379, 18397, 18401, 18413, 18427, 18433, 18439,
                  18443, 18451, 18457, 18461, 18481, 18493, 18503, 18517, 18521, 18523, 18539, 18541, 18553, 18583,
                  18587, 18593, 18617, 18637, 18661, 18671, 18679, 18691, 18701, 18713, 18719, 18731, 18743, 18749,
                  18757, 18773, 18787, 18793, 18797, 18803, 18839, 18859, 18869, 18899, 18911, 18913, 18917, 18919,
                  18947, 18959, 18973, 18979, 19001, 19009, 19013, 19031, 19037, 19051, 19069, 19073, 19079, 19081,
                  19087, 19121, 19139, 19141, 19157, 19163, 19181, 19183, 19207, 19211, 19213, 19219, 19231, 19237,
                  19249, 19259, 19267, 19273, 19289, 19301, 19309, 19319, 19333, 19373, 19379, 19381, 19387, 19391,
                  19403, 19417, 19421, 19423, 19427, 19429, 19433, 19441, 19447, 19457, 19463, 19469, 19471, 19477,
                  19483, 19489, 19501, 19507, 19531, 19541, 19543, 19553, 19559, 19571, 19577, 19583, 19597, 19603,
                  19609, 19661, 19681, 19687, 19697, 19699, 19709, 19717, 19727, 19739, 19751, 19753, 19759, 19763,
                  19777, 19793, 19801, 19813, 19819, 19841, 19843, 19853, 19861, 19867, 19889, 19891, 19913, 19919,
                  19927, 19937, 19949, 19961, 19963, 19973, 19979, 19991, 19993, 19997, 20011, 20021, 20023, 20029,
                  20047, 20051, 20063, 20071, 20089, 20101, 20107, 20113, 20117, 20123, 20129, 20143, 20147, 20149,
                  20161, 20173, 20177, 20183, 20201, 20219, 20231, 20233, 20249, 20261, 20269, 20287, 20297, 20323,
                  20327, 20333, 20341, 20347, 20353, 20357]

    ## (business_id , [user1, user2, ...])
    baskets = tmp.map(lambda x: [x.split(",")[1], x.split(",")[0]]).mapValues(lambda x: [x]).reduceByKey(
        lambda a, b: a + b) \
        .mapValues(lambda x: sorted(list(set(x))))

    Total_user_list = \
    baskets.map(lambda x: x[1]).flatMap(lambda x: x).map(lambda x: (x, 1)).reduceByKey(lambda a, b: a).map(
        lambda x: (1, [x[0]])).reduceByKey(lambda a, b: a + b).collect()[0][1]
    Total_business_list = baskets.map(lambda x: (1, [x[0]])).reduceByKey(lambda a, b: a + b).collect()[0][1]
    business_to_idx = {}
    for idx, business in enumerate(Total_business_list):
        business_to_idx[business] = idx
    num_business = idx + 1
    user_to_idx = {}
    for idx, user in enumerate(Total_user_list):
        user_to_idx[user] = idx
    num_Users = idx + 1

    ## Signiture matrix

    ## busindess_id : [1,3,4,7...]
    idx_baskets = baskets.map(lambda x: (business_to_idx[x[0]], userId_to_idx(x[1], user_to_idx))).cache()
    idx_baskets_dict = {}
    for i in idx_baskets.collect():
        idx_baskets_dict[i[0]] = i[1]

    print("Duration : ", time.time() - t)
    ## MinHash
    signature_basket = idx_baskets.map(lambda x: (x[0], minHash(x[1], Signature_len, p, num_Users, prime_list))).cache()

    ##LSH b= 20 r =5
    print("Duration : ", time.time() - t)

    candi_list = []

    for band_num in range(b):
        ## (hash_value, [pair1, pair2, ...])
        hashed_lsh = signature_basket.map(lambda x: (lsh(x[1][band_num * r:(band_num + 1) * r], prime_list), x[0])) \
            .groupByKey().mapValues(lambda x: list(combinations(x, 2)))
        candi_list += hashed_lsh.flatMap(
            lambda x: [(value, Jaccard_sim(value, idx_baskets_dict)) for value in x[1]]).filter(
            lambda x: x[1] >= 0.5).collect()

    answer = set(candi_list)

    idx_to_user = {v: k for k, v in user_to_idx.items()}
    idx_to_business = {v: k for k, v in business_to_idx.items()}

    answer = sorted([(sorted([idx_to_business[x[0][0]], idx_to_business[x[0][1]]]), x[1]) for x in answer])
    with open(output_file, 'w') as csvfile:
        writer_ = csv.writer(csvfile, delimiter=',')
        writer_.writerow(["business_id_1", "business_id_2", "similarity"])
        for line in answer:
            writer_.writerow([line[0][0], line[0][1], str(line[1])])

    duration = time.time() - t
    print("Duration : ", time.time() - t)


