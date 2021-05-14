import sys
import time
import math
from itertools import combinations

from pyspark import SparkContext
import os

sc = SparkContext('local[*]', 'task1')


def singleton_count_bitmap(baskets, ps):
    '''
    count singletons from basket and create bitmap
    '''

    bucket_dict = {}
    count_num_dict = {}
    bitmap = [0 for i in range(n_bucket)]
    # singleton
    for basket in baskets:
        for item in basket:
            count_num_dict[item] = count_num_dict.get(item, 0) + 1

        # bucket
        for pair in combinations(basket, 2):
            hash_val = hash_function(pair)
            #         bucket_dict[hash_val] = bucket_dict.get(hash_val, 0) + 1
            bitmap[hash_val] = bitmap[hash_val] + 1

    freq_singleton = sorted(filter_dict(count_num_dict, ps))
    bitmap = count_to_bitmap(bitmap, ps)

    return freq_singleton, bitmap


def hash_function(pair):
    summation = sum(int(i) for i in pair)
    hash_val = summation % n_bucket
    return hash_val


def filter_dict(dictionary, ps):
    return_list = []
    for key in dictionary.keys():
        if dictionary[key] >= ps:
            return_list.append(key)

    return return_list


def count_to_bitmap(bitmap, ps):
    for i in range(len(bitmap)):
        if bitmap[i] >= ps:
            bitmap[i] = True
        else:
            bitmap[i] = False
    return bitmap


def merge_basket_2d(baskets):
    '''
    partition to basket list
    '''
    a = list(baskets)
    collected_basket = []
    for i in a:
        collected_basket += [i]
    return collected_basket


def scaled_down_support_threshold(basket_list, support, entire_size):
    '''
    calculate ps, scaled down support threshold
    '''
    partition_size = len(basket_list)
    return math.ceil((partition_size / entire_size) * int(support))


def generate_permutations(candidate_list, pair_size):
    permu_list = []
    if len(candidate_list) > 0:
        for idx, itemset in enumerate(candidate_list[:-1]):
            for itemset2 in candidate_list[idx + 1:]:
                if list(itemset[:-1]) == list(itemset2[:-1]):
                    candi = sorted(list(itemset) + [itemset2[-1]])
                    if candi not in permu_list:
                        check_list = [tuple(sorted(i)) for i in list(combinations(candi, pair_size - 1))]
                        if set(check_list).issubset(candidate_list):
                            permu_list.append(tuple(candi))

    return permu_list


def pair_counter(baskets, ps, freq_singleton, bitmap):
    pair_count_dict = {}
    for basket in baskets:
        for pair in combinations(basket, 2):
            if pair[0] in freq_singleton and pair[1] in freq_singleton:
                if bitmap[hash_function(pair)] == True:
                    pair_count_dict[pair] = pair_count_dict.get(pair, 0) + 1
    return pair_count_dict


def counter_above3(pair_list, baskets):
    count_dict = {}
    for pair in pair_list:
        for basket in baskets:
            if set(pair).issubset(set(basket)):
                count_dict[pair] = count_dict.get(pair, 0) + 1
    return count_dict


def single_format(x):
    if type(x) == str:
        return (x,)
    else:
        return x


def pcy(partition, support, entire_size):
    '''
    perform pariori algorithm in the partition
    with scaled down support threshold
    '''
    ## partition to baskets
    baskets = merge_basket_2d(partition)
    ## get ps
    ps = scaled_down_support_threshold(baskets, support, entire_size)

    total_candidate = []

    ## singleton
    freq_singleton, bitmap = singleton_count_bitmap(baskets, ps)
    total_candidate += freq_singleton
    baskets = [sorted(list(set(basket).intersection(set(freq_singleton)))) for basket in baskets]

    ## pairs
    pair_size = 2
    pair_list = combinations(freq_singleton, 2)
    pair_count_dict = pair_counter(baskets, ps, freq_singleton, bitmap)
    freq_singleton = filter_dict(pair_count_dict, ps)
    candidate_list = freq_singleton
    total_candidate += candidate_list

    # above pair_size 3
    while len(candidate_list) > 0:
        pair_size += 1  ## pair_size = 3
        pair_list = generate_permutations(candidate_list, pair_size)
        count_dict = counter_above3(pair_list, baskets)
        candidate_list = filter_dict(count_dict, ps)
        total_candidate += candidate_list

    return total_candidate


if __name__ == '__main__':

    case = int(sys.argv[1])
    support = int(sys.argv[2])  # support threshold
    input_file = sys.argv[3]
    output_file = sys.argv[4]
    n = 2  # partition_num
    n_bucket = 100


    start_time = time.time()
    textRDD = sc.textFile(input_file, n)
    header = textRDD.first()  ## remove header
    tmp = textRDD.filter(lambda x: x != header)

    ## get basket
    if case == 1:
        baskets = tmp.map(lambda x: x.split(",")).mapValues(lambda x: [x]).reduceByKey(lambda a, b: a + b) \
            .mapValues(lambda x: sorted(list(set(x)))).map(lambda x: x[1])
        basket_list = baskets.collect()

    elif case == 2:
        baskets = tmp.map(lambda x: [x.split(",")[1], x.split(",")[0]]).mapValues(lambda x: [x]).reduceByKey(
            lambda a, b: a + b) \
            .mapValues(lambda x: sorted(list(set(x)))).map(lambda x: x[1])
        basket_list = baskets.collect()

    entire_size = len(basket_list)

    #     num = baskets.mapPartitions(lambda x: [sum(1 for i in x)]).collect() ## number of items in each partition

    ## Phase 1
    ## pcy algoritm for each partition
    intermediate = baskets.mapPartitions(lambda x: pcy(x, support, entire_size)) \
        .map(lambda x: (single_format(x), 1)).reduceByKey(lambda a, b: a)
    candidate_itemsets = intermediate.collect()
    candidate_itemsets = sorted(candidate_itemsets, key=lambda x: (len(x[0]), x[0]))


    # phase2
    def Map2(basket, candidate_itemsets):
        answer_list = []
        for candidate in candidate_itemsets:
            if set(candidate[0]).issubset(set(basket)):
                answer_list.append((candidate[0], 1))
        size = len(answer_list)

        return answer_list


    freq_itemsets = baskets.map(lambda x: Map2(x, candidate_itemsets)).flatMap(lambda x: x).reduceByKey(
        lambda a, b: a + b).filter(lambda x: x[1] >= int(support)).map(lambda x: x[0]).collect()

    freq_itemsets = sorted(freq_itemsets, key=lambda x: (len(x), x))


    ## file write

    file = open(output_file, "w")
    file.write("Candidates:")

    ### format candidate
    max_len = len(candidate_itemsets[-1][0])
    answer_str = ""
    answer_list = ["" for i in range(max_len + 1)]
    for candidate in candidate_itemsets:
        for i in range(1, max_len + 1):
            if len(candidate[0]) == i:
                answer = "('"
                for j in range(len(candidate[0])):
                    answer += candidate[0][j]
                    answer += ","
                answer = answer[:-1] + "')"
                answer_list[i] += answer
                answer_list[i] += ','
    for i in range(len(answer_list)):
        answer_list[i] = answer_list[i][:-1]
        answer_str += answer_list[i]
        answer_str += '\n\n'
    file.write(answer_str)

    file.write("Frequent Itemsets:")

    ### format frequent itemsets
    max_len = len(freq_itemsets[-1])
    answer_str = ""
    answer_list = ["" for i in range(max_len + 1)]
    for candidate in freq_itemsets:
        for i in range(1, max_len + 1):
            if len(candidate) == i:
                answer = "('"
                for j in range(len(candidate)):
                    answer += candidate[j]
                    answer += ","
                answer = answer[:-1] + "')"
                answer_list[i] += answer
                answer_list[i] += ','
    for i in range(len(answer_list)):
        answer_list[i] = answer_list[i][:-1]
        answer_str += answer_list[i]
        answer_str += '\n\n'
    file.write(answer_str)
    file.close()



    duration = (time.time() - start_time)
    print("Duration: ", duration)






