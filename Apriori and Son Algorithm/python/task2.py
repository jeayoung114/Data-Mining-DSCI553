import csv
import sys
import math
import time
from itertools import combinations

from pyspark import SparkContext

sc = SparkContext.getOrCreate()

def scaled_down_support_threshold(basket_list, support, entire_size):
    '''
    calculate ps, scaled down support threshold
    '''
    partition_size = len(basket_list)
    return math.ceil((partition_size / entire_size) * int(support))


def count_singleton(baskets):
    '''
    generate dictionaray of singleton count
    '''
    count_num_dict = {}
    for basket in baskets:
        for item in basket:
            count_num_dict[item] = count_num_dict.get(item, 0) + 1
    return count_num_dict


def get_candidate_singleton(count_dict, ps):
    '''
    Generate candidate from count dictionary
    '''
    candidate_list = []
    for i in count_dict.keys():
        if count_dict[i] >= ps:
            candidate_list.append(i)

    return candidate_list


def get_candidate_pair(count_dict, ps):
    '''
    Generate candidate from count dictionary
    '''
    candidate_list = []
    for i in count_dict.keys():
        if count_dict[i] >= ps:
            candidate_list.append(i)

    return candidate_list


def merge_basket_2d(baskets):
    '''
    partition to basket list
    '''
    a = list(baskets)
    collected_basket = []
    for i in a:
        collected_basket += [i]
    return collected_basket


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






# def count_pairs(baskets, singleton_candidate):
#     count_dict = {}
#     for basket in baskets:
#         for b1 in basket:
#             for b2 in basket:
#                 if b1 < b2 and b1 in singleton_candidate and b2 in singleton_candidate:
#                             count_dict[(b1, b2)] = count_dict.get((b1, b2), 0) + 1

#     return count_dict

# def get_pair_candidate(baskets, singleton_candidate, ps):
#     pair_candi_list = []
#     pair_count = count_pairs(baskets, singleton_candidate)
#     for candidate in pair_count.keys():
#         if pair_count[candidate] > ps:
#             pair_candi_list.append(candidate)
#     return pair_candi_list


def generate_pairs(singleton_candidate):
    pair_list = []
    for i in singleton_candidate:
        for j in singleton_candidate:
            if i < j:
                pair_list.append((i, j))
    return pair_list


def pair_count(baskets, pair_list):
    count_dict = {}
    for pair in pair_list:
        for basket in baskets:
            if set(pair).issubset(set(basket)):
                count_dict[pair] = count_dict.get(pair, 0) + 1
    return count_dict


def get_candidate_pair(count_dict, ps):
    candidate_list = []
    for pair in count_dict.keys():
        if count_dict[pair] >= ps:
            candidate_list.append(pair)

    return candidate_list


def apriori(partition, support, entire_size):
    '''
    perform apriori algorithm in the partition
    with scaled down support threshold
    '''
    ## partition to baskets
    baskets = merge_basket_2d(partition)

    total_candidate = []

    ## singleton
    ps = scaled_down_support_threshold(baskets, support, entire_size)
    singleton_candidate = get_candidate_singleton(count_singleton(baskets), ps)
    baskets = [sorted(list(set(basket).intersection(set(singleton_candidate)))) for basket in baskets]
    total_candidate += [(i,) for i in singleton_candidate]

    ## pairs
    pair_size = 2
    pairs = generate_pairs(singleton_candidate)
    counted_dict = pair_count(baskets, pairs)
    pair_candidate = sorted(get_candidate_pair(counted_dict, ps))
    total_candidate += pair_candidate

    ##above triples
    while len(pair_candidate) > 0:
        pair_size += 1  ## pair_size = 3
        pairs_to_be_counted = generate_permutations(pair_candidate, pair_size)
        counted_dict = pair_count(baskets, pairs_to_be_counted)
        pair_candidate = get_candidate_pair(counted_dict, ps)
        total_candidate += pair_candidate


    return total_candidate


if __name__ == '__main__':



    filter_threshold = int(sys.argv[1])
    support = int(sys.argv[2])  # support threshold
    input_file = sys.argv[3]
    output_file = sys.argv[4]
    n = 10  # partition_num

    ## preprocessing
    start_time = time.time()
    textRDD = sc.textFile(input_file)
    header = textRDD.first()  ## remove header
    tmp = textRDD.filter(lambda x: x != header)

    tmp = tmp.map(
        lambda x: [(x.split(",")[0][:-4] + x.split(",")[0][-2:])[1:-1] + '-' + str(int(x.split(",")[1][1:-1])),
                   int(x.split(",")[5][1:-1])])

    fields = ["DATE-CUSTOMER_ID", "PRODUCT_ID"]
    data = tmp.collect()

    with open('preprocessed_data.csv', 'w') as f:
        write = csv.writer(f)
        write.writerow(fields)
        write.writerows(data)

    input_file = "preprocessed_data.csv"

    ## Start Son

    textRDD = sc.textFile(input_file, n)
    header = textRDD.first()  ## remove header
    tmp = textRDD.filter(lambda x: x != header)

    baskets = tmp.map(lambda x: x.split(",")).mapValues(lambda x: [x]).reduceByKey(lambda a, b: a + b) \
        .mapValues(lambda x: sorted(list(set(x)))).map(lambda x: x[1]).filter(lambda x: len(x) > filter_threshold)
    basket_list = baskets.collect()

    entire_size = len(basket_list)

    num = baskets.mapPartitions(lambda x: [sum(1 for i in x)]).collect()  ## number of items in each partition

    ## Phase 1

    intermediate = baskets.mapPartitions(lambda x: apriori(x, support, entire_size)) \
        .map(lambda x: (x, 1)).reduceByKey(lambda a, b: a)

    candidate_itemsets = list(set(intermediate.collect()))

    num = tmp.mapPartitions(lambda x: [sum(1 for i in x)]).collect()  ## number of items in each partition
    candidate_itemsets = sorted(candidate_itemsets, key=lambda x: (len(x[0]), x[0]))


    ## Phase 2

    def Map2(basket, candidate_itemsets):
        answer_list = []
        for candidate in candidate_itemsets:
            if set(candidate[0]).issubset(set(basket)):
                answer_list.append((candidate[0], 1))
        size = len(answer_list)

        return answer_list


    freq_itemsets = baskets.map(lambda x: Map2(x, candidate_itemsets)).flatMap(lambda x: x)\
        .reduceByKey(lambda a, b: a + b).filter(lambda x: x[1] >= int(support)).map(lambda x: x[0]).collect()

    candidate_itemsets = [candidate[0] for candidate in candidate_itemsets]
    freq_itemsets = sorted(freq_itemsets, key=lambda x: (len(x), x))

    ## file write

    file = open(output_file, "w")
    file.write("Candidates:\n")

    ### format candidate
    max_len = len(candidate_itemsets[-1])
    answer_str = ""
    answer_list = ["" for i in range(max_len + 1)]
    for candidate in candidate_itemsets:
        for i in range(1, max_len + 1):
            if len(candidate) == i:
                answer = "("
                for j in range(len(candidate)):
                    answer += "'"+candidate[j]+"'"
                    answer += ","
                answer = answer[:-1] + ")"
                answer_list[i] += answer
                answer_list[i] += ','
    for i in range(1,len(answer_list)):
        answer_list[i] = answer_list[i][:-1]
        answer_str += answer_list[i]
        answer_str += '\n\n'
    file.write(answer_str)

    file.write("Frequent Itemsets:\n")

    ### format frequent itemsets
    max_len = len(freq_itemsets[-1])
    answer_str = ""
    answer_list = ["" for i in range(max_len + 1)]
    for candidate in freq_itemsets:
        for i in range(1, max_len + 1):
            if len(candidate) == i:
                answer = "("
                for j in range(len(candidate)):
                    answer += "'"+candidate[j]+"'"
                    answer += ","
                answer = answer[:-1] + ")"
                answer_list[i] += answer
                answer_list[i] += ','
    for i in range(1,len(answer_list)):
        answer_list[i] = answer_list[i][:-1]
        answer_str +=answer_list[i]
        answer_str +='\n\n'
    file.write(answer_str)
    file.close()
    duration = (time.time() - start_time)
    print("Duration: ", duration)




