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
import copy


def bfs(root, node_list, edge_list):

    node_list_tmp = copy.deepcopy(node_list)
    edge_list_tmp = []
    node_list_tmp.remove(root)

    node_distance_dict = {}
    node_distance_dict[0] = {root: []}
    shortest_path_num_dict = {}
    for node in node_list:
        shortest_path_num_dict[node] = 0
    shortest_path_num_dict[root] = 1

    ## build tree
    count = 0
    stop = False
    while stop == False:
        count += 1
        tmp = {}
        rem_list = []
        for edge in edge_list:
            if edge[0] in node_distance_dict[count - 1].keys():
                if edge[1] in node_list_tmp:
                    tmp[edge[1]] = tmp.get(edge[1], []) + [edge[0]]
                    rem_list.append(edge[1])
                    edge_list_tmp.append(edge)
                    shortest_path_num_dict[edge[1]] += shortest_path_num_dict[edge[0]]
            elif edge[1] in node_distance_dict[count - 1].keys():
                if edge[0] in node_list_tmp:
                    tmp[edge[0]] = tmp.get(edge[0], []) + [edge[1]]
                    rem_list.append(edge[0])
                    edge_list_tmp.append(edge)
                    shortest_path_num_dict[edge[0]] += shortest_path_num_dict[edge[1]]
        #             print(tmp)
        #             print(node_list)

        rem_list = set(rem_list)
        node_list_tmp = set(node_list_tmp).difference(rem_list)

        if tmp == {}:
            stop = True
        else:
            node_distance_dict[count] = tmp

    return node_distance_dict, edge_list_tmp, shortest_path_num_dict


def edge_credit(tree, edge_list_tmp, shortest_path_num_dict):
    edge_credit_dict = {}
    node_credit_dict = {}
    dist_from_root = max(tree.keys())

    while dist_from_root >= 0:
        ## leaf node
        if dist_from_root == max(tree.keys()):
            for leaf_node in tree[dist_from_root].keys():
                node_credit_dict[leaf_node] = 1
                if len(tree[dist_from_root][leaf_node]) != 0:
                    shortest_path_sum = 0
                    for upper_node in tree[dist_from_root][leaf_node]:
                        shortest_path_sum += shortest_path_num_dict[upper_node]
                #                     edge_credit = 1/len(tree[dist_from_root][leaf_node])

                else:
                    edge_credit = 0
                for upper_node in tree[dist_from_root][leaf_node]:
                    if (leaf_node, upper_node) in edge_list_tmp:
                        edge_credit = shortest_path_num_dict[upper_node] / shortest_path_sum
                        edge_credit_dict[(leaf_node, upper_node)] = edge_credit
                    if (upper_node, leaf_node) in edge_list_tmp:
                        edge_credit = shortest_path_num_dict[upper_node] / shortest_path_sum
                        edge_credit_dict[(upper_node, leaf_node)] = edge_credit
            dist_from_root -= 1

        elif dist_from_root != 0:
            for leaf_node in tree[dist_from_root].keys():
                node_credit_dict[leaf_node] = 1
                for edge in edge_credit_dict.keys():
                    if leaf_node in edge:
                        node_credit_dict[leaf_node] += edge_credit_dict[edge]
                if len(tree[dist_from_root][leaf_node]) != 0:
                    shortest_path_sum = 0
                    for upper_node in tree[dist_from_root][leaf_node]:
                        shortest_path_sum += shortest_path_num_dict[upper_node]
                #                     edge_credit = 1/len(tree[dist_from_root][leaf_node]) * node_credit_dict[leaf_node]
                else:
                    edge_credit = 0
                for upper_node in tree[dist_from_root][leaf_node]:
                    if (leaf_node, upper_node) in edge_list_tmp:
                        edge_credit = shortest_path_num_dict[upper_node] / shortest_path_sum * node_credit_dict[
                            leaf_node]
                        edge_credit_dict[(leaf_node, upper_node)] = edge_credit
                    if (upper_node, leaf_node) in edge_list_tmp:
                        edge_credit = shortest_path_num_dict[upper_node] / shortest_path_sum * node_credit_dict[
                            leaf_node]
                        edge_credit_dict[(upper_node, leaf_node)] = edge_credit
            dist_from_root -= 1

        elif dist_from_root == 0:
            for leaf_node in tree[dist_from_root].keys():
                node_credit_dict[leaf_node] = 0
                for edge in edge_credit_dict.keys():
                    if leaf_node in edge:
                        node_credit_dict[leaf_node] += edge_credit_dict[edge]
            dist_from_root -= 1

    return edge_credit_dict, node_credit_dict


if __name__ == '__main__':
    sc = SparkContext('local[*]', 'task1')
    sc.setLogLevel('ERROR')

    filter_threshold = int(sys.argv[1])
    input_file = sys.argv[2]
    betweenness_output_file_path = sys.argv[3]
    community_output_file_path = sys.argv[4]

    t = time.time()
    n = 30

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

    edge_list = edge_list.map(lambda x: x[0]).collect()

    user_to_idx = {}
    for idx, user in enumerate(user_in_edge_list):
        user_to_idx[user] = idx
    idx_to_user = {v: k for k, v in user_to_idx.items()}

    user_in_edge_list = [user_to_idx[user] for user in user_in_edge_list]
    edge_list = [(user_to_idx[user1], user_to_idx[user2]) for (user1, user2) in edge_list]


    ### Girvan - Newman Algortihm
    # build bfs tree for each node
    def Map1(root, node_list, edge_list):
        tree, edge_list_tmp, shortest_path_num_dict = bfs(root, node_list, edge_list)
        edge_credit_dict, node_credit_dict = edge_credit(tree, edge_list_tmp, shortest_path_num_dict)
        return list(edge_credit_dict.items())


    edge_betweenness = sc.parallelize(user_in_edge_list, n) \
        .map(lambda x: Map1(x, user_in_edge_list, edge_list)) \
        .flatMap(lambda x: x) \
        .reduceByKey(lambda a, b: a + b) \
        .map(lambda x: (tuple(sorted((x[0][0], x[0][1]))), round(x[1] / 2, 5))) \
        .collect()

    edge_betweenness = sorted(edge_betweenness,
                              key=lambda line: (-line[1], idx_to_user[line[0][0]], idx_to_user[line[0][1]]))

    f = open(betweenness_output_file_path, "w")
    for line in edge_betweenness:
        tmp = "('"
        tmp += idx_to_user[line[0][0]]
        tmp += "', '"
        tmp += idx_to_user[line[0][1]]
        tmp += "'),"
        tmp += str(line[1])

        f.write(tmp)
        f.write("\n")

    ### Remove edge and get community

    m = len(edge_list)
    A = [[0 for j in user_in_edge_list] for i in user_in_edge_list]
    for i, j in edge_list:
        A[i][j] = 1
        A[j][i] = 1

    ##Modularity
    oldQ = 0
    newQ = 0
    old_community = []
    new_community = []
    community_list = []
    while oldQ <= newQ:
        print(newQ)
        highest_edge = edge_betweenness[0][0]
        #         print(highest_edge)
        #         print(newQ)
        try:
            edge_list.remove(highest_edge)
        except:
            edge_list.remove((highest_edge[1], highest_edge[0]))

        ## get ki and kj, node degree
        node_degree_dict = {}
        for node in user_in_edge_list:
            node_degree_dict[node] = 0
        for (i, j) in edge_list:
            node_degree_dict[i] = node_degree_dict.get(i, 0) + 1
            node_degree_dict[j] = node_degree_dict.get(j, 0) + 1

        ## get new edge betweenness
        edge_betweenness = sc.parallelize(user_in_edge_list, n) \
            .map(lambda x: Map1(x, user_in_edge_list, edge_list)) \
            .flatMap(lambda x: x) \
            .reduceByKey(lambda a, b: a + b) \
            .map(lambda x: (tuple(sorted((x[0][0], x[0][1]))), round(x[1] / 2, 5))) \
            .collect()

        edge_betweenness = sorted(edge_betweenness,
                                  key=lambda line: (-line[1], idx_to_user[line[0][0]], idx_to_user[line[0][1]]))

        old_community = community_list
        ## get community
        community_list = []
        node_list_tmp_1 = copy.deepcopy(user_in_edge_list)
        while len(node_list_tmp_1) >= 1:
            node_list_tmp_1 = list(node_list_tmp_1)
            node = node_list_tmp_1[-1]
            node_list_tmp_1.remove(node)
            community = []
            for i in bfs(node, user_in_edge_list, edge_list)[0].values():
                community += list(i.keys())
            community_list.append(community)
            node_list_tmp_1 = set(node_list_tmp_1).difference(set(community))

        ## calculate Q
        oldQ = newQ
        newQ = 0

        for community in community_list:
            for i in community:
                for j in community:
                    if i != j:
                        newQ += (A[i][j] - node_degree_dict[i] * node_degree_dict[j] / (2 * m)) / (2 * m)
                    if i == j:
                        newQ += ((A[i][j] - node_degree_dict[i] * node_degree_dict[j] / (2 * m)) / (2 * m))*1/2
        new_community = community_list

    old_community = [sorted([idx_to_user[item] for item in community]) for community in old_community]
    old_community = sorted(old_community, key=lambda x: (len(x), x))

    f = open(community_output_file_path, "w")
    for community in old_community:
        line = ""
        for node in community:
            line += "'" + str(node) + "', "
        line = line[:-2]
        f.write(line)
        f.write("\n")
    f.close()

    print("Duration : ", time.time() - t)