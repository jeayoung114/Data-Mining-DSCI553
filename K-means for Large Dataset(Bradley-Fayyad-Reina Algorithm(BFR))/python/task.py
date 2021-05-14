from pyspark import SparkContext
import random
from sklearn.cluster import KMeans
import sys
import time
import warnings
warnings.filterwarnings(action='ignore')


def random_split_5():
    rand_num = random.random()
    if rand_num < 0.2:
        return 0
    elif rand_num < 0.4:
        return 1
    elif rand_num < 0.6:
        return 2
    elif rand_num < 0.8:
        return 3
    else:
        return 4


def Mahalanobis_Distance(x, N, SUM, SUMSQ):
    d_to_cluster = dict()
    for cluster_idx in range(len(SUM)):
        tmp = 0
        for idx in range(len(x) - 2):
            c_i = SUM[cluster_idx][1][idx] / N[cluster_idx][1]
            sigma_i = ((SUMSQ[cluster_idx][1][idx] / N[cluster_idx][1] - (
                        SUM[cluster_idx][1][idx] / N[cluster_idx][1]) ** 2) ** 0.5)
            tmp += ((x[idx + 2] - c_i) / sigma_i) ** 2
        d = tmp ** 0.5
        d_to_cluster[SUM[cluster_idx][0]] = d

    return d_to_cluster


def Mahalanobis_Distance_btw_two_clusters(cluster_id, N, SUM, SUMSQ, dim):
    d_to_cluster = dict()
    c_n = dict(N)[cluster_id]
    c_sum = dict(SUM)[cluster_id]
    c_sumsq = dict(SUMSQ)[cluster_id]

    c_centroid = [dict(SUM)[cluster_id][idx] / dict(N)[cluster_id] for idx in range(dim)]
    c_sigma = [((dict(SUMSQ)[cluster_id][idx] / dict(N)[cluster_id] - (
                dict(SUM)[cluster_id][idx] / dict(N)[cluster_id]) ** 2) ** 0.5) for idx in range(dim)]
    for cluster_idx in range(len(SUM)):
        tmp = 0
        for idx in range(dim):
            c_i = SUM[cluster_idx][1][idx] / N[cluster_idx][1]
            sigma_i = ((SUMSQ[cluster_idx][1][idx] / N[cluster_idx][1] - (
                        SUM[cluster_idx][1][idx] / N[cluster_idx][1]) ** 2) ** 0.5)
            tmp += ((c_centroid[idx] - c_i) ** 2) / (c_sigma[idx] * sigma_i)
        d = tmp ** 0.5
        d_to_cluster[SUM[cluster_idx][0]] = d

    return d_to_cluster


def Mahalanobis_Distance_btw_CS_DS(cluster_id, N, SUM, SUMSQ, dim, N_CS, SUM_CS, SUMSQ_CS):
    d_to_cluster = dict()
    c_n = dict(N_CS)[cluster_id]
    c_sum = dict(SUM_CS)[cluster_id]
    c_sumsq = dict(SUMSQ_CS)[cluster_id]

    c_centroid = [dict(SUM_CS)[cluster_id][idx] / dict(N_CS)[cluster_id] for idx in range(dim)]
    c_sigma = [((dict(SUMSQ_CS)[cluster_id][idx] / dict(N_CS)[cluster_id] - (
                dict(SUM_CS)[cluster_id][idx] / dict(N_CS)[cluster_id]) ** 2) ** 0.5) for idx in range(dim)]
    for cluster_idx in range(len(SUM)):
        tmp = 0
        for idx in range(dim):
            c_i = SUM[cluster_idx][1][idx] / N[cluster_idx][1]
            sigma_i = ((SUMSQ[cluster_idx][1][idx] / N[cluster_idx][1] - (
                        SUM[cluster_idx][1][idx] / N[cluster_idx][1]) ** 2) ** 0.5)
            tmp += ((c_centroid[idx] - c_i) ** 2) / (c_sigma[idx] * sigma_i)
        d = tmp ** 0.5
        d_to_cluster[SUM[cluster_idx][0]] = d

    return d_to_cluster


def N_SUM_SSQ(CS):
    N_CS = []
    SUM_CS = []
    SUMSQ_CS = []
    for key in CS:
        rows = CS[key]
        num = 0
        s = [0 for i in range(len(rows[0]) - 2)]
        ssq = [0 for i in range(len(rows[0]) - 2)]
        for row in rows:
            num += 1
            for i in range(len(rows[0]) - 2):
                s[i] += row[i + 2]
                ssq[i] += row[i + 2] ** 2
        N_CS.append((key, num))
        SUM_CS.append((key, s))
        SUMSQ_CS.append((key, ssq))
    return N_CS, SUM_CS, SUMSQ_CS


if __name__ == '__main__':

    t = time.time()
    n = 30
    sc = SparkContext('local[*]', 'task')
    sc.setLogLevel('ERROR')

    input_file = sys.argv[1]
    n_cluster = int(sys.argv[2])
    output_file = sys.argv[3]

    ## RDDs
    textRDD = sc.textFile(input_file, n)

    # Step 1. Load 20% of the data randomly.
    random_data = textRDD \
        .map(lambda x: [float(feature) for feature in x.split(",")]) \
        .map(lambda x: (x, random_split_5())) \
        .cache()

    random_data_0 = random_data.filter(lambda x: x[1] == 0).map(lambda x: x[0])
    Round = 1
    dim = len(random_data_0.take(1)[0]) - 2
    #     print("num_round : " , len(random_data_0.collect()))

    # Step 2. Run K-Means (e.g., from sklearn) with a large K (e.g., 5 times of the number of the input clusters) on the data in memory using the Euclidean distance as the similarity measurement.

    collected_data = random_data_0.collect()
    large_K = n_cluster * 20
    kmeans = KMeans(n_clusters=large_K, random_state=0).fit([row[2:] for row in collected_data])  ## 0 : id, 1 : label

    # Step 3. In the K-Means result from Step 2, move all the clusters that contain only one point to RS (outliers).

    RS = []
    # result of kmeans which has only one point
    RS_labels = sc.parallelize(kmeans.labels_, n) \
        .map(lambda x: (x, 1)) \
        .reduceByKey(lambda a, b: a + b) \
        .filter(lambda x: x[1] == 1) \
        .map(lambda x: x[0]).collect()
    for row, label in zip(collected_data, kmeans.labels_):
        if label in RS_labels:
            RS.append(row)

    # Step 4. Run K-Means again to cluster the rest of the data points with K = the number of input clusters.

    collected_data = [row for row in collected_data if row not in RS]
    kmeans = KMeans(n_clusters=n_cluster, random_state=0).fit([row[2:] for row in collected_data])
    result = dict()
    for row, label in zip(collected_data, kmeans.labels_):
        result[int(row[0])] = label

    # Step 5. Use the K-Means result from Step 4 to generate the DS clusters (i.e., discard their points and generate statistics).
    c = sc.parallelize(collected_data, n) \
        .map(lambda x: x[2:]) \
        .map(lambda x: (kmeans.predict([x])[0], x)).cache()  ## [label, [a,b,c,d,e]], ....
    N = c.map(lambda x: (x[0], 1)).reduceByKey(lambda a, b: a + b).collect()
    SUM = c.reduceByKey(lambda a, b: [a[i] + b[i] for i in range(len(a))]).collect()
    SUMSQ = c.map(lambda x: (x[0], [feature ** 2 for feature in x[1]])) \
        .reduceByKey(lambda a, b: [a[i] + b[i] for i in range(len(a))]).collect()

    #     print("Duration : ", time.time()-t)

    # The initialization of DS has finished, so far, you have K numbers of DS clusters (from Step 5) and some numbers of RS (from Step 3).
    # Step 6. Run K-Means on the points in the RS with a large K (e.g., 5 times of the number of the input clusters) to generate CS (clusters with more than one points) and RS (clusters with only one point).
    CS = dict()
    try:
        kmeans = KMeans(n_clusters=n_cluster * 2, random_state=0).fit([row[2:] for row in RS])  ## 0 : id, 1 : label
    except:
        kmeans = KMeans(n_clusters=len(RS), random_state=0).fit([row[2:] for row in RS])
    RS_new = []
    RS_labels = sc.parallelize(kmeans.labels_, n) \
        .map(lambda x: (x, 1)) \
        .reduceByKey(lambda a, b: a + b) \
        .filter(lambda x: x[1] == 1) \
        .map(lambda x: x[0]).collect()
    CS_count = 0
    RS_count = 0
    for row, label in zip(RS, kmeans.labels_):
        if label in RS_labels:
            RS_count += 1
            RS_new.append(row)
        if label not in RS_labels:
            CS_count += 1
            CS[label + Round * 10000] = CS.get(label + Round * 10000, []) + [row]
    RS = RS_new
    #####
    N_CS, SUM_CS, SUMSQ_CS = N_SUM_SSQ(CS)

    file = open(output_file, 'w')
    file.write("The intermediate results:\n")
    file.write("Round {}: ".format(1) + str(sum([cluster[1] for cluster in N])) + "," + str(len(CS)) + "," + str(
        CS_count) + "," + str(RS_count) + '\n')

    ## number of DS points, number of CS set, number of CS points, number of RS points



    for Round in [2, 3, 4, 5]:
        #     Round = 1
        # Step 7. Load another 20% of the data randomly.
        random_data_round = random_data.filter(lambda x: x[1] == Round - 1).map(lambda x: x[0])
        # print("num_round : ", len(random_data_round.collect()))

        # Step 8. For the new points, compare them to each of the DS using the Mahalanobis Distance and assign
        # them to the nearest DSclusters if the distance is <2 root 𝑑.
        # d = root(sigma( (x - SUM/N)/((SUMSQ/N - (SUM/N)^2) **0.5) ))

        base = 2 * ((10 ** 0.5))
        filtered_data = random_data_round \
            .map(lambda x: [x, Mahalanobis_Distance(x, N, SUM, SUMSQ)]) \
            .map(lambda x: [x[0], min(x[1].items(), key=lambda y: y[1])]) \
            .map(lambda x: [x[0], x[1][0]] if x[1][1] < base else [x[0], "not DS"]).cache()
        for row, label in filtered_data.collect():
            if label != "not DS":
                result[int(row[0])] = label
        N_new = filtered_data.map(lambda x: (x[1], 1)).reduceByKey(lambda a, b: a + b).collect()
        SUM_new = filtered_data.map(lambda x: (x[1], x[0][2:])) \
            .reduceByKey(lambda a, b: [a[i] + b[i] for i in range(len(a))]).collect()
        SUMSQ_new = filtered_data.map(lambda x: (x[1], [feature ** 2 for feature in x[0][2:]])) \
            .reduceByKey(lambda a, b: [a[i] + b[i] for i in range(len(a))]).collect()

        tmp_N_dict = dict()
        tmp_SUM_dict = dict()
        tmp_SUMSQ_dict = dict()
        for key in dict(N).keys():
            tmp_N_dict[key] = dict(N)[key] + dict(N_new)[key]
            tmp_SUM_dict[key] = [dict(SUM)[key][i] + dict(SUM_new)[key][i] for i in range(dim)]
            tmp_SUMSQ_dict[key] = [dict(SUMSQ)[key][i] + dict(SUMSQ_new)[key][i] for i in range(dim)]

        N = list(tmp_N_dict.items())
        SUM = list(tmp_SUM_dict.items())
        SUMSQ = list(tmp_SUMSQ_dict.items())

        # Step 9. For the new points that are not assigned to DS clusters, using the Mahalanobis Distance and
        # assign the points to the nearest CS clusters if the distance is <2 root 𝑑
        NOT_DS = filtered_data.filter(lambda x: x[1] == "not DS").map(lambda x: x[0]) \
            .map(lambda x: [x, Mahalanobis_Distance(x, N_CS, SUM_CS, SUMSQ_CS)]) \
            .map(lambda x: [x[0], min(x[1].items(), key=lambda y: y[1])] if len(N_CS) > 0 else [x[0], (0, base + 1)]) \
            .map(lambda x: [x[0], x[1][0]] if x[1][1] < base else [x[0], "not CS"]).cache()

        ## add rows to CS
        for row in NOT_DS.collect():
            if row[1] != "not CS":
                CS[row[1]] = CS.get(row[1], []) + [row[0]]

        N_CS_new = NOT_DS.map(lambda x: (x[1], 1)).reduceByKey(lambda a, b: a + b).collect()
        SUM_CS_new = NOT_DS.map(lambda x: (x[1], x[0][2:])) \
            .reduceByKey(lambda a, b: [a[i] + b[i] for i in range(len(a))]).collect()
        SUMSQ_CS_new = NOT_DS.map(lambda x: (x[1], [feature ** 2 for feature in x[0][2:]])) \
            .reduceByKey(lambda a, b: [a[i] + b[i] for i in range(len(a))]).collect()

        tmp_N_dict = dict()
        tmp_SUM_dict = dict()
        tmp_SUMSQ_dict = dict()
        for key in dict(N_CS).keys():
            tmp_N_dict[key] = dict(N_CS)[key] + dict(N_CS_new).get(key, 0)
            tmp_SUM_dict[key] = [dict(SUM_CS)[key][i] + dict(SUM_CS_new).get(key, [0 for idx in range(dim)])[i] for i in
                                 range(dim)]
            tmp_SUMSQ_dict[key] = [dict(SUMSQ_CS)[key][i] + dict(SUMSQ_CS_new).get(key, [0 for idx in range(dim)])[i]
                                   for i in range(dim)]

        N_CS = list(tmp_N_dict.items())
        SUM_CS = list(tmp_SUM_dict.items())
        SUMSQ_CS = list(tmp_SUMSQ_dict.items())

        # Step 10. For the new points that are not assigned to a DS cluster or a CS cluster, assign them to RS.
        NOT_CS = NOT_DS.filter(lambda x: x[1] == "not CS").map(lambda x: x[0]).collect()
        RS = RS + NOT_CS

        # Step 11. Run K-Means on the RS with a large K (e.g., 5 times of the number of the input clusters) to generate CS (clusters with more than one points) and RS (clusters with only one point).
        try:
            kmeans = KMeans(n_clusters=n_cluster * 2, random_state=0).fit([row[2:] for row in RS])  ## 0 : id, 1 : label
        except:
            kmeans = KMeans(n_clusters=len(RS), random_state=0).fit([row[2:] for row in RS])

        RS_labels = sc.parallelize(kmeans.labels_, n) \
            .map(lambda x: (x, 1)) \
            .reduceByKey(lambda a, b: a + b) \
            .filter(lambda x: x[1] == 1) \
            .map(lambda x: x[0]).collect()
        CS_count = 0
        RS_count = 0

        rm_row = []
        for row, label in zip(RS, kmeans.labels_):
            if label in RS_labels:
                RS_count += 1

            if label not in RS_labels:
                CS_count += 1
                CS[label + Round * 10000] = CS.get(label + Round * 10000, []) + [row]
                rm_row.append(row)

        for row in rm_row:
            RS.remove(row)

        N_CS, SUM_CS, SUMSQ_CS = N_SUM_SSQ(CS)

        # Step 12.Merge CS clusters that have a Mahalanobis Distance <2 root 𝑑.
        New_N_CS_len = 10 * 10
        while len(N_CS) != New_N_CS_len and New_N_CS_len > 1:
            N_CS, SUM_CS, SUMSQ_CS = N_SUM_SSQ(CS)
            centroids_CS = dict()
            for cluster_id in CS:
                #         print(dict(N_CS)[cluster_id])
                tmp = []
                for idx in range(dim):
                    tmp.append(dict(SUM_CS)[cluster_id][idx] / dict(N_CS)[cluster_id])
                centroids_CS[cluster_id] = tmp

            tmp = 0
            for cluster_id in centroids_CS:
                if tmp == 0:
                    m_dist = Mahalanobis_Distance_btw_two_clusters(cluster_id, N_CS, SUM_CS, SUMSQ_CS, dim)
                    del m_dist[cluster_id]
                    closest_cluster, mindist = min(m_dist.items(), key=lambda y: y[1])
                    if mindist < base:
                        tmp += 1
                        CS[cluster_id] = CS[cluster_id] + CS[closest_cluster]
                        del CS[closest_cluster]
            New_N_CS_len = len(CS)

        N_CS, SUM_CS, SUMSQ_CS = N_SUM_SSQ(CS)

        ## Step 13. merge CS with DS that have a Mahalanobis Distance <2 root d
        if Round == 5:
            del_cluster = []
            for cluster in CS:
                dist_to_DS = Mahalanobis_Distance_btw_CS_DS(cluster, N, SUM, SUMSQ, dim, N_CS, SUM_CS, SUMSQ_CS)
                closest_DS, mindist = min(dist_to_DS.items(), key=lambda y: y[1])

                if mindist < base:
                    del_cluster.append(cluster)
                    tmp_N_dict = dict(N)
                    tmp_SUM_dict = dict(SUM)
                    tmp_SUMSQ_dict = dict(SUMSQ)
                    tmp_N_dict[closest_DS] = tmp_N_dict[closest_DS] + dict(N_CS)[cluster]
                    tmp_SUM_dict[closest_DS] = tmp_SUM_dict[closest_DS] + dict(SUM_CS)[cluster]
                    tmp_SUMSQ_dict[closest_DS] = tmp_SUMSQ_dict[closest_DS] + dict(SUMSQ_CS)[cluster]

                    N = list(tmp_N_dict.items())
                    SUM = list(tmp_SUM_dict.items())
                    SUMSQ = list(tmp_SUMSQ_dict.items())

                    for row in CS[cluster]:
                        result[int(row[0])] = closest_DS

            for cluster in del_cluster:
                del CS[cluster]

        N_CS, SUM_CS, SUMSQ_CS = N_SUM_SSQ(CS)
        file.write(
            "Round {}: ".format(Round) + str(sum([cluster[1] for cluster in N])) + "," + str(len(CS)) + "," + str(
                sum(list(dict(N_CS).values()))) + "," + str(len(RS)) + "\n")

        # print("Round {}:".format(Round), sum([cluster[1] for cluster in N]), ",", len(CS), ",",
        #       sum(list(dict(N_CS).values())), ",", len(RS))
    #         print("num_res : ",  len(result)+sum(list(dict(N_CS).values()))+len(RS))
    #     print("Round {}: ".format(Round), len(result), "\n")
        for row in RS:
            result[int(row[0])] = -1
        for key in CS:
            for row in CS[key]:
                result[int(row[0])] = -1
        # print("Round {}: ".format(Round), len(result), "\n")

    file.write("\n")
    file.write("The clustering results:\n")
    for idx in range(len(result)):
        file.write(str(idx) + "," + str(result[idx]) + "\n")
    file.close()
    print("Duration : ", time.time() - t)