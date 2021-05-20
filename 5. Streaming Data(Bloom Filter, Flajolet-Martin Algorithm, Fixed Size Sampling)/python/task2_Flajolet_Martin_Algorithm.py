from blackbox import BlackBox
import sys
import binascii
import time

import random

random.seed(553)
num_hash_ftn = 100

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


# def hash_ftn(x,i):
#     return (prime_list[i]*x + prime_list[-i])%prime_list[2*i+1]

def myhashs(user):
    result = []
    s = int(binascii.hexlify(user.encode("utf8")), 16)
    for f in hash_function_list:
        result.append(f(s))
    return result


def num_consecutive_0(x):
    '''
    count consecutive 0s' in x(binary form)
    '''
    one = False
    count = 0
    i = 1
    while one == False:
        if x[-i] == '0':
            count += 1
        elif x[-i] == '1':
            one = True
        if x[-i] == 'b':
            one = True
        i += 1
    return count


def median(l):
    sorted_l = sorted(l)
    length = len(l)

    if length % 2 == 0:
        b = sorted_l[int(length / 2)]
        c = sorted_l[int(length / 2) - 1]
        d = (b + c) / 2
        return d

    else:
        return sorted_l[int(length / 2)]


def myftn(stream_users):
    L = [0 for j in range(num_hash_ftn)]  ## 100

    for idx, user in enumerate(stream_users):


        hash_vals = myhashs(user)  ##[hash1(num), hash2(num), ...]
        #         print(hash_vals)
        r_val = [num_consecutive_0(bin(x)) for x in hash_vals]  ##[7,9,8,6,7,8,...]

        for hash_ftn_num, val in enumerate(r_val):
            if L[hash_ftn_num] < val:
                L[hash_ftn_num] = val
    #     print(L)

    small_groups = [[] for i in range(20)]
    for idx, val in enumerate(L):
        small_groups[idx % 20] += [2 ** val]
    #     print(small_groups)
    small_groups = [sum(x) / len(x) for x in small_groups]
    return median(small_groups)


if __name__ == '__main__':
    random.seed(553)
    t = time.time()
    Estimation = []

    input_filename = sys.argv[1]
    stream_size = int(sys.argv[2])
    num_of_asks = int(sys.argv[3])
    output_filename = sys.argv[4]

    hash_function_list = [(lambda y: (lambda x: (prime_list[y] * x + prime_list[-y]) % prime_list[2 * y + 1]))(i) for i
                          in range(num_hash_ftn)]
    bx = BlackBox()

    for _ in range(num_of_asks):
        stream_users = bx.ask(input_filename, stream_size)
        est = myftn(stream_users)
        Estimation.append(est)

    with open(output_filename, 'w') as file:
        file.write("Time,Ground Truth,Estimation\n")
        for idx, est in enumerate(Estimation):
            file.write(str(idx) + "," + str(stream_size) + "," + str(int(est)))
            file.write("\n")

        file.close()

    print("Duration : ", time.time() - t)