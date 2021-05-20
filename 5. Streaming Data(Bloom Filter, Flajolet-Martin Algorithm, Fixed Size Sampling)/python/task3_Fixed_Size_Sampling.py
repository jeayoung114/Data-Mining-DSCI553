from blackbox import BlackBox
import sys
import time

import random

random.seed(553)

if __name__ == '__main__':

    input_filename = sys.argv[1]
    stream_size = int(sys.argv[2])
    num_of_asks = int(sys.argv[3])
    output_filename = sys.argv[4]


    random.seed(553)
    t = time.time()
    mem = []
    bx = BlackBox()
    count = 0
    with open(output_filename, 'w') as file:
        file.write("seqnum,0_id,20_id,40_id,80_id\n")
        for _ in range(num_of_asks):
            stream_users = bx.ask(input_filename, stream_size)
            if stream_size * (_ + 1) <= 100:
                print("Case 1")
                for idx, user in enumerate(stream_users):
                    mem.append(user)
                    count += 1

            if stream_size * (_ + 1) > 100:
                idx = 0
                while len(mem) < 100:
                    print("Case 2")
                    mem.append(stream_users[idx])
                    idx += 1
                    count += 1

                while idx < stream_size:
                    count += 1
                    a = random.random()
                    if a > 100/count:
                        pass
                    elif a <= 100/count:
                        b = random.randint(0, 99)
                        mem[b] = stream_users[idx]

                    idx += 1
            seqnum = stream_size * (_ + 1)
            id_0 = mem[0]
            id_20 = mem[20]
            id_40 = mem[40]
            id_60 = mem[60]
            id_80 = mem[80]
            file.write(str(seqnum) + "," + id_0 + "," + id_20 + "," + id_40 + "," + id_60 + "," + id_80 + '\n')

    file.close

    print("Duration : ", time.time() - t)
