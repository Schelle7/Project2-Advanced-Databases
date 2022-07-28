from time import time
from run_queries import *

def time_joins():
    # start_time = time()
    # run_query_hash_join()
    # print("Hash join query needed:", f"{(time() - start_time):.2f}", "seconds to run the procedure")
    
    start_time = time()
    result = run_query_sort_merge_join(split_up=True)
    print("Sort merge join query needed:", f"{(time() - start_time):.2f}", "seconds to run the procedure")
    # print('Write extract result to file')
    # result = result[:5000]
    # with open('sort_merge_join_result.txt', 'w') as f:
    #     f.write('')
    # with open('sort_merge_join_result.txt', 'a') as f:
    #     for line in result:
    #         f.write(','.join(line) + '\n')
    
    # start_time = time()
    # run_query_skip_sort_merge_join()
    # print("Sort merge join query needed:", f"{(time() - start_time):.2f}", "seconds to run the procedure")
    # start_time = time()
    # run_query_parallel_hash_join()
    # print("Parallel sort merge join query needed:", f"{(time() - start_time):.2f}", "seconds to run the procedure")
    # start_time = time()
    # run_query_parallel_sort_merge_join()
    # print("Parallel sort merge join query needed:", f"{(time() - start_time):.2f}", "seconds to run the procedure")

time_joins()