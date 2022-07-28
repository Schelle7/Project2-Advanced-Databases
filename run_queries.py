from time import time, perf_counter
from hash_join import *
from sort_merge_join import *
from load_tables import load_tables

tables = load_tables(justTesting=False)


def join_multiple_tables(lst_tables, join=hash_join):
    # lst_tables has to be exactly that, a list of tables with at least one table
    result = lst_tables[0]
    for table in lst_tables[1:]:
        # print("len(result)", len(result))
        # print("len(table)", len(table))
        start_time = time()
        result = join(result, table)
        print(f"Join needed {time() - start_time :.2f}")
    return result

def run_query_hash_join():
    return join_multiple_tables([tables['follows'], tables['friendOf'], tables['likes'], tables['hasReview']], join=hash_join)


def run_query_sort_merge_join(split_up=False):
    if split_up:
        run_query_split_up(join=sort_merge_join)
    else:
        return join_multiple_tables([tables['follows'], tables['friendOf'], tables['likes'], tables['hasReview']], join=sort_merge_join)



def run_query_skip_sort_merge_join(split_up=False):
    if split_up:
        run_query_split_up(join=sort_merge_join)
    else:
        return join_multiple_tables([tables['follows'], tables['friendOf'], tables['likes'], tables['hasReview']], join=skip_sort_merge_join)



def run_query_parallel_sort_merge_join(split_up=False):
    if split_up:
        run_query_split_up(join=sort_merge_join)
    else:
        join_multiple_tables([tables['follows'], tables['friendOf'], tables['likes'], tables['hasReview']], join=parallel_sort_merge_join)


def run_query_parallel_hash_join(split_up=False):
    if split_up:
        run_query_split_up(join=sort_merge_join)
    else:
        join_multiple_tables([tables['follows'], tables['friendOf'], tables['likes'], tables['hasReview']], join=parallel_hash_join)


def run_query_split_up(join=sort_merge_join):
    follows_lst = []
    chunk_size = len(tables['follows'])//100
    for i in range(0, len(tables['follows']), chunk_size):
        print("i, i+chunk_size", i, i+chunk_size)
        follows_lst.append(tables['follows'][i:i+chunk_size])
    for i, follows in enumerate(follows_lst):
        print(f"({i}/100)")
        join_multiple_tables([follows, tables['friendOf'], tables['likes'], tables['hasReview']], join=join)


if __name__ == '__main__':
    run_query_hash_join()