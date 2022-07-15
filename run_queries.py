from time import time

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


# follows  friendOf  likes  hasReview
# wsdbm:follows  wsdbm:friendOf  wsdbm:likes  rev:hasReview
def run_query_hash_join():
    join_multiple_tables([tables['wsdbm:follows'], tables['wsdbm:friendOf'], tables['wsdbm:likes'], tables['rev:hasReview']], join=hash_join)


def run_query_sort_merge_join():
    join_multiple_tables([tables['wsdbm:follows'], tables['wsdbm:friendOf'], tables['wsdbm:likes'], tables['rev:hasReview']], join=sort_merge_join)


def run_query_skip_sort_merge_join():
    join_multiple_tables([tables['wsdbm:follows'], tables['wsdbm:friendOf'], tables['wsdbm:likes'], tables['rev:hasReview']], join=skip_sort_merge_join)



def run_query_parallel_sort_merge_join():
    join_multiple_tables([tables['wsdbm:follows'], tables['wsdbm:friendOf'], tables['wsdbm:likes'], tables['rev:hasReview']], join=parallel_sort_merge_join)


def run_query_parallel_hash_join():
    join_multiple_tables([tables['wsdbm:follows'], tables['wsdbm:friendOf'], tables['wsdbm:likes'], tables['rev:hasReview']], join=parallel_hash_join)
