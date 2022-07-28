from multiprocessing import Pool
from load_tables import load_tables
from hash_join import *
from sort_merge_join import *
from run_queries import *

def tests():
    table1 = [('wsdbm:User0', 'wsdbm:User1'), ('wsdbm:User3', 'wsdbm:User1'), ('wsdbm:User0', 'wsdbm:User2'), ('wsdbm:User0', 'wsdbm:User3'),\
        ('wsdbm:User5', 'wsdbm:User6'), ('wsdbm:User3', 'wsdbm:User7'), ('wsdbm:User0', 'wsdbm:User9'), ('wsdbm:User8', 'wsdbm:User12'),\
            ('wsdbm:User0', 'wsdbm:User19'), ('wsdbm:User3', 'wsdbm:User81'), ('wsdbm:User0', 'wsdbm:User52'), ('wsdbm:User0', 'wsdbm:User93')]
    table2 = [('wsdbm:User0', 'wsdbm:User1'), ('wsdbm:User1', 'wsdbm:User5'), ('wsdbm:User2', 'wsdbm:2'), ('wsdbm:User2', 'wsdbm:User3'), ('wsdbm:User2', 'wsdbm:User7')]
    # print(parallel_sort_merge_join(table1, table2))

    # load_tables()
    # print(len(hash_join(tables['wsdbm:follows'], tables['wsdbm:friendOf'])))
    # print(len(parallel_hash_join(tables['wsdbm:follows'], tables['wsdbm:friendOf'])))
    # print(len(parallel_sort_merge_join(tables['wsdbm:follows'], tables['wsdbm:friendOf'])))

    # assert set(hash_join(tables['wsdbm:follows'], tables['wsdbm:friendOf'])) == set(parallel_hash_join(tables['wsdbm:follows'], tables['wsdbm:friendOf'])),\
    #     "the two sorting algorithms should return equivalent results"

    # assert set(hash_join(tables['wsdbm:follows'], tables['wsdbm:friendOf'])) == set(parallel_sort_merge_join(tables['wsdbm:follows'], tables['wsdbm:friendOf'])),\
    #     "the two sorting algorithms should return equivalent results"
    
    hash_join_result = run_query_hash_join()  # hash_join(tables['follows'], tables['friendOf'])
    sort_merge_join_result = run_query_sort_merge_join()  # sort_merge_join(tables['follows'], tables['friendOf'], tryskipping=True)

    assert set(hash_join_result) == set(sort_merge_join_result), "the two sorting algorithms should return equivalent results"
    assert len(hash_join_result) == len(sort_merge_join_result), "the two sorting algorithms should return results of equivalent length"
    assert len(hash_join_result) == len(set(hash_join_result)), "Set shouldn't loose length"
    # assert 


    # assert set(hash_join(tables['wsdbm:follows'], tables['wsdbm:friendOf'])) == set(sort_merge_join(tables['wsdbm:follows'], tables['wsdbm:friendOf'])),\
    #     "the two sorting algorithms should return equivalent results"


if __name__ == "__main__":
    tables = load_tables()
    tests()