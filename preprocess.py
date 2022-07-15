from concurrent.futures import process
import csv
from tqdm import tqdm
from time import time
from multiprocessing import Pool

tables = dict()

def load_tables(justTesting=True):
    if justTesting:
        input_file = "100k.txt"
    else:
        input_file = "watdiv.10M.nt"
    with open(input_file) as f:
        lines = []
        for line in f.readlines():
            line = line.strip('\n. ')
            lines.append(line)
        
        # Maybe there are tabs in the quoted parts therefore I use the csv.reader instead of line.split('\t')
        reader = csv.reader(lines, delimiter='\t')

        # print(len(lines))

        for triple in tqdm(reader, total=len(lines), desc='load_tables'):
            subject = triple[0]
            relation =  triple[1]
            object = triple[2]

            tables[relation] = tables.get(relation, list()) + [(subject, object)]


def hash_join(table1, table2):
    result = []

    # skip partitioning
    hash_dict = dict()
    # later it's necessary to accept bigger tables too therefore maybe join_attr and other_attributes
    for row in table1:
        # object of table1 is join key so is subject of table2
        # print("Hallo", hash_dict.get(row[-1], list()))
        # print(row[:-1])
        hash_dict[row[-1]] = hash_dict.get(row[-1], list()) + [row[:-1]]
    

    for row_table2 in table2:
        if hash_dict.get(row_table2[0]) is not None:
            for row_table1 in hash_dict[row_table2[0]]:
                result.append((*row_table1, *row_table2))
    return result


def parallel_hash_join_helper(hash_dict, row_table2):
    part_result = []
    if hash_dict.get(row_table2[0]) is not None:
        for row_table1 in hash_dict[row_table2[0]]:
            part_result.append((*row_table1, *row_table2))
        return part_result


def parallel_hash_join(table1, table2):
    # skip partitioning
    hash_dict = dict()
    # later it's necessary to accept bigger tables too therefore maybe join_attr and other_attributes
    for row in table1:
        # object of table1 is join key so is subject of table2
        # print("Hallo", hash_dict.get(row[-1], list()))
        # print(row[:-1])
        hash_dict[row[-1]] = hash_dict.get(row[-1], list()) + [row[:-1]]
    

    # for row_table2 in table2:
    result = []
    with Pool() as pool:
        for row in pool.starmap(parallel_hash_join_helper, [(hash_dict, row) for row in table2]):
            if row is not None:
                result.extend(row)
        
    return result


def skip_sort_merge_join(table1, table2):
    return sort_merge_join(table1, table2, tryskipping=True)


# TODO not tested yet
def sort_merge_join(table1, table2, sort=True, tryskipping=False):
    result = []
    if sort:
        table1 = sorted(table1, key=lambda x: x[-1])
        table2 = sorted(table2, key=lambda x: x[0])

    # compare entries in both tables
    # if entry in table1 smaller than entry of table2
    # take next entry of table1 one
    # if equal add to result
    # else next entry of table2

    table1_position = 0
    table2_position = 0
    length_table1 = len(table1)
    length_table2 = len(table2)
    while not table1_position >= length_table1 and not table2_position >= length_table2:
        join_attr_table1 = table1[table1_position][-1]
        join_attr_table2 = table2[table2_position][0]
        if join_attr_table1 == join_attr_table2:
            result.append((*table1[table1_position][:], *table2[table2_position][1:]))
            table2_position_temporary = table2_position + 1
            # there might be multiple equal results for each entry in table1
            while table2_position_temporary < len(table2) and join_attr_table1 == table2[table2_position_temporary][0]:
                result.append((*table1[table1_position][:], *table2[table2_position_temporary][1:]))
                table2_position_temporary += 1
            table1_position += 1
        elif join_attr_table1 < join_attr_table2:
            table1_position += 1
            if tryskipping:
                possible_new_position = table1_position + length_table1 // 1000
                if possible_new_position < length_table1 and table1[possible_new_position][-1] < join_attr_table2:
                    table1_position = possible_new_position
        else:
            table2_position += 1
            if tryskipping:
                possible_new_position = table2_position + length_table2 // 1000
                if possible_new_position < length_table2 and table2[possible_new_position][0] < join_attr_table1:
                    table2_position = possible_new_position

    return result



def parallel_sort_merge_join(table1, table2):
    table1 = sorted(table1, key=lambda x: x[-1])
    table2 = sorted(table2, key=lambda x: x[0])

    split = 10

    print(len(table1))

    time_build_tables = time()

    table1_lst = []
    if len(table1) >= split:
        split_size = len(table1) // split
        for i in range(split - 1):
            temp_table = table1[i*split_size:(i+1)*split_size]
            print(len(temp_table))
            table1_lst.append(temp_table)
        table1_lst.append(table1[(split-1)*split_size:])  # in this bin there might be up to split - 1 more entries
    else:
        table1_lst.append(table1)
    
    table2_lst = []
    if len(table2) >= split:
        split_size = len(table2) // split
        for i in range(split - 1):
            table2_lst.append(table2[i*split_size:(i+1)*split_size])
        table2_lst.append(table2[(split-1)*split_size:])  # in this bin there might be up to split - 1 more entries
    else:
        table2_lst.append(table2)
    
    multi_processing_table = []
    for table1_part in table1_lst:
        for table2_part in table2_lst:
            if table1_part[0][-1] > table2_part[-1][0] or table1_part[-1][-1] < table2_part[0][0]:
                continue  # don't append if there is no overlap
            multi_processing_table.append((table1_part, table2_part, False))
    if multi_processing_table == []:
        return []


    print(f"It takes {time() - time_build_tables :.2f} seconds to build the multiprocessing_table")

    with Pool(processes=8) as pool:  # split + 1
        time_starmapping = time()
        intermediate_result = pool.starmap(sort_merge_join, multi_processing_table)
        print(f"Star mapping takes {time() - time_starmapping :.2f}")

        result = []
        # TODO test how long each part of the function takes
        start_time = time()
        for part_result in intermediate_result:
            result.extend(part_result)  # += part_result
        print(f"Add results together takes {time() - start_time :.2f} seconds")
        return result
        




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
    
    assert set(hash_join(tables['wsdbm:follows'], tables['wsdbm:friendOf'])) == set(sort_merge_join(tables['wsdbm:follows'], tables['wsdbm:friendOf'], tryskipping=True)),\
        "the two sorting algorithms should return equivalent results"


    # assert set(hash_join(tables['wsdbm:follows'], tables['wsdbm:friendOf'])) == set(sort_merge_join(tables['wsdbm:follows'], tables['wsdbm:friendOf'])),\
    #     "the two sorting algorithms should return equivalent results"


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


def time_joins():
    # start_time = time()
    # run_query_hash_join()
    # print("Hash join query needed:", f"{(time() - start_time):.2f}", "seconds to run the procedure")
    start_time = time()
    run_query_skip_sort_merge_join()
    print("Sort merge join query needed:", f"{(time() - start_time):.2f}", "seconds to run the procedure")
    # start_time = time()
    # run_query_parallel_hash_join()
    # print("Parallel sort merge join query needed:", f"{(time() - start_time):.2f}", "seconds to run the procedure")
    start_time = time()
    run_query_parallel_sort_merge_join()
    print("Parallel sort merge join query needed:", f"{(time() - start_time):.2f}", "seconds to run the procedure")
    


if __name__ == "__main__":
    load_tables()
    tests()
    time_joins()
