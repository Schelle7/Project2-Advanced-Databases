from time import time
from multiprocessing import Pool

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