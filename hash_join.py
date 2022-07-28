from multiprocessing import Pool


def hash_join(table1, table2):
    result = []

    # skip partitioning
    hash_dict = dict()
    # later it's necessary to accept bigger tables too therefore maybe join_attr and other_attributes
    for row in table1:
        # object of table1 is join key so is subject of table2
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