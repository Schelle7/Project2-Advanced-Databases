    object = ''
    lst = []
    length_table1 = len(table1)
    for i, row in enumerate(table1):
        if object == row[-1]:
            lst.append(row[:-1])
            if i+1 == length_table1:
                hash_dict[row[-1]] = lst
        else:
            if not i == 0:
                hash_dict[object] = lst
            object = row[-1]
            lst = [row[:-1]]
            if i+1 == length_table1:
                hash_dict[row[-1]] = lst
    print(list(hash_dict.items())[:10])

    