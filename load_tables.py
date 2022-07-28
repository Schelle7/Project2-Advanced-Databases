import csv
from tqdm import tqdm
import json
import os.path


def load_tables(justTesting=True):
    tables = dict()

    if justTesting:
        input_file = "100k.txt"
        if os.path.exists('100k.json'):
            with open('100k.json') as f:
                return json.load(f)
    else:
        input_file = "watdiv.10M.nt"
        if os.path.exists('watdiv.10M.json'):
            with open('watdiv.10M.json') as f:
                return json.load(f)
    with open(input_file) as f:
        lines = []
        for line in f.readlines():
            line = line.strip('\n. ')
            lines.append(line)

        # lines = [lines[i] for i in range(len(lines)) if i%100 == 0]  for testing only
        
        # Maybe there are tabs in the quoted parts therefore I use the csv.reader instead of line.split('\t')
        reader = csv.reader(lines, delimiter='\t')
        
        friendOf = []
        likes = []
        follows = []
        hasReview = []

        for triple in tqdm(reader, total=len(lines), desc='load_tables'):
            subject = triple[0]
            relation =  triple[1]
            object = triple[2]

            if 'follows' in relation:
                follows.append((subject, object))
            elif 'friendOf' in relation:
                friendOf.append((subject, object))
            elif 'likes' in relation:
                likes.append((subject, object))
            elif 'hasReview' in relation:
                hasReview.append((subject, object))

    # print("len(friendOf)", len(friendOf))
    # print(len(follows))
    # print(len(likes))
    # print(len(hasReview))
    # print(len(friendOf)*len(follows)*len(hasReview)*len(likes))
    # raise Exception("Testing")



    tables["friendOf"] = friendOf
    tables["follows"] = follows
    tables["likes"] = likes
    tables["hasReview"] = hasReview

    return tables

def save_100k_to_json():
    result = load_tables(justTesting=True)
    with open('100k.json', 'w') as f:
        json.dump(result, f)

def save_10M_to_json():
    result = load_tables(justTesting=False)
    with open('watdiv.10M.json', 'w') as f:
        json.dump(result, f)
    

if __name__ == "__main__":
    save_10M_to_json()
    # save_100k_to_json()
    # print("result.keys():", result.keys())
    # print("len(result) follows:", len(result['follows']))
    # print("len(result) friendOf:", len(result['friendOf']))
    # print("len(result) likes:", len(result['likes']))
    # print("len(result) hasReview:", len(result['hasReview']))
    # print("Extract follows:", result['follows'][:10])
    # print("Extract friendOf:", result['friendOf'][:10])
    # print("Extract likes:", result['likes'][:10])
    # print("Extract hasReview:", result['hasReview'][:10])
