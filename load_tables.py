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

        # lines = [lines[i] for i in range(len(lines)) if i%100 == 0]  # TODO change only for Testing
        
        # Maybe there are tabs in the quoted parts therefore I use the csv.reader instead of line.split('\t')
        reader = csv.reader(lines, delimiter='\t')

        # print(len(lines))

        for triple in tqdm(reader, total=len(lines), desc='load_tables'):
            subject = triple[0]
            relation =  triple[1]
            object = triple[2]

            friendOf = []
            likes = []
            follows = []
            hasReview = []

            if not justTesting and relation not in ['<http://db.uwaterloo.ca/~galuc/wsdbm/friendOf>', '<http://db.uwaterloo.ca/~galuc/wsdbm/likes>',\
                '<http://db.uwaterloo.ca/~galuc/wsdbm/follows>', '<http://purl.org/stuff/rev#hasReview>']:
                continue
            if justTesting and relation not in ['wsdbm:follows', 'wsdbm:friendOf', 'wsdbm:likes', 'rev:hasReview']:  # since we only want to join 4 tables the rest can be ignored
                continue

            '<http://db.uwaterloo.ca/~galuc/wsdbm/User58403>'
            if justTesting:
                relation = relation.split(':')[-1]
            else:
                relation = relation.replace('#', '/').strip('<>')
                relation =  relation.split('/')[-1]
                subject = subject.split('/')[-1].strip('<>')
                object = object.split('/')[-1].strip('<>')

            match relation:
                case "friendOf":
                    friendOf.append((subject, object))
                case "follows":
                    follows.append((subject, object))
                case "likes":
                    likes.append((subject, object))
                case "hasReview":
                    hasReview.append((subject, object))
        
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
    # save_10M_to_json()
    save_100k_to_json()
    # print("result.keys():", result.keys())
    # print("len(result) follows:", len(result['follows']))
    # print("len(result) friendOf:", len(result['friendOf']))
    # print("len(result) likes:", len(result['likes']))
    # print("len(result) hasReview:", len(result['hasReview']))
    # print("Extract follows:", result['follows'][:10])
    # print("Extract friendOf:", result['friendOf'][:10])
    # print("Extract likes:", result['likes'][:10])
    # print("Extract hasReview:", result['hasReview'][:10])
