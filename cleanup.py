import json
import ftfy
import os
from tqdm import tqdm


def fix_encodings(path):
    print(f'Fixing encodings in {path}')

    # If the new directory does not exist, create it
    if not os.path.exists(f'{path}_ftfy'):
        os.makedirs(f'{path}_ftfy')

    # Loops through the files in the directory
    for filename in tqdm(os.listdir(path)):
        with open(f'{path}/{filename}', 'r') as f:
            with open(f'{path}_ftfy/{filename}', 'w') as f2:
                f2.write(ftfy.fix_encoding(f.read()))

    return f'{path}_ftfy'


def load_json(file, verbose=True):
    if verbose:
        print(f'Loading json in {file}')

    data = []
    with open(file, 'r') as f:
        for line in f:
            data.append(json.loads(line))
    return data


def load_json_by_path(path):
    print(f'Loading json in {path}')
    data = []
    for filename in tqdm(os.listdir(path)):
        data.append(load_json(f'{path}/{filename}'), False)
    return data


def remove_empty_articles(path):
    print(f'Removing empty articles in {path}')
    nr_empty = 0
    total = 0

    # If the new directory does not exist, create it
    if not os.path.exists(f'{path}_non-empty'):
        os.makedirs(f'{path}_non-empty')

    # Loops through the files in the directory
    for filename in tqdm(os.listdir(path)):
        data = load_json(f'{path}/{filename}', False)
        total += len(data)

        # Write the non-empty articles to the file
        with open(f'{path}_non-empty/{filename}', 'w') as f:
            for article in data:
                if article['text'] != '':
                    f.write(json.dumps(article) + '\n')
                else:
                    nr_empty += 1

    print(f'Total number of articles removed in {path}: {nr_empty}/{total} ({nr_empty/total*100:.2f}%)')

    return f'{path}_non-empty'


if __name__ == '__main__':
    path_AA = './data/AA'
    path_AB = './data/AB'

    # Make sure these directories exist and contain data
    if not os.path.exists(path_AA):
        raise Exception(f'{path_AA} does not exist')
    if not os.path.exists(path_AB):
        raise Exception(f'{path_AB} does not exist')

    path_AA = remove_empty_articles(path_AA)
    path_AB = remove_empty_articles(path_AB)

    path_AA = fix_encodings(path_AA)
    path_AB = fix_encodings(path_AB)

    data = load_json(f'{path_AA}/wiki_00')
    print(f'Number of non-empty articles in wiki_00: {len(data)}')
    print(f'First non-empty article in wiki_00:\n{data[0]}')
    print(f'Last non-empty article in wiki_00:\n{data[-1]}')
