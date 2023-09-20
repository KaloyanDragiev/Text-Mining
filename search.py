import json
import os
from contextlib import nullcontext

from tqdm import tqdm
from cleanup import load_json

def apply(path, mapper, reducer=None):
    result = None
    for filename in tqdm(os.listdir(path)):
        data = load_json(f'{path}/{filename}', False)
        mapped = mapper(data, filename)
        if reducer:
            result = reducer(result, mapped)

    return result


def get_search_mapper(keywords, save_articles=False, path='./data', minimum_count=1):
    def mapper(data, filename):
        # For each article in the data, count how often the word "music" occurs
        # Keep track of how often each count occurs
        results = {keyword: {} for keyword in keywords}
        results['combined'] = {}

        if save_articles:
            if not os.path.exists(path):
                os.makedirs(path)

        with open(f'{path}/{filename}', 'w') if save_articles else nullcontext() as f:
            for article in data:
                combined_count = 0
                for keyword in keywords:
                    if '.\nDevelopment.\n' not in article['text']:
                        continue
                    count = article['text'].split('.\nDevelopment.\n')[0].count(keyword)
                    combined_count += count
                    if count in results[keyword]:
                        results[keyword][count] += 1
                    else:
                        results[keyword][count] = 1

                if combined_count in results['combined']:
                    results['combined'][combined_count] += 1
                else:
                    results['combined'][combined_count] = 1

                if combined_count >= minimum_count and save_articles:
                    f.write(json.dumps(article) + '\n')

        return results
    return mapper


def get_search_reducer(keywords):
    keywords = keywords.copy()
    keywords.append('combined')

    def reducer(result, mapped):
        # Add the counts from the mapped data to the counts from the result
        if result:
            for keyword in keywords:
                for key, value in mapped[keyword].items():
                    if key in result[keyword]:
                        result[keyword][key] += value
                    else:
                        result[keyword][key] = value
        else:
            result = mapped

        return result
    return reducer


def main(path, keywords, save_articles, save_path, minimum_count=1):
    print(f'Keywords: {", ".join(keywords)}')
    result = apply(path, get_search_mapper(keywords, save_articles, save_path, minimum_count),
                   get_search_reducer(keywords))
    print(result)

    print(
        f'Total number of articles: {sum([value for value in result[keywords[0]].values()])}')
    for keyword in keywords:
        print(f'\nKeyword: {keyword}')
        print(
            f'Number of articles with "{keyword}" in them: {sum([value if key >= minimum_count else 0 for key, value in result[keyword].items()])}')
        if sum([value if key >= minimum_count else 0 for key, value in result[keyword].items()]) > 0:
            print(
                f'Average number of occurrences of "{keyword}" in articles that contain it: {sum([key * value if key >= minimum_count else 0 for key, value in result[keyword].items()]) / sum([value if key >= minimum_count else 0 for key, value in result[keyword].items()])}')

    print("\nCombined:")
    print(
        f'Number of articles with any keyword in them: {sum([value if key >= minimum_count else 0 for key, value in result["combined"].items()])}')
    if sum([value if key >= minimum_count else 0 for key, value in result["combined"].items()]) > 0:
        print(
            f'Average number of occurrences of keywords in articles that contain any: {sum([key * value if key >= minimum_count else 0 for key, value in result["combined"].items()]) / sum([value if key >= minimum_count else 0 for key, value in result["combined"].items()])}')


if __name__ == '__main__':
    # Change these variables to change the behavior of the program
    # The path to the directory containing the data
    path = "./data/AA_non-empty_ftfy"
    # The keywords to search for
    keywords = ["airplane", "airliner", "aircraft"]
    # Whether to save the articles that contain the keywords
    save_articles = True
    # The minimum number of occurrences of a keyword in an article for it to be counted
    minimum_count = 1
    # The path to the directory to save the articles in
    save_path = f'./data/Filtered_{minimum_count}_AA{"-".join(keywords)}'

    main(path, keywords, save_articles, save_path, minimum_count)

    path = "./data/AB_non-empty_ftfy"
    save_path = f'./data/Filtered_1_AB{"-".join(keywords)}'

    main(path, keywords, save_articles, save_path, minimum_count)
