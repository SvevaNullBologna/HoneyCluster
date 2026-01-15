import os
from Zenodo.ZenodoCleaner import clean_zenodo_dataset
from Zenodo.ZenodoProcesser import process_dataset
from MachineLearning.HoneyClustering import clustering
from MachineLearning.DataDistributionObserver import analizing
from HoneyCluster import HoneyClusterPaths
from pathlib import Path


def _print_menu():
    print("Choose the operation you want to perform by entering the preferred number")
    print("1 = set base folder path")
    print("2 = clean your zenodo gz files (do not decompress please)")
    print("3 = process your cleaned files, that means, prepare them for clustering")
    print("4 = cluster you processed files ")
    print("5 = see the graphs resulting from the clustering process")
    print("6 = abort operation")
    print("Remember: you can stop and continue the cleaning and processing whenever you want\njust remember to erase (JUST) the last modified file")
    print("\n\nenter your number:")

def _ask_number()-> int :
    while True:
        _print_menu()
        number = input()
        try:
            number = int(number)
        except ValueError:
            print("please enter a valid number")
            continue
        if number in range(1, 7):
                return number
        else:
            print("invalid selection. Please, try again")

def _ask_path() -> Path | None :
    while True:
        print("Please, enter the complete dataset folder path. It has to contain a folder called originals, where you put your zenodo gz files. Do not decompress!")
        print("example: zenodo_dataset -> originals -> [all gz files]")
        print("type: ESC to get back to menu")
        typed_input = input().strip()
        if typed_input == "ESC":
            return None
        input_path = Path(typed_input)
        if not input_path :
            print("empty path. Please re-try")
            continue

        elif not os.path.isdir(input_path):
            print(f"the path {input_path} is not a directory. Please re-try")
            continue

        originals_path = Path(input_path, "originals")
        if not originals_path.is_dir() :
            print("no original folder. Please re-try")
            continue

        is_empty = not any(originals_path.rglob('*.json.gz*'))
        if is_empty:
            print(f"no zenodo json.gzip files in {originals_path}. Please add them and re-try")
            continue

        return input_path

def set_base_folderpath(input_path: Path) -> HoneyClusterPaths:
    return HoneyClusterPaths(input_path)

def cleaning(paths : HoneyClusterPaths | None):
    if paths is None :
        print("set base folder path first!")
        return
    clean_zenodo_dataset(paths)

def processing(paths : HoneyClusterPaths | None):
    if paths is None:
        print("set base folder path first!")
        return
    process_dataset(paths)

def compute_clustering(paths : HoneyClusterPaths | None):
    if paths is None :
        print("set base folder path first!")
        return
    clustering(paths)

def analysis(paths: HoneyClusterPaths | None):
    if paths is None :
        print("set base folder path first!")
        return
    analizing(paths)

if __name__ == "__main__":
    important_paths = None
    while True:
        number = _ask_number()
        if number == 1:
            path_selected = _ask_path()
            if path_selected:
                important_paths = set_base_folderpath(path_selected)
        elif number == 2:
            cleaning(important_paths)
        elif number == 3:
            processing(important_paths)
        elif number == 4:
            compute_clustering(important_paths)
        elif number == 5:
            analysis(important_paths)
        elif number == 6:
            print("aborted operation.")
            break
        else :
            print("invalid number. Please re-try")