import os
from Zenodo.ZenodoCleaner import extract_and_clean_all_zenodo_logs_in_folder
from Zenodo.ZenodoProcesser import process_cleaned_dataset
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
            continue
        if number in range(1, 6):
                return number

def _ask_path() -> Path :
    while True:
        print("Please, enter the complete dataset folder path. It has to contain a folder called originals, where you put your zenodo gz files. Do not decompress!")
        print("example: zenodo_dataset -> originals -> [all gz files]")
        input_path = Path(input())
        if not input_path :
            print("empty path. Please re-try")
        elif not os.path.isdir(path):
            print("the path is not a directory. Please re-try")
        originals_path = Path(input_path, "originals")
        if not os.path.isdir(originals_path) :
            print("no original folder. Please re-try")
        is_empty = not any(originals_path.rglob('*.json.gzip'))
        if is_empty:
            print(f"no zenodo json.gzip files in {originals_path}. Please add them and re-try")
        return path

def set_base_folderpath(input_path: Path):
    HoneyClusterPaths(input_path)

def cleaning(paths : HoneyClusterPaths) -> None:
    extract_and_clean_all_zenodo_logs_in_folder(originals_path=paths.base_folder, cleaned_path=paths.cleaned_folder)

def processing(paths : HoneyClusterPaths) -> None:
    process_cleaned_dataset(base_folder_path= paths.cleaned_folder)

def clustering() -> None:
    pass

def analysis() -> None:
    pass

def main(str):
    pass

if __name__ == "__main__":
    while True:
        number = _ask_number()
        if number == 1:
            path = _ask_path()
            set_base_folderpath(path)
        elif number == 2:
            cleaning()
        elif number == 3:
            processing()
        elif number == 4:
            clustering()
        elif number == 5:
            analysis()
        elif number == 6:
            print("aborted operation.")
            break