from pathlib import Path


_base_folder = Path()
def _check_base_folder():
    return _base_folder.exists() and _base_folder.is_dir()

def _print_menu():
    print("Choose the operation you want to perform by entering the preferred number")
    print("1 = set base folder path")
    print("2 = clean your zenodo gz files (do not decompress please)")
    print("3 = process your cleaned files, that means, prepare them for clustering")
    print("4 = cluster you processed files ")
    print("5 = see the graphs resulting from the clustering process")
    print("6 = abort operation")
    print("Remember: you can stop and continue the cleaning and processing whenever you want\njust remember to erase the last created file")
    print("\n\nenter your number:")


def _choose_operation(value: int):
    if value == 1:
        set_base_folderpath()
    elif value == 2:
        cleaning(_base_folder)
    elif value == 3:
        processing(_base_folder)
    elif value == 4:
        clustering(_base_folder)
    elif value == 5:
        analysis(_base_folder)
    elif value == 6:
        print("aborted operation.")
        return

def set_base_folderpath(path: str | Path = None):
    pass

def cleaning(path: Path) -> None:
    pass

def processing(path: Path) -> None:
    pass

def clustering(path: Path) -> None:
    pass

def analysis(path: Path) -> None:
    pass

def main(str):
    pass

if __name__ == "__main__":
    got_valid_number = False
    while not got_valid_number :
        _print_menu()
        try:
            number = int(input())
            if number in range(1,6):
                got_valid_number = True
        except ValueError:
            continue
