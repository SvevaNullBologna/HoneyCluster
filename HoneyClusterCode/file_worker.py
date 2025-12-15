import logging
import os
import gzip
import shutil

from pathlib import Path


from typing import Callable, TextIO



def add_row_to_file(filename: str, row: str):
    with open(filename, "a", encoding="utf-8") as f:
        f.write(row + "\n")


def read_file(filename: str,function_on_file: Callable[[TextIO], None]):
    with open(filename, "r", encoding="utf-8") as f:
        function_on_file(f)


def contains_file_type(folder: str, extension: str)-> int:
    folder_path = Path(folder)
    files = list(folder_path.glob(f"*{extension}"))
    return len(files)



def check_directory(path: str | None, creation: bool) -> bool:
    if not path:
        logging.error("Path cannot be empty")
        return False
    if not os.path.isdir(path):
        logging.error(f"Directory non valida: {path}")
        if creation:
            os.makedirs(path, exist_ok=True)
            logging.info(f"Directory creata: {path}")
            return True
        return False
    else:
        logging.info(f"Directory valida: {path}")
        return True



def extract_gz_files(origin_path: str, destination_path: str | None, extracted_gz_list_file: str | None = None) :
    """
    Estrae tutti i file .gz nella cartella specificata.
    """
    if not destination_path :
        destination_path = origin_path

    if not extracted_gz_list_file:
        extracted_gz_list_file = "extracted_gz.txt"

    if not os.path.exists(extracted_gz_list_file):
        open(extracted_gz_list_file, "a").close()

    if not (check_directory(origin_path, False) and check_directory(destination_path, True)):
        return

    logging.info(f"Extracting {origin_path} to {destination_path}")
    extracted_files = __get_extracted_files_list(extracted_gz_list_file)

    for filename in os.listdir(origin_path):
        if not filename.endswith(".gz") and not __has_already_been_extracted(extracted_files, filename):
            continue

        src_path = os.path.join(origin_path, filename)
        dst_path = os.path.join(destination_path, Path(filename).with_suffix("").name)

        logging.info(f"Decompressione: {src_path} -> {dst_path}")

        try:
            with gzip.open(src_path, "rb") as f_in, open(dst_path, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)

            __update_extracted(extracted_gz_list_file, filename, 1)
        except (OSError, EOFError) as e:
            logging.error(f"Errore decompressione {filename}: {e}")
            __update_extracted(extracted_gz_list_file, filename, 0)


def __get_extracted_files_list(extracted_file: str):
    list_of_files = []
    if not os.path.exists(extracted_file):
        return list_of_files

    with open(extracted_file, "r", encoding="utf-8") as f:
        for line in f:
            name, *_ = line.strip().split(" ", 1)
            list_of_files.append(name)
    return list_of_files

def __has_already_been_extracted(list_of_files: list[str], filename: str) -> bool:
    return filename in list_of_files

def __update_extracted(extracted_file: str, filename: str, result_of_extraction_code: int):
    match result_of_extraction_code:
        case 1:
            __append_to_file(extracted_file, f"{filename} EXTRACTED")
        case 0:
            __append_to_file(extracted_file, f"{filename} ERROR")


def __append_to_file(filename: str, row: str) -> None:
    with open(filename, "a", encoding="utf-8") as f:
        f.write(row + "\n")

def main():
    logging.basicConfig(level=logging.INFO)

    test_origin = "C:\\Users\\Sveva\\Documents\\GitHub\\zenodo_dataset"
    test_dest = "C:\\Users\\Sveva\\Desktop\\estratti"

    extract_gz_files(test_origin, test_dest)


if __name__ == "__main__":
    main()



