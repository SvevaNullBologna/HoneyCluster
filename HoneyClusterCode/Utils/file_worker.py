import logging
import os
import gzip
import shutil

from pathlib import Path


"""
////////////////////////////////////////////////////////////////////////////////////////////
                                    UTILS
////////////////////////////////////////////////////////////////////////////////////////////
"""

def drop_nulls(d: dict)-> dict:
    return {k: v for k, v in d.items() if v is not None}


def contains_file_type(folder: str, extension: str)-> int:
    folder_path = Path(folder)
    files = list(folder_path.glob(f"*{extension}"))
    return len(files)


def check_directory(path: Path | None, creation: bool) -> bool:
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


def _already_exists(destination_path, filename: str) -> bool:
    return os.path.exists(os.path.join(destination_path, filename))


def extract_gz_file(original_path: Path, destination_path: Path, filename) -> bool :
    filename = str(filename)
    if not filename.endswith(".gz") or _already_exists(destination_path, Path(filename).with_suffix("").name):
        logging.info(f"skipping {filename}")
        return False

    src_path = os.path.join(original_path, filename)
    dst_path = os.path.join(destination_path, Path(filename).with_suffix("").name)

    try:
        with gzip.open(src_path, "rb") as f_in, open(dst_path, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)
        logging.info(f"Decompressione di {filename} in {src_path} -> {dst_path}")
    except (OSError, EOFError) as e:
        logging.error(f"Errore decompressione {filename}: {e}")
        return False
    return True



"""
def extract_gz_files(origin_path: str, destination_path: str | None) -> str|None :
    
    Estrae tutti i file .gz nella cartella specificata.
    
    if not destination_path :
        destination_path = origin_path

    if not (check_directory(origin_path, False) and check_directory(destination_path, True)):
        return None

    extracted = 0
    for filename in os.listdir(origin_path):
        if extract_gz_file(origin_path, destination_path, filename):
            extracted += 1

    logging.info(f"End of extraction of gz files. Extracted : {extracted}")
    return destination_path





def _append_to_file(filename: str, row: str) -> None:
    with open(filename, "a", encoding="utf-8") as f:
        f.write(row + "\n")

def main():
    logging.basicConfig(level=logging.INFO)

    test_origin = "C:\\Users\\Sveva\\Documents\\GitHub\\zenodo_dataset"
    test_dest = "C:\\Users\\Sveva\\Desktop\\new"

    extract_gz_files(test_origin, test_dest)


if __name__ == "__main__":
    main()

"""

