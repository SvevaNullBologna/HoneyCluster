import logging
import os
import gzip
import shutil

def check_directory(path) -> bool:
    if not os.path.isdir(path):
        logging.error(f"Directory non valida: {path}")
        return False
    else:
        logging.info(f"Directory valida: {path}")
        return True



def extract_gz_files(origin_path, destination_path: null) -> bool:
    """
    Estrae tutti i file .gz nella cartella specificata.
    """
    if not check_directory(origin_path):
        return

    if not check_directory(destination_path):
        os.mkdir(destination_path)

    for filename in os.listdir(origin_path):
        if filename.endswith(".gz"):

