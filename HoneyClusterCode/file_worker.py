import logging
import os
import gzip
import shutil

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



def extract_gz_files(origin_path: str, destination_path: str | None) :
    """
    Estrae tutti i file .gz nella cartella specificata.
    """
    if destination_path is None or destination_path == "":
        destination_path = origin_path

    if check_directory(origin_path, False) and check_directory(destination_path, True):
            logging.info(f"Extracting {origin_path} to {destination_path}")
            for filename in os.listdir(origin_path):
                if filename.endswith(".gz"):
                    src_path = os.path.join(origin_path, filename)

                    output_filename = filename[:-3]
                    dst_path = os.path.join(destination_path, output_filename)

                    logging.info(f"Decompressione: {src_path} -> {dst_path}")
                try:
                    with gzip.open(src_path, "rb") as f_in:
                        with open(dst_path, "wb") as f_out:
                            shutil.copyfileobj(f_in, f_out)
                except (OSError, EOFError) as e:
                    logging.error(f"Errore decompressione {filename}: {e}")
                    continue

def main():
    logging.basicConfig(level=logging.INFO)

    test_origin = "C:\\Users\\Sveva\\Documents\\GitHub\\zenodo_dataset"
    test_dest = "C:\\Users\\Sveva\\Desktop\\estratti"

    extract_gz_files(test_origin, test_dest)


if __name__ == "__main__":
    main()



