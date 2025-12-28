import logging
import os.path
from pathlib import Path

import Zenodo.ZenodoDataStructure
from Utils import file_worker as fw

"""
        def download_zenodo_dataset(self, record_id):
    api_url = f"https://zenodo.org/api/records/{record_id}"

    try:
        response = requests.get(api_url)
        response.raise_for_status()
    except requests.RequestException as e:
        logging.error(f"Errore nel contattare Zenodo {api_url}: {e}")
        return []
    try:
        meta_record = response.json()
    except ValueError as e:
        logging.error(f"Errore nella risposta di Zenodo. Non è un json valido: {e}")
        return []

    files = meta_record.get("files", [])
    if not isinstance(files, list):
        logging.error("Campo 'files' mancante o non valido nella risposta Zenodo")
        return []

    gz_files = [f for f in files if f.get("key", "").endswith(".gz")]
    return gz_files

"""


class ZenodoInterpreter:
    def __init__(self, zenodo_local_path: Path):
        logging.info("Zenodo Interpreter started.")
        if not fw.check_directory(zenodo_local_path / "original", False):
            logging.error("no downloaded dataset directory found. Check path and try again")
            raise NotADirectoryError("no downloaded dataset directory found. Check path and try again")
        self.originals = zenodo_local_path / "original"
        fw.check_directory(zenodo_local_path / "extracted", True)
        self.extracted = zenodo_local_path / "extracted"
        fw.check_directory(zenodo_local_path / "cleaned", True)
        self.cleaned = zenodo_local_path / "cleaned"

    def extract_if_needed(self, gz_filename: str) -> Path | None:
        json_name = Path(gz_filename).with_suffix("").name
        if os.path.exists(self.extracted / json_name):
            logging.info(f"{json_name} already extracted")
            return Path(self.extracted / json_name)

        if not self.originals / gz_filename:
            logging.info(f"Cannot extract: no original file found in {self.originals}")
            return None
        else:
            ok = fw.extract_gz_file(self.originals, self.extracted , gz_filename)
            if not ok:
                logging.error(f"{gz_filename} not extracted")
                return None
            else:
                logging.info(f"{gz_filename} extracted")
                return Path(self.extracted / gz_filename)

    def extract_and_clean_raw_zenodo_log(self, gz_filename) -> bool:
        extracted_json = self.extract_if_needed(gz_filename)
        if not extracted_json:
            return False

        cleaned_json = self.cleaned / extracted_json.name
        if cleaned_json.exists():
            logging.info(f"{cleaned_json} already cleaned")
            return True

        try:
            # 1. Carica e pulisci il log
            logging.info(f"cleaning file")
            with open(extracted_json, 'r', encoding='utf-8') as f:
                raw_data = Zenodo.ZenodoDataStructure.json.load(f)

            log = Zenodo.ZenodoDataStructure.ZenodoLog.clean_json_data(raw_data, extracted_json.name)

            # 2. Scrivi il log su file (evento per evento)
            log.write_on_file(cleaned_json)

            return True
        except Exception as e:
            logging.error(f"error in extracting file : {e}")
            return False

    def interpret_all_zenodo_logs_in_originals_folder(self):
        for gz_filename in self.originals.glob("*.gz"):
            self.extract_and_clean_raw_zenodo_log(gz_filename)


def main() -> None:
    logging.basicConfig(level=logging.DEBUG)
    zenodo_interpreter = ZenodoInterpreter(Path("C:\\Users\\Sveva\\Documents\\GitHub\\zenodo_dataset"))
    zenodo_interpreter.interpret_all_zenodo_logs_in_originals_folder()
if __name__ == "__main__":
    main()