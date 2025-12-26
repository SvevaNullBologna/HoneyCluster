import json
import logging


from Utils import file_worker
import os
from ZenodoDataStructure import ZenodoLog

class ZenodoDownloader:
    def __init__(self, zenodo_local_path: str , extracted_zenodo_local_path : str | None):
        logging.info("Zenodo Interpreter started.")
        if file_worker.contains_file_type(zenodo_local_path, ".gz") > 0 :
            self.zenodo_path, self.extracted_zenodo_path = file_worker.extract_gz_files(zenodo_local_path, extracted_zenodo_local_path)

        else:
            logging.info("Zenodo Interpreter failed because of error in path.")


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




    def interpret_single_zenodo_json(self, filename: str):
        try:
            with open(filename, "r", encoding="utf-8") as json_file:
                json_data = json.load(json_file)

            date = ZenodoLog.parse_date_from_filename(filename)
            log = ZenodoLog.from_json(date, json_data)

            print(log)

        except Exception as e:
            logging.error(f"Failed to parse JSON {filename}: {e}")

def main() -> None:
    logging.basicConfig(level=logging.DEBUG)
    z_interpreter = ZenodoDownloader("C:\\Users\\Sveva\\Documents\\GitHub\\zenodo_dataset", "C:\\Users\\Sveva\\Desktop\\estratti")
    z_interpreter.interpret_single_zenodo_json(os.path.join(z_interpreter.extracted_zenodo_path, "cyberlab_2019-05-18.json"))


if __name__ == "__main__":
    main()