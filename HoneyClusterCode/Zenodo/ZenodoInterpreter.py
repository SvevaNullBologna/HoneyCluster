import json
import logging
from Utils import file_worker
import os
from ZenodoDataStructure import ZenodoLog

class ZenodoInterpreter:
    def __init__(self, zenodo_local_path: str , extracted_zenodo_local_path : str | None):
        logging.info("Zenodo Interpreter started.")
        if file_worker.contains_file_type(zenodo_local_path, ".gz") > 0 :
            self.zenodo_path, self.extracted_zenodo_path = file_worker.extract_gz_files(zenodo_local_path, extracted_zenodo_local_path)

        else:
            logging.info("Zenodo Interpreter failed because of error in path.")

    def interpret_single_zenodo_json(self, filename: str):
        try:
            with open(filename, "r", encoding="utf-8") as json_file:
                json_data = json.load(json_file)

            date = ZenodoLog.parse_date_from_filename(filename)
            log = ZenodoLog.from_json(date, json_data)

            log.print_log()

        except Exception as e:
            logging.error(f"Failed to parse JSON {filename}: {e}")

def main() -> None:
    logging.basicConfig(level=logging.DEBUG)
    z_interpreter = ZenodoInterpreter("C:\\Users\\Sveva\\Documents\\GitHub\\zenodo_dataset", "C:\\Users\\Sveva\\Desktop\\estratti")
    z_interpreter.interpret_single_zenodo_json(os.path.join(z_interpreter.extracted_zenodo_path, "cyberlab_2019-05-18.json"))

if __name__ == "__main__":
    main()