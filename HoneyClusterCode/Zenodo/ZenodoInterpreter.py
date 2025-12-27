import json
import logging
import gzip
from pathlib import Path
from Utils import file_worker as fw
from ZenodoDataStructure import ZenodoLog

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

    def extract_if_needed(self,originals, extracted, gz_filename) -> Path | None:
        json_name = Path(gz_filename).with_suffix("")
        dst = extracted / json_name

        if dst.exists():
            return dst

        gz_path = self.originals / gz_filename
        if gz_path.exists():
            with gzip.open(gz_path, 'rt', encoding='utf-8') as f_in, open(dst, 'w', encoding='utf-8') as f_out:
                f_out.write(f_in.read())
            return dst
        if fw.extract_gz_file(originals, extracted, gz_filename):
            return dst

        return None

    def interpret_raw_zenodo_log(self, gz_filename) -> bool:
        extracted_json = self.extract_if_needed(self.originals, self.extracted, gz_filename)
        if not extracted_json:
            return False

        cleaned_json = self.cleaned / extracted_json.name
        if cleaned_json.exists():
            return False

        with open(extracted_json) as f:
            raw_data = json.load(f)

        zenodo_log = ZenodoLog.clean_json_data(raw_data, extracted_json.name)

        with open(cleaned_json, "w") as f:
            json.dump(zenodo_log.to_dict(), f, indent=4)

        return True


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




def main() -> None:
    logging.basicConfig(level=logging.DEBUG)
    zenodo_interpreter = ZenodoInterpreter(Path("C:\\Users\\Sveva\\Documents\\GitHub\\zenodo_dataset"))
    zenodo_interpreter.interpret_raw_zenodo_log("cyberlab_2019-06-26.json.gz")
if __name__ == "__main__":
    main()