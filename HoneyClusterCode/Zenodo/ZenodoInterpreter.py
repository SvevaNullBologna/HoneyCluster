import logging
import os
from pathlib import Path
import gzip
import json
import ijson


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


def _clean_geolocation(geo:dict) -> dict | None:
    if not geo:
        return None
    geo_clean = {k: float(v) if type(v).__name__ == "Decimal" else v for k,v in geo.items() if v is not None}
    return geo_clean if geo_clean else None


def _parse_date_from_filename(filename:str) -> str:
    return filename.removesuffix(".gz").removeprefix("cyberlab_")


def _clean_event(e:dict) -> dict | None:
    if not e.get("eventid"):
        return None

    cleaned = {}
    for k,v in e.items():
        if k == "session_id":
            continue
        if v is not None:
            if type(v).__name__ == "Decimal":
                v = float(v)
            cleaned[k] = v
    return cleaned


class ZenodoInterpreter:
    def __init__(self, zenodo_local_path: Path):
        logging.info("Zenodo Interpreter started.")
        if not check_directory(zenodo_local_path / "original", False):
            logging.error("no downloaded dataset directory found. Check path and try again")
            raise NotADirectoryError("no downloaded dataset directory found. Check path and try again")
        self.originals = zenodo_local_path / "original"
        check_directory(zenodo_local_path / "cleaned", True)
        self.cleaned = zenodo_local_path / "cleaned"

    def extract_and_clean_all_zenodo_logs_in_folder(self) -> bool:
        completed = False
        for filename in self.originals.glob("*.json.gz"):
            completed &= self._clean_zenodo_gz(filename)
        return completed

    def _clean_zenodo_gz(self, gz_path: Path) -> bool:
        log_date = _parse_date_from_filename(gz_path.name)
        out_file = (self.cleaned / log_date).with_suffix(".json")

        if out_file.exists():
            logging.info(f"skipping {log_date}. It has already been cleaned")
            return True

        try:
            logging.info(f"cleaning {log_date} to {out_file}")
            with gzip.open(gz_path, "rb") as f, open(out_file, "w", encoding="utf-8") as out:

                out.write('{\n')
                out.write(f'    "date": "{log_date}",\n')
                out.write(' "sessions": [\n')

                first_session = True
                for session in ijson.items(f, "item"):
                    session_id, events = next(iter(session.items()))
                    cleaned_events = []

                    for e in events:
                        cleaned = _clean_event(e)
                        if not cleaned:
                            continue

                        #pulizia geolocation_data
                        geo = cleaned.get("geolocation_data")
                        geo_clean = _clean_geolocation(geo)

                        if geo_clean:
                            cleaned["geolocation_data"] = geo_clean
                        else:
                            cleaned.pop("geolocation_data", None)

                        cleaned_events.append(cleaned)

                    if not cleaned_events:
                        continue

                    if not first_session:
                        out.write(",\n")

                    first_session = False

                    json.dump({session_id: cleaned_events}, out, ensure_ascii=False, default=float)

                out.write("\n   ]\n}")
                return True
        except Exception as e:
            logging.error(f"error cleaning {gz_path.name}: {e}")
            return False


"""
////////////////////////////////////////////////////////////////////////////////////////////
                                    UTILS
////////////////////////////////////////////////////////////////////////////////////////////
"""

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
