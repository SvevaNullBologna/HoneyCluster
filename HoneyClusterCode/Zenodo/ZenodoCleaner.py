import logging
import os
from pathlib import Path
import gzip
import json
import ijson

from datetime import datetime

import ZenodoDataReader as ZK


def set_working_folder(zenodo_local_path: Path):
    if not _check_directory(zenodo_local_path / "original", False):
        logging.error("no downloaded dataset directory found. Check path and try again")
        raise NotADirectoryError("no downloaded dataset directory found. Check path and try again")
    raw_path = zenodo_local_path / "original"
    _check_directory(zenodo_local_path / "cleaned", True)
    clean_path = zenodo_local_path / "cleaned"
    return raw_path, clean_path


def extract_and_clean_all_zenodo_logs_in_folder(originals_path: Path, cleaned_path: Path) -> bool:
    completed = False
    for filename in originals_path.glob("*.json.gz"):
        completed &= clean_zenodo_gz(filename, cleaned_path)
    return completed


def clean_zenodo_gz(gz_path: Path, cleaned_path: Path) -> bool:
    log_date = parse_date_from_gz_filename(gz_path.name)
    out_file = (cleaned_path / log_date).with_suffix(".json")

    if out_file.exists():
        logging.info(f"skipping {log_date}. It has already been cleaned")
        return True

    try:
        logging.info(f"cleaning {log_date} to {out_file}")

        with gzip.open(gz_path, "rb") as f, open(out_file, "w", encoding="utf-8") as out:
            out.write('{\n  "sessions": {')
            first_session = True

            # Iteriamo sugli oggetti principali del JSON
            for session in ijson.items(f, "item"):
                # Ogni 'session' è un dict { session_id: [events] }
                for session_id, events in session.items():
                    if not events:
                        continue

                    cleaned_events = []
                    has_real_activity = False

                    # Processiamo ogni evento della sessione
                    for e in events:
                        ce = _clean_event(e)
                        if ce:
                            cleaned_events.append(ce)
                            has_real_activity = True

                    # SCRIVIAMO LA SESSIONE (Spostato dentro il ciclo corretto)
                    if has_real_activity:
                        session_data = {
                            ZK.Cleaned_Attr.START_TIME.value: events[0].get(ZK.Useful_Cowrie_Attr.TIME.value),
                            ZK.Cleaned_Attr.END_TIME.value: events[-1].get(ZK.Useful_Cowrie_Attr.TIME.value),
                            ZK.Cleaned_Attr.COUNT.value: len(events),
                            ZK.Cleaned_Attr.EVENTS.value: cleaned_events
                        }

                        if not first_session:
                            out.write(",")

                        out.write(f'\n    "{session_id}": ')
                        json.dump(session_data, out, ensure_ascii=False)
                        first_session = False

            out.write('\n  }\n}')
        return True

    except Exception as e:
        logging.error(f"error cleaning {gz_path.name}: {e}")
        if out_file.exists():
            out_file.unlink()
        return False

def get_zenodo_log_list(cleaned_path: Path):
    logging.info("getting Zenodo log list")

    logs = []

    for file in cleaned_path.glob("*json"):
        try:
            with open(file, "r", encoding="utf-8") as f:
                # "sessions" è un oggetto, vogliamo leggere session_id -> eventi
                sessions = {}
                parser = ijson.kvitems(f, "sessions")  # legge le coppie chiave-valore
                for session_id, events in parser:
                    sessions[session_id] = events  # events è già lista di dizionari
                logs.append({
                    "date": parse_date_from_json_filename(file.name),
                    "sessions": sessions
                })
        except Exception as e:
            logging.error(f"Error reading cleaned log {file.name}: {e}")

    return logs

def clean_zenodo_dataset(zenodo_folder: Path):
    originals, cleaned = set_working_folder(zenodo_folder)

    extract_and_clean_all_zenodo_logs_in_folder(originals, cleaned)


"""
    PRIVATE FUNCTION FOR USAGE PURPOSE

"""

def _check_directory(path: Path | None, creation: bool) -> bool:
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


def _clean_event(e:dict, add_geolocation: bool = False) -> dict | None: # elimina i None, converte i Decimal in Float ed elimina session id che è già usato come chiave
    eventid = e.get(ZK.Useful_Cowrie_Attr.EVENTID.value)
    if not eventid:
        return None

    status = ZK.get_status(eventid)
    if not ZK.status_is_interesting(status):
        return None

    cleaned = {
        ZK.Cleaned_Attr.STATUS.value : status,
        ZK.Cleaned_Attr.TIME.value: e.get(ZK.Useful_Cowrie_Attr.TIME.value)
    }

    specific_data = ZK.get_data_by_status(status,e)
    if specific_data:
        cleaned.update(specific_data)


    #geolocalizzazione opzionale
    if add_geolocation:
        geo = e.get(ZK.Useful_Cowrie_Attr.GEO.value)
        geo_clean = _clean_geolocation(geo)
        if geo_clean:
            cleaned[ZK.Cleaned_Attr.GEO.value] = geo_clean

    return _convert_decimals(cleaned)

def _clean_geolocation(geo:dict) -> dict | None: # mantiene solo campi validi e converte Decimal -> float
    if not geo:
        return None

    keep_keys = {"country_name", "city_name", "latitude", "longitude"}
    geo_clean = {k: float(v) if type(v).__name__ == "Decimal" else v for k,v in geo.items() if k in keep_keys and v is not None}
    return geo_clean if geo_clean else None

def _convert_decimals(obj):
    if isinstance(obj, dict):
        return {k: _convert_decimals(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [_convert_decimals(i) for i in obj]
    elif type(obj).__name__ == "Decimal":
        return float(obj)
    else:
        return obj

def parse_date_from_gz_filename(filename:str) -> str:
    return filename.removesuffix(".json.gz").removeprefix("cyberlab_")

"""
////////////////////////////////////////////////////////////////////////////////////////////
                                    UTILITIES 
////////////////////////////////////////////////////////////////////////////////////////////
"""


def parse_date_from_json_filename(filename: str) -> str: # es: cyberlab_2019-05-13.json -> 2019-05-13
    filename = filename.removesuffix(".json")
    return filename.removeprefix("cyberlab_")

def get_date_from_string(date: str, pattern: str ="%Y-%m-%d") -> datetime:
    """ yyyy-mm-dd """
    return datetime.strptime(date, pattern)

def drop_nulls(d: dict)-> dict:
    return {k: v for k, v in d.items() if v is not None}



if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    clean_zenodo_dataset(Path("C:\\Users\\Sveva\\Documents\\GitHub\\zenodo_dataset"))


