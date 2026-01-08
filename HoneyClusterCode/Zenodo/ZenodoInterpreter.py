import logging
import os
from pathlib import Path
import gzip
import json
import ijson
import re
from datetime import datetime


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
            # 1. Inizio del documento
            out.write('{\n')
            out.write('  "sessions": {')  # Apro l'oggetto sessions

            first_session = True

            # ijson.items legge un generatore, quindi la RAM resta bassa
            for session in ijson.items(f, "item"):
                for session_id, events in session.items():
                    cleaned_events = []

                    for e in events:
                        ce = _clean_event(e)
                        if not ce:
                            continue

                        cleaned_events.append(ce)

                    if not cleaned_events:
                        continue

                    # 2. Gestione della virgola tra le chiavi di "sessions"
                    if not first_session:
                        out.write(",")

                    out.write(f'\n    "{session_id}": ')
                    # dumpiamo solo la lista di eventi di UNA sessione alla volta
                    json.dump(cleaned_events, out, ensure_ascii=False, default=float)

                    first_session = False

            # 3. Chiusura della struttura (fondamentale!)
            out.write('\n  }\n}')

        return True

    except Exception as e:
        logging.error(f"error cleaning {gz_path.name}: {e}")
        # Se fallisce, rimuoviamo il file parziale per evitare corruzioni al prossimo avvio
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
    eventid = e.get("eventid")
    if not eventid:
        return None

    cleaned = {
       "eventid": eventid
    }

    #timestamp sempre utile (timing, inter-command time)
    if e.get("timestamp") is not None:
        cleaned["timestamp"] = _convert_decimals(e["timestamp"])

    #message solo se è un comando
    if is_command(eventid) != -1:
        msg = e.get("message")
        if msg:
            msg = _isolate_command(msg)
            cleaned["message"] = _convert_decimals(msg)


    #geolocalizzazione opzionale
    if add_geolocation:
        geo = e.get("geolocation_data")
        if geo:
            geo_clean = _clean_geolocation(geo)
            if geo_clean:
                cleaned["geolocation_data"] = geo_clean

    return cleaned

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



def get_time(timestamp: str) -> datetime | None:
    """ nel json "timestamp": "2019-05-18T00:00:16.582846Z" """
    if not timestamp:
        logging.error("empty timestamp")
        return None
    try:
        dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        return dt
    except ValueError:
        logging.warning(f"Invalid timestamp: {timestamp}")
        return None

def is_command(eventid: str) -> int:
    """
    -1 = not a command
    0 = command failure
    1 = command success
    2 = command input / unknown
    """

    if not eventid.startswith("cowrie.command"):
        return -1
    if eventid.endswith("success"):
        return 1
    if eventid.endswith("failure"):
        return 0
    return 2

def _isolate_command(message: str) -> str:
    s = message.strip()
    s = re.sub(r'^Command found:\s*', '', s, flags=re.IGNORECASE)
    s = re.sub(r'^CMD:\s*', '', s, flags=re.IGNORECASE)
    s = re.sub(r'^Command not found:\s*', '', s, flags=re.IGNORECASE)
    return s

def extract_command(message: str, eventid: str) -> str | None:
    if not message:
        # logging.error("no valid message")
        return None
    if is_command(eventid) == -1:
        # logging.error("invalid command")
        return None
    return _isolate_command(message)


def parse_date_from_json_filename(filename: str) -> str: # es: cyberlab_2019-05-13.json -> 2019-05-13
    filename = filename.removesuffix(".json")
    return filename.removeprefix("cyberlab_")

def get_date_from_string(date: str, pattern: str ="%Y-%m-%d") -> datetime:
    """ yyyy-mm-dd """
    return datetime.strptime(date, pattern)

def drop_nulls(d: dict)-> dict:
    return {k: v for k, v in d.items() if v is not None}



if __name__ == "__main__":
    clean_zenodo_dataset(Path("C:\\Users\\Sveva\\Documents\\GitHub\\zenodo_dataset"))


