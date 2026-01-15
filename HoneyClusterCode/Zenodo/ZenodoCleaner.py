import logging
from pathlib import Path
import gzip
import json
import ijson

import ZenodoDataReader as ZK
from Main.HoneyCluster import HoneyClusterPaths


def clean_zenodo_dataset(paths :HoneyClusterPaths):
    extract_and_clean_all_zenodo_logs_in_folder(paths.original_folder, paths.cleaned_folder)

def extract_and_clean_all_zenodo_logs_in_folder(originals_path: Path, cleaned_path: Path): # cleans all gz zenodo files in a directory
    for filename in originals_path.glob("*.json.gz"):
        clean_zenodo_gz(filename, cleaned_path)

def clean_zenodo_gz(gz_path: Path, cleaned_path: Path) -> bool: # cleans single file
    log_date = _parse_date_from_gz_filename(gz_path.name)
    out_file = (cleaned_path / log_date).with_suffix(".json")

    if out_file.exists():
        logging.info(f"skipping {log_date}. It has already been cleaned")
        return True

    try:
        logging.info(f"cleaning {log_date} to {out_file}")

        with gzip.open(gz_path, "rb") as f, open(out_file, "w", encoding="utf-8") as out:
            out.write('[\n')  # inizio lista JSON
            first_session = True

            # Iteriamo sugli oggetti principali del JSON
            for session in ijson.items(f, "item"):
                for _, events in session.items():  # ignoriamo session_id
                    if not events:
                        continue

                    cleaned_events = []
                    has_real_activity = False

                    for e in events:
                        ce = _clean_event(e)
                        if ce:
                            cleaned_events.append(ce)
                            has_real_activity = True

                    if has_real_activity:
                        session_data = {
                            ZK.Cleaned_Attr.START_TIME.value: events[0].get(ZK.Useful_Cowrie_Attr.TIME.value),
                            ZK.Cleaned_Attr.END_TIME.value: events[-1].get(ZK.Useful_Cowrie_Attr.TIME.value),
                            ZK.Cleaned_Attr.EVENTS.value: cleaned_events
                        }

                        if not first_session:
                            out.write(",\n")

                        # scriviamo la sessione direttamente
                        out.write(json.dumps(session_data, ensure_ascii=False))
                        first_session = False

            out.write('\n]')  # chiusura lista JSON

        return True

    except Exception as e:
        logging.error(f"error cleaning {gz_path.name}: {e}")
        if out_file.exists():
            out_file.unlink()
        return False



"""
    PRIVATE FUNCTION FOR USAGE PURPOSE

"""


def _clean_event(e:dict) -> dict | None: # elimina i None, converte i Decimal in Float ed elimina session id che è già usato come chiave
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

    specific_data = ZK.get_interesting_data_by_status(status, e)
    if specific_data:
        cleaned.update(specific_data)

    return _convert_decimals(cleaned)


def _convert_decimals(obj):
    if isinstance(obj, dict):
        return {k: _convert_decimals(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [_convert_decimals(i) for i in obj]
    elif type(obj).__name__ == "Decimal":
        return float(obj)
    else:
        return obj

def _parse_date_from_gz_filename(filename:str) -> str:
    return filename.removesuffix(".json.gz").removeprefix("cyberlab_")

"""
GET COMMANDS
"""
def DEBUG_from_all_files_print_commands(jsonpath : Path, out_commands: Path) -> None:
    for gz_path in jsonpath.glob("*.json.gz"):
        with gzip.open(gz_path, "rb") as f:

            # Iteriamo sugli oggetti principali del JSON
            for session in ijson.items(f, "item"):
                commands = []
                for _, events in session.items():  # ignoriamo session_id
                    if not events:
                        continue
                    for event in events:
                        eventid = event.get(ZK.Useful_Cowrie_Attr.EVENTID.value)
                        status = ZK.get_status(eventid)
                        if ZK.is_only_command(status):
                            msg = event.get(ZK.Useful_Cowrie_Attr.MSG.value)
                            if msg:
                                commands.append(msg)
                        if status == ZK.Event.TCPIP_DATA:
                            logging.info(f"Data {status}")
                            data = event.get(ZK.Useful_Cowrie_Attr.DATA.value)
                            logging.debug(data)
                            if msg:
                                commands.append(data)
                if commands:
                    with open(out_commands, "a", encoding="utf-8") as out:
                        out.writelines('\n\n'.join(commands))



if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    #clean_zenodo_dataset(HoneyClusterPaths(Path("C:\\Users\\Sveva\\Documents\\GitHub\\zenodo_dataset")))
    DEBUG_from_all_files_print_commands(Path("C:\\Users\\Sveva\\Documents\\GitHub\\zenodo_dataset\\original"), Path("C:\\Users\\Sveva\\Desktop\\comandi.txt"))

