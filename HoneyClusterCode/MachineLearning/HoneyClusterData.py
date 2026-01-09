import logging
import os
from dataclasses import dataclass
from difflib import SequenceMatcher
from pathlib import Path

import ijson
import pandas as pd
from datetime import datetime

from Zenodo.ZenodoInterpreter import is_command

"""
DALLA CONSEGNA:

Implementation Guidance
● Extract temporal features: inter-command timing, session duration,
time-of-day patterns
● Command-based features: unique commands ratio, command diversity, tool
signatures
● Behavioral patterns: reconnaissance vs. exploitation ratio, error rate,
command correction attempts
"""

@dataclass
class HoneyClusterData:
    # temporal features
    inter_command_timing : float    # tempo che passa tra i comandi inviati
    session_duration: float     # durata della sessione
    time_of_day_patterns: float # codifica dell'abitudine temporale
    # command based features
    unique_commands_ratio : float
    command_diversity_ratio : float
    tool_signatures : float # presenza di famiglie di comandi per esempio: scanning -> nmap, ifconfig, netstat ; download -> wget, curl ; privilege escalation -> sudo, chmod
    # behavioral patterns
    reconnaissance_vs_exploitation_ratio: float #numero comandi di esplorazione vs numero comandi attivi e intrusivi
    error_rate: float # comandi errati / comandi
    command_correction_attempts: int # quante volte in media l'attaccante cerca di correggersi

"""
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
                PROCESSING DATASET INTO USEFUL DATAS
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
"""
def process_to_parquet(json_path: Path, output_parquet: Path, chunk_size: int = 500): # * vedi spiegazione sul formato parquet in documenti
    """
        {
            "sessions": {
                "idnumerico" : [
                    {
                        "eventid" : "stringa",
                        "timestamp" : "valo reinstringa",
                        "message": "comando" <- ATTENZIONE: questo campo compare solo se eventid corrisponde ad un comando
                    }
                ],
                ...
            }
        }
        """

    if not os.path.exists(json_path):
        return

    buffer = []

    with open(json_path, 'rb') as f:
        parser = ijson.kvitems(f, 'sessions')

        for session_id, events in parser:
            # estrazione immediata dei dati necessari dalla sessione corrente
            timestamps, commands, verbs, statuses = [], [], [], []

            for event in events:
                # estraiamo i cambi base
                eid = event.get('eventid')
                ts_str = event.get('timestamp')
                msg = event.get('message')

                # molto più veloce di pd.to_datetime
                ts = datetime.fromisoformat(ts_str.replace('Z', '+00:00'))

                statuses.append(is_command(eid))
                timestamps.append(ts)
                if msg:
                    commands.append(msg)
                    verbs.append(_get_verb_of_command(msg))

            if not timestamps:
                continue

            # qui posso già calcolare i valori voluti in HoneyClusterData:
            data_obj = HoneyClusterData(
                inter_command_timing=_get_inter_command_timing(timestamps),
                session_duration=_get_session_duration(timestamps),
                time_of_day_patterns=_get_time_of_day_patterns(timestamps),
                unique_commands_ratio=_get_unique_commands_ratio(commands),
                command_diversity_ratio=_get_command_diversity_ratio(verbs),
                tool_signatures=_get_tool_signatures(verbs),
                reconnaissance_vs_exploitation_ratio=_get_reconnaissance_vs_exploitation_ratio(verbs),
                error_rate=_get_error_rate(statuses),
                command_correction_attempts =_get_command_correction_attempts(statuses, commands)
            )

            row = data_obj.__dict__
            row['session_id'] = session_id
            buffer.append(row)

            # Quando il buffer è pieno, scrivi su disco
            if len(buffer) >= chunk_size:
                _save_chunk(buffer, output_parquet)
                buffer = []  # Svuota il buffer

            # Scrivi gli ultimi rimasti

        if buffer:
            _save_chunk(buffer, output_parquet)


def _save_chunk(data_list: list, file_path: Path):
    df_chunk = pd.DataFrame(data_list)

    if not file_path.exists():
        df_chunk.to_parquet(file_path, engine='fastparquet')
    else:
        df_chunk.to_parquet(file_path, engine='fastparquet', append=True)

"""
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
                EXTRACTING FEATURES
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
"""
"""
    EXTRACT TEMPORAL FEATURES
"""

def _get_inter_command_timing(command_times: list[datetime] = None) -> float:
    """ otteniamo il tempo medio tra un comando e l'altro """
    if not command_times or len(command_times) < 2:
        return 0.0 # non abbiamo informazioni

    deltas = [ # lista delle sottrazioni dei tempi dei comandi
        (command_times[i+1] - command_times[i]).total_seconds()
        for i in range(len(command_times) - 1)
    ]

    return sum(deltas) / len(deltas) # restituisce la media

def _get_session_duration(times: list[datetime] = None) -> float:
    """ durata totale della sessione in secondi """
    if not times or len(times) < 2:
        return 0.0
    return (max(times) - min(times)).total_seconds()

def _get_time_of_day_patterns(times: list[datetime] = None) -> float:
    """ otteniamo l'abitudine oraria dell'attaccante """
    if not times: # se non abbiamo informazioni
        return 0.0

    # il timestamp in zenodo è universale, quindi non dobbiamo attenzionare la posizione geografica
    hours = [t.hour + t.minute / 60 for t in times] # calcoliamo una lista con le ore in cui attacca
    avg_hour = sum(hours) / len(hours)
    return avg_hour / 24 # se il valore è vicino a 0, sta attaccando di notte, alle 0.5 è pomeriggio

"""
    EXTRACT COMMAND BASED FEATURES
"""


_SIGNATURES = {
        'scanning': {'nmap', 'netstat', 'ifconfig', 'arp', 'route', 'ping'},
        'download': {'wget', 'curl', 'tftp', 'ftp', 'scp'},
        'priv_esc': {'sudo', 'chmod', 'chown', 'su', 'visudo'},
        'discovery': {'whoami', 'id', 'uname', 'pwd', 'cat /etc/passwd'},
        'cleanup': {'history -c', 'rm -rf', 'unset HISTFILE'}
}

def _get_unique_commands_ratio(commands: list[str] = None)-> float:
    if not commands: # non abbiamo informazioni
        return 0.0
    return len(set(commands)) / len(commands) # quanti comandi unici ci sono sul loro totale. Qui non si considerano gli errori nella scrittura dei comandi ma il 'movimento'

def _get_command_diversity_ratio(verbs: list[str] = None)-> float:
    """ quanti strumenti diversi conosce l'attaccante """
    if not verbs:
        return 0.0

    return len(set(verbs))/len(verbs)


_BEHAVIORAL_MAP = {
    "reconnaissance": {
        "file_system": {"ls", "cd", "pwd", "which", "find", "du", "stat"},
        "file_reading": {"cat", "head", "tail", "more", "less"},
        "system_info": {"uname", "lscpu", "free", "uptime", "hostname", "df"},
        "user_info": {"whoami", "id", "groups", "last", "w"},
        "processes_env": {"ps", "top", "env", "history", "export", "alias"},
        "network": {"ifconfig", "netstat", "ip", "route", "arp", "ping"}
    },
    "exploitation": {
        "malware_download": {"wget", "curl", "tftp", "ftp", "scp", "sftp"},
        "priv_esc": {"chmod", "chown", "sudo", "su", "visudo"},
        "scripting_exec": {"sh", "bash", "python", "python3", "perl", "php", "gcc", "make"},
        "persistence_mod": {"crontab", "mkdir", "touch", "echo", "rm", "mv", "cp", "ln"},
        "defense_evasion": {"pkill", "kill", "killall", "unset", "iptables", "ufw"}
    }


}


def _get_tool_signatures(verbs: list[str] = None)-> float:
    """ conta quante sotto-categorie (firme) diverse sono state attivate """
    if not verbs:
        return 0.0

    found_signatures = set()
    verbs_set = set(verbs) #per ricerca veloce, così non ci ripetiamo

    # Cicliamo su Recon e Exploitation
    for macro_category in _BEHAVIORAL_MAP.values():
        # Cicliamo sulle sotto-categorie (es. file_system, network, priv_esc...)
        for signature_name, signature_verbs in macro_category.items():
            # Se l'attaccante ha usato almeno un verbo di questa firma
            if any(v in verbs_set for v in signature_verbs):
                found_signatures.add(signature_name)


    return float(len(found_signatures))

"""
    EXTRACT BEHAVIORAL PATTERNS 
"""

def _get_reconnaissance_vs_exploitation_ratio(verbs : list[str] = None)-> float:
    """ calcola il rapporto tra verbi di ricerca e verbi di attacco """
    if not verbs:
        return 0.0

    all_recon = set().union(*_BEHAVIORAL_MAP["reconnaissance"].values() )
    all_exploit = set().union(*_BEHAVIORAL_MAP["exploitation"].values() )

    recon_count = sum(1 for v in verbs if v in all_recon)
    exploit_count = sum(1 for v in verbs if v in all_exploit)

    if exploit_count == 0: # se non c'è exploitation, il rapporto è basato solo sulla recon
        return float(recon_count)

    return recon_count / exploit_count


def _get_error_rate(statuses: list[int] = None)-> float:
    if not statuses:
        return 0.0

    return statuses.count(0) / len(statuses)


def _get_command_correction_attempts(statuses: list[str] = None, commands: list[str] = None)-> int:

    cmd_statuses = [s for s in statuses if s != -1] # la lunghezza dovrebbe matchare quella della lista dei comandi

    if len(commands) < 2 or len (cmd_statuses) != len(commands) :
        return 0

    corrections = 0
    for i in range(1, len(commands)):
        if cmd_statuses[i-1] == 0 :
            # calcoliamo quanto il comando attuale è simile al precedente
            similarity = SequenceMatcher(None, commands[i-1], commands[i]).ratio()

            if 0.6 <= similarity < 1.0 : # se le similitudini vanno dal 60% a salire, è una correzione
                corrections += 1

    return corrections
"""
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
                                    UTILITIES
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
"""

def _get_verb_of_command(cmd: str = None) -> str: # prendiamo il verbo del comando ovvero : uname -a -> uname
    # strip elimina gli spazi all'inizio e alla fine
    # split divide in sottostringhe secondo un delimitatore. Senza nulla dentro, divide per spazi
    return cmd.strip().split()[0]



"""
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
                                        EXECUTION
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
"""


def process_cleaned_dataset(base_folder_path: Path):
    if not (base_folder_path and base_folder_path.exists()):
        logging.error(f"base_folder_path {base_folder_path} non existent")
        return
    starting_path = Path(base_folder_path, "cleaned")
    resulting_path = base_folder_path / "processed"
    if not starting_path.exists():
        print(f"Errore: La cartella {starting_path} non esiste.")
        return

    resulting_path.mkdir(parents=True, exist_ok=True)

    for json_file in starting_path.glob("*.json"):
        parquet_output = resulting_path / json_file.with_suffix('.parquet').name
        if os.path.exists(parquet_output):
            logging.info(f"skipping {parquet_output}.")
            continue

        logging.info(f"Processing {json_file} ...")
        try:
            process_to_parquet(json_file, parquet_output)
            logging.info(f"Completed {json_file}")
        except Exception as e:
            logging.warning(f"Errore durante il processamento di {json_file.name}: {e}")

def read_parquet(output_path: Path):
    if not output_path.exists():
        print("Il file non esiste ancora.")
        return pd.DataFrame()

        # Usiamo lo stesso engine usato per la scrittura
    return pd.read_parquet(output_path, engine='fastparquet')

def concat_parquets(parquets_folder_path : Path) -> pd.DataFrame:
    all_files = parquets_folder_path.glob('*.parquet')

    df = pd.concat((pd.read_parquet(f) for f in all_files), ignore_index=True)

    logging.info(f"Number of loaded files: {len(df)}")

    df.to_parquet(parquets_folder_path.parent / "complete_dataset.parquet", index=False)
    return df



if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    process_cleaned_dataset(Path("C:\\Users\\Sveva\\Documents\\GitHub\\zenodo_dataset"))
    concat_parquets(Path("C:\\Users\\Sveva\\Documents\\GitHub\\zenodo_dataset\\processed"))