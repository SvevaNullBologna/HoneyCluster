import os
from dataclasses import dataclass
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


def process_to_parquet(json_path: Path, output_parquet: Path, chunk_size: int = 500):
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
            timestamps, commands, eventids = [], [], []

            for event in events:
                # estraiamo i cambi base
                eid = event.get('eventid')
                ts_str = event.get('timestamp')
                msg = event.get('message')

                # molto più veloce di pd.to_datetime
                ts = datetime.fromisoformat(ts_str.replace('Z', '+00:00'))

                eventids.append(eid)
                timestamps.append(ts)
                if msg:
                    commands.append(msg)

            if not timestamps:
                continue

            # qui posso già calcolare i valori voluti in HoneyClusterData:
            data_obj = HoneyClusterData(
                inter_command_timing=get_inter_command_timing(timestamps),
                session_duration=get_session_duration(timestamps),
                time_of_day_patterns=get_time_of_day_patterns(timestamps),
                unique_commands_ratio=get_unique_commands_ratio(commands),
                command_diversity_ratio=get_command_diversity_ratio(commands),
                tool_signatures=get_tool_signatures(commands),
                reconnaissance_vs_exploitation_ratio=get_reconnaissance_on_exploitation_ratio(eventids),
                error_rate=get_error_rate(eventids),
                command_correction_attempts = get_command_correction_attempts(eventids, commands)
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
    EXTRACT TEMPORAL FEATURES
"""

def get_inter_command_timing(command_times: list[datetime] = None) -> float:
    """ otteniamo il tempo medio tra un comando e l'altro """
    if not command_times or len(command_times) < 2:
        return 0.0 # non abbiamo informazioni

    deltas = [ # lista delle sottrazioni dei tempi dei comandi
        (command_times[i+1] - command_times[i]).total_seconds()
        for i in range(len(command_times) - 1)
    ]

    return sum(deltas) / len(deltas) # restituisce la media



def get_session_duration(times: list[datetime] = None) -> float:
    """ durata totale della sessione in secondi """
    if not times or len(times) < 2:
        return 0.0
    return (max(times) - min(times)).total_seconds()


def get_time_of_day_patterns(times: list[datetime] = None) -> float:
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

def get_unique_commands_ratio(commands: list[str] = None)-> float:
    if not commands: # non abbiamo informazioni
        return 0.0
    return 4

def get_command_diversity_ratio(commands: list[str] = None)-> float:
    if not commands:
        return 0.0
    return 5

def get_tool_signatures(commands: list[str] = None)-> float:
    if not commands:
        return 0.0
    return 6

"""
    EXTRACT BEHAVIORAL PATTERNS 
"""

def get_reconnaissance_on_exploitation_ratio(eventids : list[str] = None)-> float:
    return 7

def get_error_rate(eventids: list[str] = None)-> float:
    if not eventids:
        return 0.0

    command_statuses = [status for eid in eventids if (status := is_command(eid)) != -1]

    if not command_statuses:
        return 0.0

    return command_statuses.count(0) / len(command_statuses)


def get_command_correction_attempts(eventids: list[str] = None, commands: list[str] = None)-> int:
    if not eventids:
        return 0


    
    return 9
"""
"""

def read_parquet(result_path: Path):
    if not result_path.exists():
        print("Il file non esiste ancora.")
        return pd.DataFrame()

        # Usiamo lo stesso engine usato per la scrittura
    return pd.read_parquet(result_path, engine='fastparquet')


if __name__ == "__main__":
    result_path = Path("C:\\Users\\Sveva\\Desktop\\result")

    process_to_parquet(Path("C:\\Users\\Sveva\\Documents\\GitHub\\zenodo_dataset\\cleaned\\2019-05-14.json"), result_path, 500)
    print(read_parquet(result_path))