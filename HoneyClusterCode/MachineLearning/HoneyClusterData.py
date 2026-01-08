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
    reconnaissance_on_exploitation_ratio: float # da comprendere
    error_rate: float
    command_correction_attempts: int


def get_data_from_session(json_path: Path):
    if not os.path.exists(json_path):
        return None
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

    rows = []

    with open(json_path, 'rb') as f:
        parser = ijson.kvitems(f, 'sessions')

        for session_id, events in parser:
            for event in events:
                # estraiamo i cambi base
                eid = event.get('eventid')
                ts = event.get('timestamp')
                msg = event.get('message')

                rows.append(
                    {
                        "session_id": session_id,
                        "event": eid,
                        "timestamp": ts,
                        "message": msg
                    }
                )

            # qui posso già calcolare i valori voluti in HoneyClusterData:
            #
            #
            #

        df = pd.DataFrame(rows)
        df['timestamp'] = pd.to_datetime(df['timestamp'])

        return df

"""
    EXTRACT TEMPORAL FEATURES
"""

def get_inter_command_timing(command_times: list[datetime]) -> float:
    pass

def get_session_duration(times: list[datetime]) -> float:
    pass

def get_time_of_day_patterns(times: list[datetime]) -> float:
    pass

"""
    EXTRACT COMMAND BASED FEATURES
"""

def get_unique_commands_ratio(commands: list[str])-> float:
    pass

def get_command_diversity_ratio(commands: list[str])-> float:
    pass

def get_tool_signatures(commands: list[str])-> float:
    pass

"""
    EXTRACT BEHAVIORAL PATTERNS 
"""

def get_reconnaissance_on_exploitation_ratio(eventids : list[str])-> float:
    pass

"""def get_error_rate(eventids: list[str])-> float:
    f = 0
    f += 1 if is_command(eventid) == 0 for eventid in eventids
    return f/len(eventids)

def get_command_correction_attempts(eventids: list[str], commands: list[str])-> float:
    pass
"""


if __name__ == "__main__":
    result_dataframe = get_data_from_session(Path("C:\\Users\\Sveva\\Documents\\GitHub\\zenodo_dataset\\cleaned\\2019-05-14.json"))
    print(result_dataframe)