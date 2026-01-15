
import re
from datetime import datetime
from typing import Tuple

from MachineLearning.command_vocabularies import get_fast_check_set, TLS_VERSIONS_MAP, HTTP_VERBS_MAP, TLS_NOT_KNOWN
from Zenodo.Zenodo_keys import Status, Event, Useful_Cowrie_Attr, Cleaned_Attr
"""
/////////////////////////////////////////////////ALWAYS_USEFUL///////////////////////////////////////////////////////
"""

def get_status(event_id: str) -> int:
    for e in Event:
        if event_id == e.value:
            return Status[e.name].value
    return Status.IGNORED.value

def status_is_interesting(status: int) -> bool :
    return status != Status.IGNORED.value

def get_datetime(timestamp: str) -> datetime | None:
    """ nel json "timestamp": "2019-05-18T00:00:16.582846Z" """
    if not timestamp:
        return None
    try:
        dt = datetime.fromisoformat(timestamp.replace("Z","+00:00"))
        return dt
    except ValueError:
        return None

def get_interesting_data_by_status(status: int, event: dict) -> dict | None:
    if is_login(status):
        return get_login_data(event)
    if is_only_command(status):
        return get_command_data(event)

    if is_tunneling_data(status):
        return get_tcpip_data(event)
    return None


"""
//////////////////////////////////////////////////GESTIONE LOGIN/////////////////////////////////////////////////////
"""
def is_login(status: int)-> bool:
    return status in [Status.LOGIN_FAILED.value, Status.LOGIN_SUCCESS.value]

def get_login_data(event: dict) -> dict | None:
    return {
        Cleaned_Attr.USER.value : event.get(Useful_Cowrie_Attr.USER.value),
        Cleaned_Attr.PASS.value : event.get(Useful_Cowrie_Attr.PASS.value)
    }

def get_tuple_login_data(event: dict) -> Tuple[str, str]| None:
    user = event.get(Useful_Cowrie_Attr.USER.value)
    password = event.get(Useful_Cowrie_Attr.PASS.value)
    if not user and not password:
        return None
    return str(user),str(password)

def count_logins(statuses: list[int]) -> int:
    if not statuses:
        return 0
    return sum(is_login(status) for status in statuses)
"""
//////////////////////////////////////////////////GESTIONE VERSION///////////////////////////////////////////////////
"""

def is_version(status:int) -> bool:
    return status == Status.VERSION.value

def count_versioning(statuses: list[int]) -> int:
    if not statuses:
        return 0
    return sum(is_version(status) for status in statuses)
"""
//////////////////////////////////////////////////GESTIONE FINGERPRINT///////////////////////////////////////////////
"""
def is_fingerprint(status:int) -> bool:
    return status == Status.FINGERPRINT.value

"""
//////////////////////////////////////////////////GESTIONE TUNNELING/////////////////////////////////////////////////
"""

def is_tunneling(status:int)->bool:
    return status in [Status.TCPIP_DATA.value, Status.TCPIP_REQUEST.value]

def is_tunneling_data(status:int) -> bool:
    return status == Status.TCPIP_DATA.value

def count_tunneling(statuses: list[int]) -> int:
    if not statuses:
        return 0
    return sum(is_tunneling(status) for status in statuses)

def get_tcpip_data(event_dict: dict):
    raw = event_dict.get(Useful_Cowrie_Attr.DATA.value)
    if not raw:
        return {}  # Ritorna dizionario vuoto invece di mille None

    # Normalizzazione a-capo per gestire vari formati di log
    raw = raw.strip("'").replace("\\\\r\\\\n", "\n").replace("\\r\\n", "\n")
    lines = raw.split("\n")

    # Inizializziamo solo ciò che troviamo
    data_extracted = {}
    if lines:
        data_extracted[Cleaned_Attr.MSG.value] = clean_tcip_message(lines[0])

    return data_extracted


def clean_tcip_message(message: str) -> str:
    if not message:
        return ""

    # Pulizia dai wrapper python b'...'
    msg = message.strip()
    if (msg.startswith("b'") or msg.startswith('b"')) and msg.endswith(msg[1]):
        msg = msg[2:-1]

    # 1. Controllo TLS (Handshake)
    for magic, label in TLS_VERSIONS_MAP.items():
        if magic in msg:
            return label

    # 2. Controllo HTTP
    # Spesso nei tunnel HTTP c'è un percorso dopo il verbo (es: GET /index.html)
    parts = msg.split()
    if parts:
        verb_candidate = parts[0].upper()
        if verb_candidate in HTTP_VERBS_MAP:
            return HTTP_VERBS_MAP[verb_candidate]

    return TLS_NOT_KNOWN

"""
//////////////////////////////////////////////////GESTIONE COMANDI///////////////////////////////////////////////////
"""
def is_command(status: int) -> bool:
    return status < 0

def is_only_command(status: int) -> bool:
    return status < 0 and status != Status.TCPIP_DATA.value

def clean_command(message: str) -> list[str]:
    cmd_isolated = _isolate_command(message)
    return _get_multiple_commands(cmd_isolated)

def _isolate_command(message: str) -> str:
    s = message.strip()
    s = re.sub(r'^(Command found:|CMD:|Command not found:)\s*', '', s, flags=re.IGNORECASE)
    return s

def _get_multiple_commands(message: str) -> list[str]:
     return re.split(r';|&&|\|\||\|', message)

def get_command_data(event_dict: dict)-> dict | None:
    msg = event_dict.get(Useful_Cowrie_Attr.MSG.value)
    if not msg:
        return {}
    cmd_clean = clean_command(msg)

    return {
        Cleaned_Attr.MSG.value : cmd_clean
    }


def get_verb_of_command(cmd: str = None, fast_check_set: set[str] = None) -> str: # prendiamo il verbo del comando ovvero : uname -a -> uname
    # strip elimina gli spazi all'inizio e alla fine
    # split divide in sottostringhe secondo un delimitatore. Senza nulla dentro, divide per spazi
    if not cmd:
        return ""
    cmd_clean = cmd.strip()
    # 1. Controlliamo prima i "pezzi grossi"
    if not fast_check_set:
        fast_check_set = get_fast_check_set()
    for long_verb in fast_check_set:
        if cmd_clean.startswith(long_verb):
            return long_verb

    return cmd_clean.split()[0]

