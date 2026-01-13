from enum import Enum
import re
from datetime import datetime

from MachineLearning.command_vocabularies import FAST_CHECK_LONGER_VERBS, TLS_MAGIC, HTTP_VERBS


class Status(Enum):
    # login (per i bruteforce)
    LOGIN_FAILED = 1
    LOGIN_SUCCESS = 2
    # tool signatures
    VERSION = 3
    FINGERPRINT = 4
    #commands (negative to be able to distinguish them)
    INPUT = -1
    COMMAND_FAILED = -2
    COMMAND_SUCCESS = -3
    # tunneling
    TCPIP_DATA = -4 # contains command, that's why it's negative
    # no important info
    IGNORED = 0


class Event(Enum):
    # login (per i bruteforce)
    LOGIN_FAILED = "cowrie.login.failed"
    LOGIN_SUCCESS = "cowrie.login.success"
    # tool signatures
    VERSION = "cowrie.client.version"
    FINGERPRINT = "cowrie.client.fingerprint"
    # tunneling
    TCPIP_DATA = "cowrie.direct-tcpip.data"
    # commands
    INPUT = "cowrie.command.input"
    COMMAND_FAILED = "cowrie.command.failed"
    COMMAND_SUCCESS = "cowrie.command.success"

class Useful_Cowrie_Attr(Enum):
    EVENTID = "eventid"
    TIME = "timestamp"
    USER = "username"  # when login failed\success, fingerprint
    PASS = "password" # when login failed\success
    MSG = "message" # when Command input, failed, success, itcp request
    VERSION = "ssh_client_version" # when version
    FINGERPRINT = "fingerprint" # when fingerprint
    DATA = "data" # when itcp data
    GEO = "geolocation_data"

class Cleaned_Attr(Enum):
    STATUS = "status"
    TIME = "timestamp"
    START_TIME = "session_start"
    END_TIME = "session_end"
    COUNT = "raw_event_count"
    EVENTS = "events"
    MSG = "message"  # when Command input, failed, success, itcp request
    DATA = "data"  # when itcp data

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

def is_tunneling_data(status:int) -> bool:
    return status == Status.TCPIP_DATA.value

def count_tunneling(statuses: list[int]) -> int:
    if not statuses:
        return 0
    return sum(is_tunneling_data(status) for status in statuses)

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
    msg = message.strip()

    if msg.startswith("b'") and msg.endswith("'"):
        msg = msg[2:-1]

    if msg.startswith(TLS_MAGIC):
        return "TLS_PROBE"

    for verb in HTTP_VERBS: # sappiamo che è HTTP
        if msg.startswith(verb + " "):
            return f"HTTP_{verb}"

    return "UNKNOWN_PROBE"

"""
//////////////////////////////////////////////////GESTIONE COMANDI///////////////////////////////////////////////////
"""
def is_command(status: int) -> bool:
    return status < 0

def is_only_command(status: int) -> bool:
    return status < 0 and status != Status.TCPIP_DATA.value

def isolate_command(message: str) -> str:
    s = message.strip()
    s = re.sub(r'^(Command found:|CMD:|Command not found:)\s*', '', s, flags=re.IGNORECASE)
    return s

def get_command_data(event_dict: dict)-> dict | None:
    msg = event_dict.get(Useful_Cowrie_Attr.MSG.value)
    if not msg:
        return {}
    return {
        Cleaned_Attr.MSG.value : isolate_command(msg)
    }


def get_verb_of_command(cmd: str = None) -> str: # prendiamo il verbo del comando ovvero : uname -a -> uname
    # strip elimina gli spazi all'inizio e alla fine
    # split divide in sottostringhe secondo un delimitatore. Senza nulla dentro, divide per spazi
    if not cmd:
        return ""
    cmd_clean = cmd.strip()
    # 1. Controlliamo prima i "pezzi grossi"
    for long_verb in FAST_CHECK_LONGER_VERBS:
        if cmd_clean.startswith(long_verb):
            return long_verb

    return cmd_clean.split()[0]
