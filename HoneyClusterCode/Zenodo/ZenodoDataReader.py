from enum import Enum
import re
from datetime import datetime



class Status(Enum):
    # login (per i bruteforce)
    LOGIN_FAILED = 1
    LOGIN_SUCCESS = 2
    # tool signatures
    VERSION = 3
    FINGERPRINT = 4
    # tunneling
    TCPIP_REQUEST = 5 # dst = null, data_len = 0
    TCPIP_DATA = 6 # same
    #commands (negative to be able to distinguish them)
    INPUT = -1
    COMMAND_FAILED = -2
    COMMAND_SUCCESS = -3

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
    TCPIP_REQUEST = "cowrie.direct-tcpip.request"
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
    GEO = "geo"
    USER = "username"  # when login failed\success, fingerprint
    PASS = "password"  # when login failed\success
    MSG = "message"  # when Command input, failed, success, itcp request
    VERSION = "ssh_client_version"  # when version
    FINGERPRINT = "fingerprint"  # when fingerprint
    DATA = "data"  # when itcp data
    DST_ID = "dst_ip_identifier" # when itcp request
    DST_PORT = "dst_port" # when itcp request
    HOST = "host" # when itcp data
    USER_AGENT = "user_agent" # when itcp data
    ACCEPT = "accept" # when itcp data

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

def get_data_by_status(status: int, event: dict) -> dict | None:
    if is_login(status):
        return get_login_data(event)

    if is_command(status):
        return get_command_data(event)

    if is_version(status):
        return get_version_data(event)
    if is_fingerprint(status):
        return get_fingerprint_data(event)
    if is_tunneling_request(status):
        return get_tcpip_request_data(event)
    if is_tunneling_data(status):
        return get_tcpip_data_data(event)
    return None


"""
//////////////////////////////////////////////////GESTIONE LOGIN/////////////////////////////////////////////////////
"""
def is_login(status: int)-> bool:
    return status in [Status.LOGIN_FAILED.value, Status.LOGIN_SUCCESS.value]

def get_login_data(event_dict: dict):
    return {
        Cleaned_Attr.USER.value: event_dict.get(Useful_Cowrie_Attr.USER.value),
        Cleaned_Attr.PASS.value: event_dict.get(Useful_Cowrie_Attr.PASS.value)
    }
"""
//////////////////////////////////////////////////GESTIONE VERSION///////////////////////////////////////////////////
"""

def is_version(status:int) -> bool:
    return status == Status.VERSION.value

def get_version_data(event_dict: dict) -> dict:
    v = event_dict.get(Useful_Cowrie_Attr.VERSION.value)
    if v:
        # Pulizia della stringa b'SSH-2.0...' -> SSH-2.0...
        v = clean_version_data(v)
        return {Useful_Cowrie_Attr.VERSION.value: v}
    return {}

def clean_version_data(version: str) -> str:
   return  re.sub(r"^b['\"]|['\"]$", "", str(version))
"""
//////////////////////////////////////////////////GESTIONE FINGERPRINT///////////////////////////////////////////////
"""
def is_fingerprint(status:int) -> bool:
    return status == Status.FINGERPRINT.value

def get_fingerprint_data(event_dict: dict) -> dict:
    return {
        Cleaned_Attr.FINGERPRINT.value: event_dict.get(Useful_Cowrie_Attr.FINGERPRINT.value),
        Cleaned_Attr.USER.value : event_dict.get(Useful_Cowrie_Attr.USER.value)
    }

"""
//////////////////////////////////////////////////GESTIONE TUNNELING/////////////////////////////////////////////////
"""
def is_tunneling_request(status:int) -> bool:
    return status == Status.TCPIP_REQUEST.value
def is_tunneling_data(status:int) -> bool:
    return status == Status.TCPIP_DATA.value

def get_tcpip_request_data(event_dict: dict):
    # Cerca il pattern 'to [ID/IP]:[PORTA] from'
    match = re.search(r'to\s+([^:]+):(\d+)\s+from', event_dict.get(Useful_Cowrie_Attr.MSG.value))
    if not match:
        return {
            Cleaned_Attr.DST_ID.value : None,
            Cleaned_Attr.DST_PORT.value: None
        }

    target_id = match.group(1)
    target_port = int(match.group(2))
    return {
        Cleaned_Attr.DST_ID.value : target_id,
        Cleaned_Attr.DST_PORT.value : target_port
    }


def get_tcpip_data_data(event_dict: dict):
    raw = event_dict.get(Useful_Cowrie_Attr.DATA.value)
    if not raw:
        return {}  # Ritorna dizionario vuoto invece di mille None

    # Normalizzazione a-capo per gestire vari formati di log
    raw = raw.strip("'").replace("\\\\r\\\\n", "\n").replace("\\r\\n", "\n")
    lines = raw.split("\n")

    # Inizializziamo solo ciò che troviamo
    data_extracted = {}
    if lines:
        data_extracted[Cleaned_Attr.MSG.value] = lines[0]

    for line in lines:
        line_lower = line.lower()
        if line_lower.startswith("host:"):
            data_extracted[Cleaned_Attr.HOST.value] = line.split(":", 1)[1].strip()
        elif line_lower.startswith("user-agent:"):
            data_extracted[Cleaned_Attr.USER_AGENT.value] = line.split(":", 1)[1].strip()
        elif line_lower.startswith("accept:"):
            data_extracted[Cleaned_Attr.ACCEPT.value] = line.split(":", 1)[1].strip()

    return data_extracted

"""
//////////////////////////////////////////////////GESTIONE COMANDI///////////////////////////////////////////////////
"""
def is_command(status: int) -> bool:
    return status < 0

def isolate_command(message: str) -> str:
    s = message.strip()
    s = re.sub(r'^(Command found:|CMD:|Command not found:)\s*', '', s, flags=re.IGNORECASE)
    return s

def get_command_data(event_dict: dict)-> dict | None:
    msg = event_dict.get(Useful_Cowrie_Attr.MSG.value)
    return {
        Cleaned_Attr.MSG.value : isolate_command(msg)
    }
