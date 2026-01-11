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
    TCPIP_REQUEST = 5
    TCPIP_DATA = 6
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

class Login_Attr(Enum):
    USER = "username"
    PASS = "password"

class Command_Attr(Enum):
    MSG = "message"

class Version_Attr(Enum):
    VERSION = "ssh_client_version"

class Fingerprint_Attr(Enum):
    KEY = "key"
    TYPE = "type"
    USERNAME = "username"
    FINGERPRINT = "fingerprint"

class Tunnel_Attr(Enum):
    DST_IP = "dst_ip_identifier"
    DATA = "data"


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
    if is_tunneling(status):
        return get_tcpip_data(event)

    return None


"""
//////////////////////////////////////////////////GESTIONE LOGIN/////////////////////////////////////////////////////
"""
def is_login(status: int)-> bool:
    return status in [Status.LOGIN_FAILED.value, Status.LOGIN_SUCCESS.value]

def get_login_data(event_dict: dict):
    return {
        Login_Attr.USER.value: event_dict.get(Login_Attr.USER.value),
        Login_Attr.PASS.value: event_dict.get(Login_Attr.PASS.value)
    }
"""
//////////////////////////////////////////////////GESTIONE VERSION///////////////////////////////////////////////////
"""

def is_version(status:int) -> bool:
    return status == Status.VERSION.value

def get_version_data(event_dict: dict) -> dict:
    v = event_dict.get(Version_Attr.VERSION.value)
    if v:
        # Pulizia della stringa b'SSH-2.0...' -> SSH-2.0...
        v = clean_version_data(v)
        return {"version": v}
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
        "fp": event_dict.get(Fingerprint_Attr.FINGERPRINT.value),
        "key_type": event_dict.get(Fingerprint_Attr.TYPE.value),
        "user": event_dict.get(Fingerprint_Attr.USERNAME.value)
    }

"""
//////////////////////////////////////////////////GESTIONE TUNNELING/////////////////////////////////////////////////
"""
def is_tunneling(status:int) -> bool:
    return status in [Status.TCPIP_REQUEST.value ,Status.TCPIP_DATA.value]

def get_tcpip_data(event_dict: dict) -> dict:
    return {
        "dst": event_dict.get(Tunnel_Attr.DST_IP.value),
        "data_len": len(str(event_dict.get(Tunnel_Attr.DATA.value))) if event_dict.get(Tunnel_Attr.DATA.value) else 0
    }
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
    msg = event_dict.get(Command_Attr.MSG.value)
    return {
        "msg" : isolate_command(msg)
    }
