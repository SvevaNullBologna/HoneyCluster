from enum import Enum

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
    TCPIP_REQUEST = 5  # it's a direct TCP connection attempt
    TCPIP_DATA = -4 # contains command, that's why it's negative. It's also a tool signature

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
    EVENTS = "events"
    USER = "username"
    PASS = "password"
    MSG = "message"  # when Command input, failed, success, itcp request
    DATA = "data"  # when itcp data
