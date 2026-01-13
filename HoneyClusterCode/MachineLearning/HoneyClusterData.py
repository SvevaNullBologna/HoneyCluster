
from dataclasses import dataclass
from difflib import SequenceMatcher
import Zenodo.ZenodoDataReader as ZDR
from datetime import datetime


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
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
                EXTRACTING FEATURES
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
"""
"""
    EXTRACT TEMPORAL FEATURES
"""

def get_inter_command_timing(command_times: list[datetime] = None) -> float:
    """ otteniamo il tempo medio tra un comando e l'altro """
    if not command_times or len(command_times) < 2:
        return 0.0 # non abbiamo informazioni

    sorted_times = sorted(command_times) # ordiniamo per robustezza (anche se di solito sono già ordinati)

    deltas = [ # lista delle sottrazioni dei tempi dei comandi
        (sorted_times[i+1] - sorted_times[i]).total_seconds()
        for i in range(len(sorted_times) - 1)
    ]

    return sum(deltas) / len(deltas) # restituisce la media

def get_session_duration(start_time: str | datetime = None, end_time: str | datetime = None) -> float:
    """ durata totale della sessione in secondi """
    if not start_time or not end_time:
        return 0.0

    t1 = start_time if isinstance(start_time, datetime) else datetime.fromisoformat(start_time.replace("Z","+00:00"))
    t2 = end_time if isinstance(end_time, datetime) else datetime.fromisoformat(end_time.replace("Z","+00:00"))
    return (t2 - t1).total_seconds()

def get_time_of_day_patterns(start_time : str | datetime = None) -> float:
    """ otteniamo l'abitudine oraria dell'attaccante """
    if not start_time: # se non abbiamo informazioni
        return 0.0

    # il timestamp in zenodo è universale, quindi non dobbiamo attenzionare la posizione geografica
    t1 = start_time if isinstance(start_time, datetime) else datetime.fromisoformat(start_time.replace("Z", "+00:00"))
    start_hour = t1.hour + (t1.minute / 60.0)
    return start_hour / 24.0

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

def get_unique_commands_ratio(commands: list[str] = None)-> float:
    if not commands: # non abbiamo informazioni
        return 0.0
    return len(set(commands)) / len(commands) # quanti comandi unici ci sono sul loro totale. Qui non si considerano gli errori nella scrittura dei comandi ma il 'movimento'

def get_command_diversity_ratio(verbs: list[str] = None)-> float:
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


def get_tool_signatures(verbs: list[str] = None)-> float:
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

def get_reconnaissance_vs_exploitation_ratio(verbs : list[str] = None)-> float:
    """ calcola il rapporto tra verbi di ricerca e verbi di attacco """
    if not verbs:
        return 0.5 # è un valore neutro. 0.0 significa recon, 1.0 significa exploitation

    all_recon = set().union(*_BEHAVIORAL_MAP["reconnaissance"].values() )
    all_exploit = set().union(*_BEHAVIORAL_MAP["exploitation"].values() )

    recon_count = sum(1 for v in verbs if v in all_recon)
    exploit_count = sum(1 for v in verbs if v in all_exploit)

    total = recon_count + exploit_count

    if total == 0:
        return 0.5

    return exploit_count / total


def get_error_rate(statuses: list[int] = None)-> float:
    if not statuses :
        return 0.0

    fail_count = statuses.count(ZDR.Status.LOGIN_FAILED.value) + statuses.count(ZDR.Status.COMMAND_FAILED.value)

    return fail_count / len(statuses)

def get_command_error_rate(statuses: list[int] = None)-> float:
    cmd_statuses = [status for status in statuses if ZDR.is_command(status)]
    if not cmd_statuses:
        return 0.0
    return cmd_statuses.count(ZDR.Status.LOGIN_FAILED.value) / len(cmd_statuses)

def get_command_correction_attempts(statuses:list[int] = None, commands:list[str] = None)-> int:
    # be careful . Each status must belong to each command
    if not statuses or not commands :
        return 0
    if len(statuses) != len(commands) or len(commands) < 2:
        return 0

    corrections = 0

    for i in range(1, len(commands)):
        if statuses[i-1] == ZDR.Status.COMMAND_FAILED.value:
            similiarity = SequenceMatcher(None, commands[i-1], commands[i]).ratio()

            if 0.6 <= similiarity < 1.0:
                corrections += 1
    return corrections


