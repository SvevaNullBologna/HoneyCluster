
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

def get_session_duration(start_time: datetime = None, end_time: datetime = None) -> float:
    """ durata totale della sessione in secondi """
    if not start_time or not end_time:
        return 0.0
    return (end_time - start_time).total_seconds()

def get_time_of_day_patterns(start_time : datetime = None) -> float:
    """ otteniamo l'abitudine oraria dell'attaccante, ovvero in che ora della giornata attacca? """
    if not start_time: # se non abbiamo informazioni
        return 0.0

    # il timestamp in zenodo è universale, quindi non dobbiamo attenzionare la posizione geografica
    start_hour = start_time.hour + (start_time.minute / 60.0)
    return start_hour / 24.0

"""
    EXTRACT COMMAND BASED FEATURES
"""





def get_unique_commands_ratio(verbs: list[str] = None)-> float:
    """ quanti comandi diversi ha usato l'attaccante durante una sessione. Si suppone che più ne usi, meno è probabile che si basi di un bot"""
    if not verbs: # non abbiamo informazioni
        return 0.0
    unique_verbs = set(verbs)

    return len(unique_verbs) / len(verbs)

def get_command_diversity_ratio(verbs: list[str] = None, all_known_verbs: set[str] = None)-> float:
    """ quanti strumenti diversi conosce l'attaccante -> più ne conosce più è bravo """
    if not verbs:
        return 0.0

    unique_verbs = set(verbs)
    unknown_verbs = unique_verbs - all_known_verbs # è la differenza tra due insiemi, non tra numeri

    return len(set(unique_verbs))/len(all_known_verbs) + len(unknown_verbs)/len(all_known_verbs) # premiamo quando non conosciamo il verbo usato

def get_tool_signatures(statuses: list[int], verbs: list[str]) -> float:
    """ Calcola lo score di expertise basato sulle firme attivate -> quanto è bravo un attaccante a seconda dei tool usati """
    if not verbs and not statuses:
        return 0.0

    found_signatures = set()

    if verbs:
        verbs_set = set(verbs)
        # Stringa unica per beccare i comandi complessi (es. 'cat /etc/passwd')
        full_session_text = " ".join(verbs).lower()

        for sig_name, sig_commands in _SIGNATURES.items():
            if any(cmd in verbs_set for cmd in sig_commands):
                found_signatures.add(sig_name)

        # TUNNELING (Pattern matching su stringhe magiche)
        if any(magic in full_session_text for magic in ZDR.TLS_MAGIC):
            found_signatures.add("tunneling_tls")

        if any(h_verb.lower() in full_session_text for h_verb in ZDR.HTTP_VERBS):
            found_signatures.add("tunneling_http")

    if statuses:
        login_occurrence = ZDR.count_logins(statuses)
        if len(statuses) > 0 and (login_occurrence / len(statuses) >= 0.6):
            found_signatures.add("login_occurrence")

    if not found_signatures:
        return 0.0

    # Restituiamo la somma dei pesi per differenziare la "bravura"
    return sum(SIGNATURE_WEIGHTS.get(sig, 1.0) for sig in found_signatures)

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
    # in pratica conta il numero di failed . Più è alto il valore, più è scarso l'attaccante, infatti restituiamo il valore negativo
    if not statuses :
        return 0.0

    fail_count = statuses.count(ZDR.Status.LOGIN_FAILED.value) + statuses.count(ZDR.Status.COMMAND_FAILED.value)

    return - fail_count / len(statuses)

def get_command_error_rate(statuses: list[int] = None)-> float:
    # quante volte un comando fallisce? Pi
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


"""
/////////////////////////////////////////////VOCABULARY//////////////////////////////////////////////////
"""

_SIGNATURES = {
    'file_system': {'ls', 'cd', 'pwd', 'which', 'find', 'du', 'stat'},
    'file_reading': {'cat', 'head', 'tail', 'more', 'less'},
    'system_info': {'uname', 'lscpu', 'free', 'uptime', 'hostname', 'df'},
    'user_info': {'whoami', 'id', 'groups', 'last', 'w'},
    'processes_env': {'ps', 'top', 'env', 'history', 'export', 'alias'},
    'network': {'ifconfig', 'netstat', 'ip', 'route', 'arp', 'ping', 'nmap'},

    'malware_download': {'wget', 'curl', 'tftp', 'ftp', 'scp', 'sftp'},
    'priv_esc': {'sudo', 'chmod', 'chown', 'su', 'visudo'},
    'scripting_exec': {'sh', 'bash', 'python', 'python3', 'perl', 'php', 'gcc', 'make'},
    'persistence_mod': {'crontab', 'mkdir', 'touch', 'echo', 'rm', 'mv', 'cp', 'ln'},
    'defense_evasion': {'pkill', 'kill', 'killall', 'unset', 'iptables', 'ufw'},

    # Firme Specifiche (Comandi complessi o pattern)
    'discovery': {'cat /etc/passwd', 'cat /etc/shadow'},  # Qui usiamo stringhe intere
    'cleanup': {'history -c', 'rm -rf', 'unset HISTFILE'},
    'scanning': {'nmap', 'masscan', 'zmap'}
}

# Quanto è esperto un attaccante in base a una signature
SIGNATURE_WEIGHTS = {
    'file_system': 1.0,
    'system_info': 1.2,
    'user_info': 1.2,
    'processes_env': 2.0,
    'file_reading': 2.0,
    'network': 2.2,
    'scanning': 2.2,
    'discovery': 2.5,
    'download': 2.5,
    'malware_download': 3.0,
    'cleanup': 3.0,
    'persistence_mod': 3.5,
    'scripting_exec': 3.5,
    'priv_esc': 3.8,
    'defense_evasion': 4.0,
    'login_occurrence': 1.0,  # Gestito dagli status
    'tunneling_http': 3.5,  # Gestito dai magic bytes
    'tunneling_tls': 4.5  # Gestito dai magic bytes
}

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


def get_all_known_verbs()-> set:
    commands = set()

    # Estrazione da BEHAVIORAL_MAP
    # Usiamo .values() per scendere nei livelli senza dover richiamare le chiavi
    for families in _BEHAVIORAL_MAP.values():
        for cmd_set in families.values():
            commands.update(cmd_set) # update per aggiungere collezioni di elementi alla lista

    # Estrazione da SIGNATURES
    for cmd_set in _SIGNATURES.values():
        commands.update(cmd_set)

    return commands