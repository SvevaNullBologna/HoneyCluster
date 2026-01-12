
from dataclasses import dataclass
from difflib import SequenceMatcher

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
        return 0.0

    all_recon = set().union(*_BEHAVIORAL_MAP["reconnaissance"].values() )
    all_exploit = set().union(*_BEHAVIORAL_MAP["exploitation"].values() )

    recon_count = sum(1 for v in verbs if v in all_recon)
    exploit_count = sum(1 for v in verbs if v in all_exploit)

    if exploit_count == 0: # se non c'è exploitation, il rapporto è basato solo sulla recon
        return float(recon_count)

    return recon_count / exploit_count


def get_error_rate(statuses: list[int] = None)-> float:
    if not statuses :
        return 0.0

    return statuses.count(0) / len(statuses)


def get_command_correction_attempts(statuses: list[str] = None, commands: list[str] = None)-> int:

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

def get_verb_of_command(cmd: str = None) -> str: # prendiamo il verbo del comando ovvero : uname -a -> uname
    # strip elimina gli spazi all'inizio e alla fine
    # split divide in sottostringhe secondo un delimitatore. Senza nulla dentro, divide per spazi
    return cmd.strip().split()[0]


