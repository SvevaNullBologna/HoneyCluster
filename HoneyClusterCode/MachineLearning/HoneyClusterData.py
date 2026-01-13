
from dataclasses import dataclass
from difflib import SequenceMatcher

from MachineLearning.command_vocabularies import TLS_MAGIC_CLEANED, HTTP_VERBS_CLEANED
from command_vocabularies import _SIGNATURES, SIGNATURE_WEIGHTS


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

def get_command_diversity_ratio(verbs: list[str] = None, all_known_verbs: set[str] = None, bonus: float = 0.3)-> float:
    """ quanti strumenti diversi conosce l'attaccante -> più ne conosce più è bravo """
    if not verbs or not all_known_verbs: return 0.0

    unique_verbs = set(verbs)
    unknown_verbs = unique_verbs - all_known_verbs # è la differenza tra due insiemi, non tra numeri
    known_verbs = unique_verbs - unknown_verbs
    return len(known_verbs)/len(all_known_verbs) + bonus * len(unknown_verbs)/len(all_known_verbs) # premiamo quando non conosciamo il verbo usato

def get_tool_signatures(statuses: list[int], verbs: list[str]) -> float:
    """ Calcola lo score di expertise basato sulle firme attivate -> quanto è bravo un attaccante a seconda dei tool usati """
    if not verbs and not statuses:
        return 0.0

    found_signatures = set()

    if verbs:
        verbs_set = set(verbs)

        for sig_name, sig_commands in _SIGNATURES.items():
            if any(cmd in verbs_set for cmd in sig_commands):
                found_signatures.add(sig_name)

        # TUNNELING (Pattern matching su stringhe magiche)
        if any(v in TLS_MAGIC_CLEANED for v in verbs):
            found_signatures.add("tunneling_tls")

        if any(v in HTTP_VERBS_CLEANED for v in verbs):
            found_signatures.add("tunneling_http")

    if statuses:
        if ZDR.Status.FINGERPRINT.value in statuses:
            found_signatures.add("fingerprinting")

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

def get_reconnaissance_vs_exploitation_ratio(statuses: list[int], verbs : list[str], all_recon: set, all_exploit: set)-> float:
    """ calcola il rapporto tra verbi di ricerca e verbi di attacco """
    recon_count = 0
    exploit_count = 0

    if statuses:
        recon_count += ZDR.count_versioning(statuses)
        recon_count += ZDR.count_logins(statuses)
        exploit_count += ZDR.count_tunneling(statuses)

    if verbs:
        recon_count += sum(1 for v in verbs if v in all_recon)
        exploit_count += sum(1 for v in verbs if v in all_exploit)

    total = recon_count + exploit_count

    if total == 0:
        return 0.5

    return exploit_count / total


def get_error_rate(statuses: list[int] = None)-> float:
    # in pratica conta il numero di failed . Più è alto il valore, più è scarso l'attaccante, infatti restituiamo il valore negativo
    if not statuses :
        return 0.0

    attempts = [status for status in statuses if ZDR.is_only_command(status) or ZDR.is_login(status)]
    # ignoriamo versioning e tunneling perché non sappiamo determinare se sono andati a buon fine
    if not attempts:
        return 0.0

    failures = [attempt for attempt in attempts if attempt == ZDR.Status.COMMAND_FAILED.value or attempt == ZDR.Status.LOGIN_FAILED.value]

    return - (len(failures) / len(attempts))

def get_command_correction_attempts(statuses:list[int], commands:list[str])-> int:
    # be careful . Each status must belong to each command
    if not statuses or not commands or len(statuses)  < 2:
        return 0

    corrections = 0

    has_eventually_logged_in = ZDR.Status.LOGIN_SUCCESS.value in statuses

    for i in range(1, len(commands)):
        prev_status = statuses[i-1]
        curr_status = statuses[i]
        prev_cmd = commands[i-1]
        curr_cmd = commands[i]

        if has_eventually_logged_in:
            if curr_status == ZDR.Status.LOGIN_FAILED.value:
                corrections += 1

        if ZDR.is_only_command(prev_status) and prev_status == ZDR.Status.COMMAND_FAILED.value:
            similiarity = SequenceMatcher(None, prev_cmd, curr_cmd).ratio()
            if 0.7 <= similiarity < 1.0:
                corrections += 1
    return corrections


