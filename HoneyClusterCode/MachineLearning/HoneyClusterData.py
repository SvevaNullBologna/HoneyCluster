
from dataclasses import dataclass
from difflib import SequenceMatcher
from typing import Tuple

from MachineLearning.command_vocabularies import MAX_SIGNATURE_SCORE
from MachineLearning.command_vocabularies import _SIGNATURES, SIGNATURE_WEIGHTS


from Zenodo import ZenodoDataReader as ZDR
from datetime import datetime
import math

from Zenodo.ZenodoDataReader import is_only_command, is_login, is_fingerprint, is_version

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
    time_of_day_patterns_sin: float
    time_of_day_patterns_cos: float # codifica dell'abitudine temporale
    # command based features
    unique_commands_ratio : float
    command_diversity_ratio : float
    tool_signatures : float # presenza di famiglie di comandi per esempio: scanning -> nmap, ifconfig, netstat ; download -> wget, curl ; privilege escalation -> sudo, chmod
    # behavioral patterns
    reconnaissance_vs_exploitation_ratio: float #numero comandi di esplorazione vs numero comandi attivi e intrusivi
    error_rate: float # comandi errati / comandi
    command_correction_attempts: float # quante volte in media l'attaccante cerca di correggersi

"""
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
                EXTRACTING FEATURES
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
"""
"""
    EXTRACT TEMPORAL FEATURES
"""

def get_inter_command_timing(command_times: list[datetime] = None) -> float: # consideriamo in questo caso anche fingerprint, tunneling, version e login -> tutti gli eventi interessanti già selezionati nella fase di cleaning
    """ otteniamo il tempo medio tra un comando e l'altro """
    if not command_times or len(command_times) < 2:
        return 0.0 # non abbiamo informazioni

    sorted_times = sorted(command_times) # ordiniamo per robustezza (anche se di solito sono già ordinati)

    deltas = [ # lista delle sottrazioni dei tempi dei comandi
        (sorted_times[i+1] - sorted_times[i]).total_seconds()
        for i in range(len(sorted_times) - 1)
    ]

    avg_delta = sum(deltas) / len(deltas) # restituisce la media
    return math.log1p(avg_delta)

def get_session_duration(start_time: datetime = None, end_time: datetime = None) -> float:
    """ durata totale della sessione in secondi """
    if not start_time or not end_time:
        return 0.0
    seconds = (end_time - start_time).total_seconds()
    return math.log1p(seconds)

def get_time_of_day_patterns(start_time : datetime = None) -> Tuple[float,float]:
    """ otteniamo l'abitudine oraria dell'attaccante, ovvero in che ora della giornata attacca?
        Viene restituito l'orario su un cerchio unitario per evitare il problema 23:59 -> 0.99 , 00:01 -> 0.0007
     """
    if not start_time: # se non abbiamo informazioni
        return 0.0, 0.0

    # il timestamp in zenodo è universale, quindi non dobbiamo attenzionare la posizione geografica
    hour_decimal = start_time.hour + (start_time.minute / 60.0) + (start_time.second / 3600.0)
    angle = math.tau * (hour_decimal / 24.0)
    return math.sin(angle), math.cos(angle)

"""
    EXTRACT COMMAND BASED FEATURES
"""


def get_unique_commands_ratio(verbs: list[str] = None)-> float: #solo tunneling e comandi effettivi ssh
    """ quanti comandi diversi ha usato l'attaccante durante una sessione. Si suppone che più ne usi, meno è probabile che si basi di un bot"""
    if not verbs: # non abbiamo informazioni
        return 0.0
    unique_verbs = set(verbs)

    return len(unique_verbs) / len(verbs)

def get_command_diversity_ratio(verbs: list[str] = None, all_known_verbs: set[str] = None, bonus: float = 0.3)-> float:
    """ quanti strumenti diversi conosce l'attaccante -> più ne conosce più è bravo """
    if not verbs or not all_known_verbs: return 0.0

    unique_verbs = set(verbs)
    if not unique_verbs:
        return 0.0

    known_count = sum(1 for v in unique_verbs if v in all_known_verbs)
    unknown_count = len(unique_verbs) - known_count

    base_diversity = len(unique_verbs)/len(verbs)
    rarity_bonus = (unknown_count * bonus) / len(unique_verbs)
    return min(base_diversity + rarity_bonus, 1.0)# premiamo quando non conosciamo il verbo usato

def get_tool_signatures(statuses: list[int], verbs: list[str]) -> float:
    """ Calcola lo score di expertise basato sulle firme attivate -> quanto è bravo un attaccante a seconda dei tool usati """
    if not verbs and not statuses:
        return 0.0

    found_signatures = set()

    if statuses:
        for status in statuses :
            if is_fingerprint(status):
                found_signatures.add("fingerprinting")

            if is_version(status):
                found_signatures.add("versioning")

            if ZDR.Status.TCPIP_REQUEST.value in statuses:
                found_signatures.add("tunneling_request")

            login_occurrence = ZDR.count_logins(statuses)
            if len(statuses) > 0 and (login_occurrence / len(statuses) >= 0.6):
                found_signatures.add("login_occurrence")

    if verbs: #ci sono
        verbs_set = set(verbs)

        for sig_name, sig_commands in _SIGNATURES.items():
            if verbs_set.intersection(sig_commands):
                found_signatures.add(sig_name)

        # TUNNELING (Pattern matching su stringhe magiche)
        for v in verbs_set:
            if v in SIGNATURE_WEIGHTS:  # Se il verbo è una delle chiavi pesate (es. TLS_1.2)
                found_signatures.add(v)

    if not found_signatures:
        return 0.0

    # Restituiamo la somma dei pesi per differenziare la "bravura"
    return sum(SIGNATURE_WEIGHTS.get(sig, 1.0) for sig in found_signatures) / MAX_SIGNATURE_SCORE

"""
    EXTRACT BEHAVIORAL PATTERNS 
"""

def get_reconnaissance_vs_exploitation_ratio(statuses: list[int], verbs: list[str], all_recon: set, all_exploit: set) -> float:
    """ Calcola il rapporto tra attività di ricognizione e attacco """
    recon_count = 0
    exploit_count = 0

    if statuses:
        recon_count += ZDR.count_versioning(statuses)
        recon_count += ZDR.count_logins(statuses)
        exploit_count += ZDR.count_tunneling(statuses)

    if verbs:
        for v in verbs:
            if v in all_recon:
                recon_count += 1
            elif v in all_exploit:
                exploit_count += 1
            # I probe di tunneling (TLS/HTTP) sono tecnicamente ricognizione/tunneling
            elif v.startswith("TLS_") or v.startswith("HTTP_") or v == "UNKNOWN_PROBE":
                exploit_count += 1 # Il tunneling è considerato un'azione attiva (exploitation)

    total = recon_count + exploit_count
    if total == 0:
        return 0.5

    return exploit_count / total


def get_error_rate(statuses: list[int] = None)-> float:
    # in pratica conta il numero di failed . Più è alto il valore, più è scarso l'attaccante, infatti restituiamo il valore negativo
    if not statuses :
        return 0.0

    attempts = [status for status in statuses if ZDR.is_only_command(status) or ZDR.is_login(status)]
    if not attempts:
        return 0.5

    successes = [s for s in attempts if s!= ZDR.Status.COMMAND_FAILED.value and ZDR.Status.LOGIN_FAILED.value]
    return len(successes) / len(attempts)


def get_command_correction_attempts(statuses: list[int], united_commands: list[str], login_data: list[tuple[str]]) -> float:
    # 1. Creiamo una lista unificata filtrata, ma mantenendo l'ordine temporale
    # Usiamo gli indici per evitare i problemi dello zip
    valid_events = []

    # Indici separati per scorrere commands e login_data solo quando troviamo l'evento giusto
    cmd_idx = 0
    login_idx = 0

    for s in statuses:
        if is_only_command(s):
            # Se è un comando, prendiamo il testo del comando corrispondente
            if cmd_idx < len(united_commands):
                # Aggiungiamo (stato, comando, None)
                valid_events.append((s, united_commands[cmd_idx], None))
                cmd_idx += 1
        elif is_login(s):
            # Se è un login, prendiamo i dati del login corrispondenti
            if login_idx < len(login_data):
                # Aggiungiamo (stato, None, dati_login)
                valid_events.append((s, None, login_data[login_idx]))
                login_idx += 1

    if len(valid_events) < 2:
        return 0.0

    corrections = 0
    # Il denominatore dovrebbe essere il numero di tentativi (non tutti gli eventi)
    total_relevant_events = len(valid_events)

    for i in range(1, total_relevant_events):
        prev_status, prev_cmd, prev_login = valid_events[i - 1]
        curr_status, curr_cmd, curr_login = valid_events[i]

        # --- CORREZIONE COMANDO ---
        # Verifichiamo che entrambi siano comandi (non None)
        if prev_status == ZDR.Status.COMMAND_FAILED.value and prev_cmd and curr_cmd:
            len_diff = abs(len(prev_cmd) - len(curr_cmd))
            command_similarity = SequenceMatcher(None, prev_cmd, curr_cmd).ratio()

            if (len(prev_cmd) <= 3 and len_diff <= 1) or (command_similarity >= 0.70):
                if prev_cmd != curr_cmd:
                    corrections += 1

        # --- CORREZIONE LOGIN ---
        # Verifichiamo che entrambi siano login (non None)
        elif prev_status == ZDR.Status.LOGIN_FAILED.value and prev_login and curr_login:
            user_similarity = SequenceMatcher(None, prev_login[0], curr_login[0]).ratio()
            password_similarity = SequenceMatcher(None, prev_login[1], curr_login[1]).ratio()

            if ((0.7 <= user_similarity < 1.0 and password_similarity >= 0.9) or
                    (user_similarity == 1.0 and 0.7 <= password_similarity < 1.0)):
                corrections += 1

    return corrections / total_relevant_events

