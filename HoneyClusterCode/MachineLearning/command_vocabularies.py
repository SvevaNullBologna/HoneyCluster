# ==============================================================================
# VOCABOLARI E FIRME PER ANALISI COMPORTAMENTALE COWRIE
# ==============================================================================

# 1. Mappatura Firme per Expertise Score
_SIGNATURES = {
    'file_system': {
        'ls', 'cd', 'pwd', 'which', 'find', 'du', 'stat', 'ls -la'
    },
    'file_reading': {
        'cat', 'head', 'tail', 'more', 'less'
    },
    'system_info': {
        'uname', 'lscpu', 'free', 'uptime', 'hostname', 'df',
        'uname -a', 'cat /proc/cpuinfo', 'cat /proc/meminfo', 'cat /etc/issue'
    },
    'user_info': {
        'whoami', 'id', 'groups', 'last', 'w'
    },
    'processes_env': {
        'ps', 'top', 'env', 'history', 'export', 'alias',
        'export HISTSIZE=0', 'export HISTFILESIZE=0', 'export HISTFILE=/dev/null'
    },
    'network': {
        'ifconfig', 'netstat', 'ip', 'route', 'arp', 'ping',
        'ip addr show', 'netstat -antp', 'netstat -tulpn'
    },
    'malware_download': {
        'wget', 'curl', 'tftp', 'ftp', 'scp', 'sftp'
    },
    'priv_esc': {
        'sudo', 'chmod', 'chown', 'su', 'visudo'
    },
    'scripting_exec': {
        'sh', 'bash', 'python', 'python3', 'perl', 'php', 'gcc', 'make'
    },
    'persistence_mod': {
        'crontab', 'mkdir', 'touch', 'echo', 'rm', 'mv', 'cp', 'ln',
        'mkdir /root/.ssh', 'touch .ssh/authorized_keys',
        'cat >> .ssh/authorized_keys', 'echo "ssh-rsa', 'crontab -l', 'crontab -e'
    },
    'defense_evasion': {
        'pkill', 'kill', 'killall', 'unset', 'iptables', 'ufw',
        'unset HISTFILE', 'unset HISTORY', 'pkill -f', 'killall -9',
        'iptables -L', 'ufw status', 'base64 --decode', 'base64 -d'
    },
    'discovery': {
        'cat /etc/passwd', 'cat /etc/shadow'
    },
    'cleanup': {
        'history -c', 'history -w', 'history -n', 'rm -rf',
        'cat /dev/null', 'rm -rf /var/log', 'rm -rf ~/.bash_history', 'rm -rf /tmp/*'
    },
    'scanning': {
        'nmap', 'masscan', 'zmap'
    }
}

# 2. Verbi composti per il Parser (da controllare prima dei singoli verbi)
_FAST_CHECK_LONGER_VERBS = (
    'cat /dev/null', 'history -c', 'history -w', 'history -n',
    'unset HISTFILE', 'unset HISTORY', 'export HISTSIZE=0', 'export HISTFILESIZE=0',
    'rm -rf /var/log', 'rm -rf ~/.bash_history', 'rm -rf /tmp/*',
    'cat /etc/passwd', 'cat /etc/shadow', 'cat /etc/issue',
    'cat /proc/cpuinfo', 'cat /proc/meminfo', 'uname -a', 'ls -la',
    'mkdir /root/.ssh', 'touch .ssh/authorized_keys', 'cat >> .ssh/authorized_keys',
    'echo "ssh-rsa', 'crontab -l', 'crontab -e',
    'ip addr show', 'netstat -antp', 'netstat -tulpn', 'iptables -L', 'ufw status',
    'rm -rf', 'pkill -f', 'killall -9', 'base64 --decode', 'base64 -d'
)

def get_fast_check_set():
    """
        Ritorna la lista dei verbi lunghi ordinata per lunghezza decrescente.
        L'ordinamento è CRUCIALE per evitare che un prefisso più corto
        (es. 'rm') mangi un comando più specifico (es. 'rm -rf /var/log').
        """
    unique_fast_check = set(_FAST_CHECK_LONGER_VERBS)
    return sorted(unique_fast_check, key=len, reverse=True)

# 3. Pesi per il Clustering (Normalizzati su MAX_SIGNATURE_SCORE)
SIGNATURE_WEIGHTS = {
    'file_system': 1.0,
    'system_info': 1.2,
    'user_info': 1.2,
    'processes_env': 2.0,
    'file_reading': 2.0,
    'network': 2.2,
    'scanning': 2.2,
    'discovery': 2.5,
    'malware_download': 3.0,
    'cleanup': 3.0,
    'persistence_mod': 3.5,
    'scripting_exec': 3.5,
    'priv_esc': 3.8,
    'defense_evasion': 4.0,
    # Eventi specializzati
    'login_occurrence': 1.0,
    'versioning': 2.6,
    'fingerprinting': 2.8,
    'tunneling_request': 3.0,
    # Protocolli Tunneling (da mappatura TLS/HTTP)
    'HTTP_GET': 3.2, 'HTTP_POST': 3.5, 'HTTP_CONNECT': 3.8,
    'TLS_1.0': 3.8, 'TLS_1.1': 4.0, 'TLS_1.2': 4.5,
    'TLS_v3_RECORD': 4.0,
    'UNKNOWN_PROBE': 1.5
}

MAX_SIGNATURE_SCORE = 4.5

# 4. Mappature Tunneling (TCP-IP Data)
TLS_VERSIONS_MAP = {
    "\\x16\\x03\\x01": "TLS_1.0",
    "\\x16\\x03\\x02": "TLS_1.1",
    "\\x16\\x03\\x03": "TLS_1.2",
    "\\x16\\x03\\x00": "TLS_v3_RECORD",
    "\x16\x03\x01": "TLS_1.0",
    "\x16\x03\x02": "TLS_1.1",
    "\x16\x03\x03": "TLS_1.2",
}

HTTP_VERBS_MAP = {v: f"HTTP_{v}" for v in ["GET", "POST", "HEAD", "PUT", "CONNECT", "OPTIONS", "PATCH"]}
TLS_NOT_KNOWN = "UNKNOWN_PROBE"

# 5. Mappa Comportamentale (Recon vs Exploit)
_BEHAVIORAL_MAP = {
    "reconnaissance": {
        "file_system", "file_reading", "system_info", "user_info",
        "processes_env", "network", "scanning", "discovery"
    },
    "exploitation": {
        "malware_download", "priv_esc", "scripting_exec",
        "persistence_mod", "defense_evasion", "cleanup"
    }
}


# ==============================================================================
# FUNZIONI DI UTILITÀ
# ==============================================================================

def get_recon_exploit_flat():
    """ Ritorna due set piatti di verbi per il calcolo del ratio """
    all_recon = set()
    for cat in _BEHAVIORAL_MAP["reconnaissance"]:
        all_recon.update(_SIGNATURES.get(cat, set()))

    all_exploit = set()
    for cat in _BEHAVIORAL_MAP["exploitation"]:
        all_exploit.update(_SIGNATURES.get(cat, set()))

    return all_recon, all_exploit


def get_all_known_verbs():
    """ Ritorna tutti i verbi e i pattern conosciuti dal sistema """
    verbs = set()
    for sig_set in _SIGNATURES.values():
        verbs.update(sig_set)
    verbs.update(get_fast_check_set())
    return verbs