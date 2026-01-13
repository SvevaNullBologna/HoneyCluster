# Quanto è esperto un attaccante in base a una signature
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
    'scanning': {'nmap', 'masscan', 'zmap'},
}

FAST_CHECK_LONGER_VERBS = ('cat /etc/passwd', 'cat /etc/shadow', 'history -c', 'rm -rf', 'unset HISTFILE')

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
    'fingerprinting': 2.8,
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

MAX_SIGNATURE_SCORE = 4.5

TLS_MAGIC = ("\\x16\\x03\\x00", "\\x16\\x03\\x01", "\\x16\\x03\\x02", "\\x16\\x03\\x03")
HTTP_VERBS = ("GET", "POST", "HEAD", "PUT", "CONNECT", "OPTIONS", "PATCH")

TLS_MAGIC_CLEANED = ("TLS_PROBE", "UNKNOW_PROBE")
HTTP_VERBS_CLEANED = ("HTTP_GET", "HTTP_POST", "HTTP_HEAD", "HTTP_PUT", "HTTP_CONNECT", "HTTP_OPTIONS", "HTTP_PATCH")

_BEHAVIORAL_MAP = {
    "reconnaissance": {
        "file_system": {"ls", "cd", "pwd", "which", "find", "du", "stat"},
        "file_reading": {"cat", "head", "tail", "more", "less"},
        "system_info": {"uname", "lscpu", "free", "uptime", "hostname", "df"},
        "user_info": {"whoami", "id", "groups", "last", "w"},
        "processes_env": {"ps", "top", "env", "history", "export", "alias"},
        "network": {"ifconfig", "netstat", "ip", "route", "arp", "ping"},
        "scanning": {"nmap", "masscan", "zmap"},
        "discovery": {"cat /etc/passwd", "cat /etc/shadow"}
    },
    "exploitation": {
        "malware_download": {"wget", "curl", "tftp", "ftp", "scp", "sftp"},
        "priv_esc": {"chmod", "chown", "sudo", "su", "visudo"},
        "scripting_exec": {"sh", "bash", "python", "python3", "perl", "php", "gcc", "make"},
        "persistence_mod": {"crontab", "mkdir", "touch", "echo", "rm", "mv", "cp", "ln"},
        "defense_evasion": {"pkill", "kill", "killall", "unset", "iptables", "ufw"},
        "cleanup": {"history -c", "rm -rf", "unset HISTFILE"}
    }
}


def get_recon_exploit_flat():
    all_recon = set().union(*_BEHAVIORAL_MAP["reconnaissance"].values())
    all_exploit = set().union(*_BEHAVIORAL_MAP["exploitation"].values())

    return all_recon, all_exploit

def get_all_known_verbs():
    verbs = set()

    for sig_set in _SIGNATURES.values():
        verbs.update(sig_set)

    for phase in _BEHAVIORAL_MAP.values():
        for sig_set in phase.values():
            verbs.update(sig_set)

    verbs.update(FAST_CHECK_LONGER_VERBS)

    return verbs