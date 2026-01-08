from dataclasses import dataclass
from pathlib import Path
import pandas as pd
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
    reconnaissance_on_exploitation_ratio: float # da comprendere
    error_rate: float
    command_correction_attempts: int


def get_info_from_json(json_folder_path: Path) -> pd.DataFrame:
    pass

"""
    EXTRACT TEMPORAL FEATURES
"""

def get_inter_command_timing(command_times: list[datetime]) -> float:
    pass

def get_session_duration(times: list[datetime]) -> float:
    pass

def get_time_of_day_patterns(times: list[datetime]) -> float:
    pass

"""
    EXTRACT COMMAND BASED FEATURES
"""

def get_unique_commands_ratio(commands: list[str])-> float:
    pass

def get_command_diversity_ratio(commands: list[str])-> float:
    pass

def get_tool_signatures(commands: list[str])-> float:
    pass

"""
    EXTRACT BEHAVIORAL PATTERNS 
"""

def get_reconnaissance_on_exploitation_ratio(eventids : list[str])-> float:
    pass

def get_error_rate(eventids: list[str], commands: list[str])-> float:
    pass

def get_command_correction_attempts(eventids: list[str], commands: list[str])-> float:
    pass



"""

    def _set_data_from_events(self, events : list[ZenodoEvent]) :
        self.number_of_events = len(events)
        self.number_of_commands = self.number_of_successes = self.number_of_failures = 0
        self.normalized_command_list = []
        country_codes = set()
        times = []

        for e in events:
            status = e.is_command()
            if status >= 0 :
                self.number_of_commands += 1
                if e.message:
                    self.normalized_command_list.append(e.normalize_command(e.message))
            if status == 1:
                self.number_of_successes += 1
            elif status == 0:
                self.number_of_failures += 1

            if e.geolocation_data and e.geolocation_data.country_code2:
                country_codes.add(e.geolocation_data.country_code2)

            t = e.get_time()
            if t is not None:
                times.append(t)

        self.geo_variability = len(country_codes)
        if self.geo_variability == 1:
            self.country_code = next(iter(country_codes))
        else:
            self.country_code = None

        if len(times) < 2:
            self.duration = None
        else:
            self.duration = (max(times) - min(times)).total_seconds()

    def get_command_diversity(self):
        return len(set(self.normalized_command_list))

    def get_success_ratio(self):
        return self.number_of_successes / self.number_of_commands if self.number_of_commands > 0 else 0

    def get_command_rate(self):
        return self.number_of_commands / self.duration if self.duration and self.duration > 0 else 0

    def get_temporal_dispersion(self):
        return self.number_of_events / self.duration if self.duration else 0

    def __init__(self, date: datetime, zenodo_session: ZenodoSession):
        self.date_of_log = date
        self._set_data_from_events(zenodo_session.events)



    def get_machine_learning_vector(self):
        return \
            [
                self.duration or 0.0, # descrive: persistenza dell'attaccante
                self.number_of_events, # attività complessiva
                self.number_of_commands, # interazione diretta
                self.get_success_ratio(), # efficacia
                self.get_command_rate(), # aggressività
                self.geo_variability, # stabilità geografica (se siamo di fronte a qualche VPN)
                self.get_command_diversity(), # varietà dei comandi
                self.get_temporal_dispersion() # densità temporale
            ]

    def to_feature_dict(self) -> dict:
        return {
            "duration": self.duration or 0.0,
            "n_events": self.number_of_events,
            "n_commands": self.number_of_commands,
            "success_ratio": self.get_success_ratio(),
            "command_rate": self.get_command_rate(),
            "geo_variability": self.geo_variability,
            "command_diversity": self.get_command_diversity(),
            "temporal_dispersion": self.get_temporal_dispersion()
        }



"""