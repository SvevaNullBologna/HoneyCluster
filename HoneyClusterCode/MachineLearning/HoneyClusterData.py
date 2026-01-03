from dataclasses import dataclass
from datetime import datetime

from Zenodo.ZenodoDataStructure import ZenodoEvent
from Zenodo.ZenodoDataStructure import ZenodoSession

@dataclass
class HoneyClusterSession:
    date : datetime
    duration : float | None
    normalized_command_list : list[str] | None
    number_of_events : int
    number_of_commands : int
    number_of_successes : int
    number_of_failures : int
    geo_variability : int
    country_code : str | None

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

    def __init__(self, date_of_log : datetime, zenodosession: ZenodoSession):
        self.date = date_of_log
        self._set_data_from_events(zenodosession.events)



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