from dataclasses import dataclass
from datetime import datetime
from Zenodo.ZenodoDataStructure import ZenodoSession

@dataclass
class HoneyClusterSession:
    date : datetime
    date_time: datetime
    duration : float | None
    normalized_command_list : list[str] | None
    number_of_events : int
    number_of_commands : int
    number_of_successes : int
    number_of_failures : int
    geo_variability : int
    country_code : str | None

    def __init__(self, date_of_log : datetime, zenodosession: ZenodoSession):
        self.date = date_of_log

        self.duration = zenodosession.get_duration()
        v = zenodosession.get_number_of_events_commands_successes_and_failures()
        self.normalized_command_list = v.get("commands")
        self.number_of_events = v.get("total_events")
        self.number_of_commands = v.get("command_count")
        self.number_of_successes = v.get("command_success")
        self.number_of_failures = v.get("command_failure")

        geo_data = zenodosession.get_geo_data()
        self.geo_variability = geo_data.get("variability")
        self.country_code = geo_data.get("countries")