from dataclasses import dataclass
from typing import Optional
from datetime import datetime
"""
                    Field                             Description
    ===============================   ===========================================================
    session_id                        Unique ID of the session
    dst_ip_identifier                 Pseudonymized dst public IPv4 of the honeypot node
    dst_host_identifier               Obfuscated (pseudonymized) name of the honeypot node
    src_ip_identifier                 Obfuscated (pseudonymized) IP address of the attacker
    eventid                           Event id of the session in the cowrie honeypot
    timestamp                         UTC time of the event
    message                           Message of the Cowrie honeypot
    protocol                          Protocol used in the cowrie honeypot; either ssh or telnet
    geolocation_data/postal_code      Source IP postal code as (determined by logstash)
    geolocation_data/continent_code   Source IP continent code (as determined by logstash)
    geolocation_data/country_code3    Source IP country code3 (as determined by logstash)
    geolocation_data/region_name      Source IP region name (as determined by logstash)
    geolocation_data/latitude         Source IP latitude (as determined by logstash)
    geolocation_data/longitude        Source IP longitude (as determined by logstash)
    geolocation_data/country_name     Source IP full country name (as determined by logstash)
    geolocation_data/timezone         Source IP timezone
    geolocation_data/country_code2    Source IP country code2
    geolocation_data/region_code      Source IP region code
    geolocation_data/city_name        Source IP city name
    src_port                          Source TCP port
    sensor                            Sensor name; serves to identify our experiment config
    arch                              Represents the CPU/OS architecture emulated by cowrie
    duration                          Session duration in seconds
    ssh_client_version                Attacker's SSH client version
    username                          Login username; only used for login events
    password                          Password; only used for login events
    macCS                             HMAC algorithms supported by the client
    encCS                             Encryption algorithms supported by the client
    kexAlgs                           Key exchange algorithms supported by the client
    keyAlgs                           Public key algorithms supported by the client

"""
"""
    zenodo_keys = {"session_id", "dst_ip_identifier", "dst_host_identifier", "src_ip_identifier", "eventid",
                    "timestamp", "message", "protocol", "geolocation_data", "src_port", "sensor", "arch",
                    "duration", "ssh_client_version", "username", "password", "macCS", "encCS", "keyAlgs","keyAlgs"}
    zenodo_geolocation_keys = {"postal_code", "continent_code", "country_code3", "region_name", "latitude",
                                "longitude", "country_name", "timezone", "country_code2", "region_code","city_name"}

"""

@dataclass
class GeolocationData:
    postal_code: Optional[str] = None
    continent_code: Optional[str] = None
    country_code3: Optional[str] = None
    region_name: Optional[str] = None
    latitude: Optional[str] = None
    longitude: Optional[str] = None
    country_name: Optional[str] = None
    timezone: Optional[str] = None
    country_code2: Optional[str] = None
    region_code: Optional[str] = None
    city_name: Optional[str] = None

    def print_geolocation(self):
        print("Geolocation:")
        if self.country_name:
            print(f"  Country  : {self.country_name}")
        if self.continent_code:
            print(f"  Continent: {self.continent_code}")
        if self.city_name:
            print(f"  City     : {self.city_name}")
        if self.latitude and self.longitude:
            print(f"  Lat/Lon  : {self.latitude}, {self.longitude}")


@dataclass
class ZenodoEvent:
    # === REQUIRED ===
    session_id: str
    eventid: str

    # === OPTIONAL ===
    timestamp: Optional[str] = None
    src_ip_identifier: Optional[str] = None
    dst_ip_identifier: Optional[str] = None
    dst_host_identifier: Optional[str] = None
    message: Optional[str] = None
    protocol: Optional[str] = None
    src_port: Optional[int] = None
    sensor: Optional[str] = None
    arch: Optional[str] = None
    duration: Optional[float] = None
    ssh_client_version: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    macCS: Optional[str] = None
    encCS: Optional[str] = None
    kexAlgs: Optional[str] = None
    keyAlgs: Optional[str] = None
    geolocation_data: Optional[GeolocationData] = None


    def print_event(self):
        print("-" * 50)
        print(f"Event ID   : {self.eventid}")
        print(f"Session ID : {self.session_id}")

        if self.timestamp:
            print(f"Timestamp  : {self.timestamp}")
        if self.protocol:
            print(f"Protocol   : {self.protocol}")
        if self.src_port is not None:
            print(f"Src Port   : {self.src_port}")
        if self.message:
            print(f"Message    : {self.message}")

        if self.geolocation_data:
            self.geolocation_data.print_geolocation()


class ZenodoSession:
    def __init__(self, session_id: str, events: list[ZenodoEvent]):
        self.session_id = session_id
        self.events = events

    def print_session(self):
        print (f"Session ID: {self.session_id}")
        for event in self.events:
            event.print_event()


class ZenodoLog:
    def __init__(self, date_of_log):
        self.date_of_log = date_of_log # funziona un po' da chiave primaria. Viene recuperato dal nome del file
        self.sessions : list[ZenodoSession] = []

    @classmethod
    def from_json(cls, date_of_log: str, data: list[dict]) -> "ZenodoLog":
        log = cls(date_of_log)

        for session_dict in data:
            # session_dict ha solo una chiave, ovvero il session_id
            for session_id, event_list in session_dict.items():
                events: list[ZenodoEvent] = []

                for e in event_list:
                    geo = e.get("geolocation_data")
                    geo_data = None

                    if geo:
                        geo_data = GeolocationData(
                            postal_code=geo.get("postal_code"),
                            continent_code=geo.get("continent_code"),
                            country_code3=geo.get("country_code3"),
                            region_name=geo.get("region_name"),
                            latitude=geo.get("latitude"),
                            longitude=geo.get("longitude"),
                            country_name=geo.get("country_name"),
                            timezone=geo.get("timezone"),
                            country_code2=geo.get("country_code2"),
                            region_code=geo.get("region_code"),
                            city_name=geo.get("city_name")
                        )

                        event = ZenodoEvent(
                            session_id=e.get("session_id", session_id),
                            eventid=e.get("eventid"),
                            timestamp=e.get("timestamp"),
                            src_ip_identifier=e.get("src_ip_identifier"),
                            dst_ip_identifier=e.get("dst_ip_identifier"),
                            dst_host_identifier=e.get("dst_host_identifier"),
                            message=e.get("message"),
                            protocol=e.get("protocol"),
                            src_port=e.get("src_port"),
                            sensor=e.get("sensor"),
                            geolocation_data=geo_data,
                        )
                        events.append(event)
                    log.sessions.append(ZenodoSession(session_id=session_id, events=events))
        return log

    @staticmethod
    def parse_date_from_filename(filename: str) -> str: # es: cyberlab_2019-05-13.json -> 2019-05-13
        filename = filename.removesuffix(".json")
        return filename.removeprefix("cyberlab_")

    def print_log(self):
        print(f"Date of Log: {self.date_of_log}")
        for s in self.sessions:
            s.print_session()