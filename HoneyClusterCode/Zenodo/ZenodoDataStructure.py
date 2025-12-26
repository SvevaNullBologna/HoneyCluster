from dataclasses import dataclass
from typing import Optional,List

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
"""
Legenda:
[ ... ] = lista
{ ... } = dizionario

ogni LogFile contiene qualcosa del tipo:
     [ 
        {
            "identificatore che è uguale al valore del session id": [
                {
                    "session_id":"f83815b1d845",
                    "dst_host_identifier":"31790014bd4d8ec9a6273dcd7335f4569c83985f98bf1ba97e029833d58e6c81",
                    "eventid":"cowrie.session.connect",
                    "timestamp":"2019-06-26T00:07:20.921797Z",
                    "src_ip_identifier":"a2bd07cfe8104e4bd7b82ad7eb1868029d93a151a96bbcae2d4b746bde838bde",
                    "dst_ip_identifier":null,
                    "message":"New connection: a2bd07cfe8104e4bd7b82ad7eb1868029d93a151a96bbcae2d4b746bde838bde:37412 (10.244.2.40:2222) [session: f83815b1d845]",
                    "protocol":"ssh",
                    "src_port":37412,
                    "sensor":"cowrie-deployment-2nwj2",
                    "geolocation_data": { * guarda sotto * },
                    "arch":null,
                    "duration":null,
                    "ssh_client_version":null,
                    "username":null,
                    "password":null,
                    "hasshAlgorithms":null,
                    "macCS":null,
                    "langCS":null,
                    "compCS":null,
                    "encCS":null,
                    "hassh":null,
                    "kexAlgs":null,
                    "keyAlgs":null,
                    "fingerprint":null,
                    "key":null,
                    "type":null,
                    "outfile":null,
                    "destfile":null,
                    "duplicate":null,
                    "shasum":null,
                    "url":null,
                    "ttylog":null,
                    "size":null,
                    "filename":null,
                    "data":null
                },
                {
                    ...
                }
            ]
        },
        { 
            ... 
        },
        { 
            ...
        }
    ]
     
    * * 
  
  "geolocation_data": {
    "country_name": "Netherlands",
    "continent_code": "EU",
    "timezone": "Europe/Amsterdam",
    "region_code": "ZH",
    "country_code2": "NL",
    "location": {
      "lon": 4.663,
      "lat": 51.8656
    },
    "region_name": "South Holland",
    "latitude": 51.8656,
    "country_code3": "NL",
    "ip": "00a59ce4892d5315f8eaec9baaf8c31ba1496edf2b2b954d13b8f896301df9fe",
    "postal_code": "2951",
    "city_name": "Alblasserdam",
    "longitude": 4.663
  }
  
  
  
  Cosa vogliamo ottenere noi?
  
  "valore data del dataLog" : {
        "valore session_id" : 
        [ "event_id": .... ,
           tutti i valori non nulli
        ]
  }
 
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

    def is_null(self):
        return not any([self.postal_code,self.continent_code,self.country_code3,self.region_name,self.latitude,self.longitude,self.country_name,self.timezone,self.country_code2,self.region_code,self.city_name])

    def __str__(self):
        parts = ["GEOLOCATION:"]
        if self.postal_code:
            parts.append(f'postal code: {self.postal_code}')
        if self.continent_code:
            parts.append(f'continent code: {self.continent_code}')
        if self.country_code3:
            parts.append(f'country code3: {self.country_code3}')
        if self.region_name:
            parts.append(f'region name: {self.region_name}')
        if self.latitude:
            parts.append(f'latitude: {self.latitude}')
        if self.longitude:
            parts.append(f'longitude: {self.longitude}')
        if self.country_name:
            parts.append(f'country name: {self.country_name}')
        if self.timezone:
            parts.append(f'timezone: {self.timezone}')
        if self.country_code2:
            parts.append(f'country code2: {self.country_code2}')
        if self.region_code:
            parts.append(f'region code: {self.region_code}')
        if self.city_name:
            parts.append(f'city name: {self.city_name}')
        return "\n".join(parts)


@dataclass
class ZenodoEvent:
    # === REQUIRED ===
    eventid: str

    # === OPTIONAL ===
    dst_host_identifier: Optional[str] = None
    timestamp: Optional[str] = None
    src_ip_identifier: Optional[str] = None
    dst_ip_identifier: Optional[str] = None
    message: Optional[str] = None
    protocol: Optional[str] = None
    scr_port: Optional[str] = None
    sensor: Optional[str] = None
    geolocation_data: Optional[GeolocationData] = None
    arch: Optional[str] = None
    duration: Optional[str] = None
    ssh_client_version: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    hasshAlgorithms: Optional[str] = None
    macCS: Optional[str] = None
    langCS: Optional[str] = None
    compCS: Optional[str] = None
    encCS: Optional[str] = None
    hassh: Optional[str] = None
    kexAlgs: Optional[str] = None
    keyAlgs: Optional[str] = None
    fingerprint: Optional[str] = None
    key: Optional[str] = None
    type: Optional[str] = None
    outfile: Optional[str] = None
    destfile: Optional[str] = None
    duplicate: Optional[str] = None
    shasum: Optional[str] = None
    url: Optional[str] = None
    ttylog: Optional[str] = None
    size: Optional[str] = None
    filename: Optional[str] = None
    data: Optional[str] = None

    def __str__(self):
        parts = ["ZENODO EVENT:", f'eventid: {self.eventid}']
        if self.dst_host_identifier :
            parts.append(f'dst_host_identifier: {self.dst_host_identifier}')
        if self.timestamp:
            parts.append(f'timestamp: {self.timestamp}')
        if self.src_ip_identifier:
            parts.append(f'src_ip_identifier: {self.src_ip_identifier}')
        if self.dst_ip_identifier:
            parts.append(f'dst_ip_identifier: {self.dst_ip_identifier}')
        if self.message:
            parts.append(f'message: {self.message}')
        if self.protocol:
            parts.append(f'protocol: {self.protocol}')
        if self.scr_port:
            parts.append(f'scr_port: {self.scr_port}')
        if self.sensor:
            parts.append(f'sensor: {self.sensor}')
        if self.geolocation_data and not self.geolocation_data.is_null():
            parts.append(str(self.geolocation_data))
        if self.arch:
            parts.append(f'arch: {self.arch}')
        if self.duration:
            parts.append(f'duration: {self.duration}')
        if self.ssh_client_version:
            parts.append(f'ssh_client_version: {self.ssh_client_version}')
        if self.username:
            parts.append(f'username: {self.username}')
        if self.password:
            parts.append(f'password: {self.password}')
        if self.hasshAlgorithms:
            parts.append(f'hasshAlgorithms: {self.hassh}')
        if self.macCS:
            parts.append(f'macCS: {self.macCS}')
        if self.langCS:
            parts.append(f'langCS: {self.langCS}')
        if self.compCS:
            parts.append(f'compCS: {self.compCS}')
        if self.hassh:
            parts.append(f'hassh: {self.hassh}')
        if self.kexAlgs:
            parts.append(f'kexAlgs: {self.kexAlgs}')
        if self.keyAlgs:
            parts.append(f'keyAlgs: {self.keyAlgs}')
        if self.fingerprint:
            parts.append(f'fingerprint: {self.fingerprint}')
        if self.key:
            parts.append(f'key: {self.key}')
        if self.type:
            parts.append(f'type: {self.type}')
        if self.outfile:
            parts.append(f'outfile: {self.outfile}')
        if self.destfile:
            parts.append(f'destfile: {self.destfile}')
        if self.duplicate:
            parts.append(f'duplicate: {self.duplicate}')
        if self.shasum:
            parts.append(f'shasum: {self.shasum}')
        if self.url:
            parts.append(f'url: {self.url}')
        if self.ttylog:
            parts.append(f'ttylog: {self.ttylog}')
        if self.size:
            parts.append(f'size: {self.size}')
        if self.filename:
            parts.append(f'filename: {self.filename}')
        if self.data:
            parts.append(f'data: {self.data}')
        return "\n".join(parts)

class ZenodoSession:
    session_id: str
    events: List[ZenodoEvent]
    def __init__(self, session_id: str, events: list[ZenodoEvent]):
        if not session_id :
            raise ValueError('session_id must not be None')
        if not events :
            raise ValueError('events must not be None')
        self.session_id = session_id
        self.events = events

    def __str__(self):
        parts = [f"Session ID: {self.session_id}"]
        for event in self.events:
            parts.append(str(event))
        return "\n".join(parts)

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
                    eventid = e.get("eventid")
                    if not eventid:
                        continue #eventid è required

                    geo_data = None
                    geo = e.get("geolocation_data")
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
                        eventid=eventid,
                        dst_host_identifier=e.get("dst_host_identifier"),
                        timestamp=e.get("timestamp"),
                        src_ip_identifier=e.get("src_ip_identifier"),
                        dst_ip_identifier=e.get("dst_ip_identifier"),
                        message=e.get("message"),
                        protocol=e.get("protocol"),
                        scr_port=e.get("src_port"),
                        sensor=e.get("sensor"),
                        geolocation_data=geo_data,
                        arch=e.get("arch"),
                        duration=e.get("duration"),
                        ssh_client_version=e.get("ssh_client_version"),
                        username=e.get("username"),
                        password=e.get("password"),
                        macCS=e.get("macCS"),
                        encCS=e.get("encCS"),
                        kexAlgs=e.get("kexAlgs"),
                        keyAlgs=e.get("keyAlgs")
                        )

                    events.append(event)

                    if events:
                        log.sessions.append(ZenodoSession(session_id, events))

        return log


    def __str__(self):
        parts = ["Zenodo Log:", f"data log: {self.date_of_log}"]
        for session in self.sessions:
            parts.append(str(session))
        return "\n".join(parts)

    @staticmethod
    def parse_date_from_filename(filename: str) -> str: # es: cyberlab_2019-05-13.json -> 2019-05-13
        filename = filename.removesuffix(".json")
        return filename.removeprefix("cyberlab_")

    def print_log(self):
        print(f"Date of Log: {self.date_of_log}")
        for s in self.sessions:
            print(s)