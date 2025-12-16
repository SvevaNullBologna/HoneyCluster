import json
import logging
import file_worker
import os

class ZenodoInterpreter:
    def __init__(self, zenodo_local_path: str , extracted_zenodo_local_path : str | None):
        logging.info("Zenodo Interpreter started.")
        if file_worker.contains_file_type(zenodo_local_path, ".gz") > 0 :
            self.zenodo_path, self.extracted_zenodo_path = file_worker.extract_gz_files(zenodo_local_path, extracted_zenodo_local_path)
            self.zenodo_keys = {"session_id", "dst_ip_identifier","dst_host_identifier","src_ip_identifier","eventid","timestamp","message","protocol","geolocation_data","src_port","sensor", "arch", "duration", "ssh_client_version", "username", "password", "macCS", "encCS", "keyAlgs", "keyAlgs"}
            self.zenodo_geolocation_keys = {"postal_code", "continent_code", "country_code3", "region_name", "latitude", "longitude", "country_name", "timezone", "country_code2", "region_code", "city_name"}

        else:
            logging.info("Zenodo Interpreter failed because of error in path.")

    def interpret_single_zenodo_json(self, filename: str) -> dict | None:
        try:
            with open(filename, "r", encoding="utf-8") as json_file:
                data = json.load(json_file)
                
            return data
        except Exception as e:
            logging.error(f"Failed to parse JSON {filename}: {e}")
            return None





def main() -> None:
    logging.basicConfig(level=logging.DEBUG)
    z_interpreter = ZenodoInterpreter("C:\\Users\\Sveva\\Documents\\GitHub\\zenodo_dataset", "C:\\Users\\Sveva\\Desktop\\estratti")
    z_interpreter.interpret_single_zenodo_json(os.path.join(z_interpreter.extracted_zenodo_path, "cyberlab_2019-05-18.json"))

if __name__ == "__main__":
    main()