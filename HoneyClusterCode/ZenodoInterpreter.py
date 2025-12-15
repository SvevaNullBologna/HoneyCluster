import json
import os
import logging
import gzip
import file_worker

class ZenodoInterpreter:
    def __init__(self, zenodo_local_path: str , extracted_zenodo_local_path : str | None , extracted_files_tab: str | None ):
        logging.info("Zenodo Interpreter started.")
        if file_worker.contains_file_type(zenodo_local_path, ".gz") > 0 :
            self.zenodo_path, self.extracted_zenodo_path, self.extracted_files_tab = file_worker.extract_gz_files(zenodo_local_path, extracted_zenodo_local_path, extracted_files_tab)

            self.zenodo_keys = {"session_id", "dst_ip_identifier","dst_host_identifier","src_ip_identifier","eventid","timestamp","message","protocol","geolocation_data","src_port","sensor", "arch", "duration", "ssh_client_version", "username", "password", "macCS", "encCS", "keyAlgs", "keyAlgs"}
            self.zenodo_geolocation_keys = {"postal_code", "continent_code", "country_code3", "region_name", "latitude", "longitude", "country_name", "timezone", "country_code2", "region_code", "city_name"}

        else:
            logging.info("Zenodo Interpreter failed because of error in path.")

    def interpret_single_zenodo_json(self, filename: str) -> dict | None:
        try:
            with open(filename, "r", encoding="utf-8") as json_file:
                data = json.load(json_file)
                print(data)
            return data
        except Exception as e:
            logging.error(f"Failed to parse JSON {filename}: {e}")
            return None





def main() -> None:
    logging.basicConfig(level=logging.DEBUG)
    z_interpreter = ZenodoInterpreter("C:\\Users\\Sveva\\Documents\\GitHub\\zenodo_dataset", "C:\\Users\\Sveva\\Desktop\\estratti", None)


if __name__ == "__main__":
    main()