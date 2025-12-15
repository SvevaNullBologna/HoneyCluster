import os
import logging
import gzip
import file_worker

class ZenodoInterpreter:
    def __init__(self, zenodo_local_path: str , extracted_zenodo_local_path : str | None , extracted_files_tab: str | None ):
        logging.info("Zenodo Interpreter started.")
        if file_worker.contains_file_type(zenodo_local_path, ".gz") > 0 :
            self.zenodo_path, self.extracted_zenodo_path, self.extracted_files_tab = file_worker.extract_gz_files(zenodo_local_path, extracted_zenodo_local_path, extracted_files_tab)
        else:
            logging.info("Zenodo Interpreter failed because of error in path.")



def main() -> None:
    logging.basicConfig(level=logging.DEBUG)
    z_interpreter = ZenodoInterpreter("C:\\Users\\Sveva\\Documents\\GitHub\\zenodo_dataset", "C:\\Users\\Sveva\\Desktop\\estratti", None)


if __name__ == "__main__":
    main()