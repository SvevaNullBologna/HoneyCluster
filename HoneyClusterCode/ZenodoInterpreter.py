import os
import logging
import gzip
import file_worker

class ZenodoInterpreter:
    def __init__(self, zenodo_local_path: str , extracted_zenodo_local_path : str | None ):
        if file_worker.check_directory(zenodo_local_path, False) and file_worker.check_directory(extracted_zenodo_local_path, True):
            self.zenodo_path = zenodo_local_path
            self.extracted_zenodo_path = extracted_zenodo_local_path

            logging.info("Zenodo Interpreter started.")
            if file_worker.contains_file_type(zenodo_local_path, ".gz") > 0 :
                file_worker.extract_gz_files(self.zenodo_path, self.extracted_zenodo_path)
        else:
            logging.info("Zenodo Interpreter failed because of error in path.")



def main() -> None:
    logging.basicConfig(level=logging.DEBUG)
    z_interpreter = ZenodoInterpreter("")


if __name__ == "__main__":
    main()