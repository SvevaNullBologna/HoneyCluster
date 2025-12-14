import os
import logging
import gzip

class ZenodoInterpreter:
    def __init__(self, zenodo_local_path):
        self.zenodo_path = zenodo_local_path
        if not os.path.isdir(self.zenodo_path):
            logging.error(f"Directory non valida: {self.zenodo_path}")
        else :
            logging.info("Zenodo Interpreter started.")



def main() -> None:
    logging.basicConfig(level=logging.DEBUG)
    z_interpreter = ZenodoInterpreter("")


if __name__ == "__main__":
    main()