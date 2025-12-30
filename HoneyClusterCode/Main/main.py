import logging
from Zenodo.ZenodoInterpreter import ZenodoInterpreter
from Zenodo.ZenodoDataStructure import ZenodoLog
from pathlib import Path

def main() -> None:
    logging.basicConfig(level=logging.DEBUG)
    zenodo_interpreter = ZenodoInterpreter(Path("C:\\Users\\Sveva\\Documents\\GitHub\\zenodo_dataset"))
    #zenodo_interpreter.extract_and_clean_all_zenodo_logs_in_folder()
    zenodo_log = ZenodoLog.read_file(Path(zenodo_interpreter.cleaned, "2020-01-02.json"))
    #ZenodoLog.write_on_file(Path("C:\\Users\\Sveva\\Desktop\\file.txt"), zenodo_log)
if __name__ == "__main__":
    main()