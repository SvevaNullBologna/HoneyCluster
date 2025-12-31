import logging
from Zenodo.ZenodoInterpreter import ZenodoInterpreter
from Zenodo.ZenodoDataStructure import ZenodoLog
from pathlib import Path

def main() -> None:
    logging.basicConfig(level=logging.DEBUG)
    zenodo_interpreter = ZenodoInterpreter(Path("C:\\Users\\Sveva\\Documents\\GitHub\\zenodo_dataset"))
    #zenodo_interpreter.extract_and_clean_all_zenodo_logs_in_folder()
    zenodo_log = ZenodoLog.read_file(Path(zenodo_interpreter.cleaned, "2020-01-02.json"))
    for session in zenodo_log.sessions:
        for event in session.events:
            with open(Path("C:\\Users\\Sveva\\Desktop\\file.txt"),'a', encoding="utf-8") as f:
                # intestazione
                if event.is_connect() == -2:
                    f.write(event.eventid + "\n")
    #ZenodoLog.write_on_file(Path("C:\\Users\\Sveva\\Desktop\\file.txt"), zenodo_log)
if __name__ == "__main__":
    main()