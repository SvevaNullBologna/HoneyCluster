import logging
from Zenodo.ZenodoInterpreter import get_zenodo_log_list
from MachineLearning.HoneyClusterML import clustering
from MachineLearning.HoneyClusterML import cluster_analysis

from pathlib import Path

def main() -> None:
    logging.basicConfig(level=logging.DEBUG)

    logs = get_zenodo_log_list()

    clustering_result = clustering(logs)

    summary = cluster_analysis(clustering_result)

    print(summary)

    # zenodo_interpreter.extract_and_clean_all_zenodo_logs_in_folder()
    """ zenodo_log = ZenodoLog.read_file(Path(zenodo_interpreter.cleaned, "2019-12-08.json"))
    for session in zenodo_log.sessions:
        for event in session.events:
            with open(Path("C:\\Users\\Sveva\\Desktop\\file.txt"),'a', encoding="utf-8") as f:
                if event.is_command():
                    f.write(event.message + "\n\n\n")
    #ZenodoLog.write_on_file(Path("C:\\Users\\Sveva\\Desktop\\file.txt"), zenodo_log) """
if __name__ == "__main__":
    main()


