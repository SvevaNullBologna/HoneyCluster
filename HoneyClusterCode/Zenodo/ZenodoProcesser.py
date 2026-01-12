from pathlib import Path

import logging
import os
import ijson
import pandas as pd
import Zenodo.ZenodoDataReader as ZDR
from Zenodo.ZenodoDataReader import Cleaned_Attr


"""
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
                PROCESSING DATASET INTO USEFUL DATAS
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
"""
"""
json iniziale:

        {
            "sessions":
            {
                "session_start" : str,
                "session_end" : str,
                "raw_event_count": int,
                "events": * solo eventi rilevanti
                [
                            {
                                "status" : int,
                                "timestamp" : str,
                                * valori che dipendono dallo status
                            }
                ]
                ...
            }
            ...
        }
        """


def process_to_parquet(json_path: Path, output_parquet: Path): # * vedi spiegazione sul formato parquet in documenti


    if not os.path.exists(json_path):
        return

    all_sessions_in_file = []

    with open(json_path, 'rb') as f:
        for session_id, session_data in ijson.kvitems(f, 'sessions'):
            session_start_time = session_data['session_start']
            session_end_time = session_data['session_end']
            session_number_of_events = session_data['raw_event_count']
            session_events = session_data['events']
            if not session_events: continue

            timestamps, commands, verbs, statuses = [],[],[],[]

            for event in session_events:
                status = event[Cleaned_Attr.STATUS]
                timestamp = event[Cleaned_Attr.TIME]

                statuses.append(status)
                timestamps.append(timestamp)

                if ZDR.is_command(status):
                    cmd = event.get(Cleaned_Attr.MSG.value)
            return
"""
# estrazione immediata dei dati necessari dalla sessione corrente
            events = session_data.get('events',[])
            if not events:
                continue

            timestamps = commands = verbs = statuses = []
            ssh_version = None

            for e in events:
                status = e.get('status')

                statuses.append(status)
                timestamps.append(time)
                # estraiamo i cambi base
                if zdr.is_command(status):
                    cmd = e.get('msg')
                    if cmd :
                        commands.append(cmd)
                        verbs.append(HCD.get_verb_of_command(cmd))

                if zdr.is_version(status):
                    ssh_version = e.get('version')


            # qui posso già calcolare i valori voluti in HoneyClusterData:
            data_obj = HCD.Honey(
                inter_command_timing=HCD.get_inter_command_timing(timestamps),
                session_duration=HCD.session_duration(start_time, end_time),
                time_of_day_patterns=HCD.get_time_of_day_patterns(timestamps),
                unique_commands_ratio=HCD.get_unique_commands_ratio(commands),
                command_diversity_ratio=HCD.get_command_diversity_ratio(verbs),
                tool_signatures=HCD.get_tool_signatures(verbs),
                reconnaissance_vs_exploitation_ratio=HCD.get_reconnaissance_vs_exploitation_ratio(verbs),
                error_rate=HCD.get_error_rate(statuses),
                command_correction_attempts =HCD.get_command_correction_attempts(statuses, commands)
            )

            row = data_obj.__dict__
            row['session_id'] = session_id
            all_sessions_in_file.append(row)

        if all_sessions_in_file:
            df = pd.DataFrame(all_sessions_in_file)
            df.to_parquet(output_parquet, engine='fastparquet', index=False)
"""

"""
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
                                        EXECUTION
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
"""


def process_cleaned_dataset(base_folder_path: Path):
    if not (base_folder_path and base_folder_path.exists()):
        logging.error(f"base_folder_path {base_folder_path} non existent")
        return
    starting_path = Path(base_folder_path, "cleaned")
    resulting_path = base_folder_path / "processed"
    if not starting_path.exists():
        print(f"Errore: La cartella {starting_path} non esiste.")
        return

    resulting_path.mkdir(parents=True, exist_ok=True)

    for json_file in starting_path.glob("*.json"):
        parquet_output = resulting_path / json_file.with_suffix('.parquet').name
        if os.path.exists(parquet_output):
            logging.info(f"skipping {parquet_output}.")
            continue

        logging.info(f"Processing {json_file} ...")
        try:
            process_to_parquet(json_file, parquet_output)
            logging.info(f"Completed {json_file}")
        except Exception as e:
            logging.warning(f"Errore durante il processamento di {json_file.name}: {e}")

def read_parquet(output_path: Path) -> pd.DataFrame:
    try:
        datas = pd.read_parquet(output_path, engine='fastparquet')
        return datas
    except Exception as e:
        logging.warning(f"Errore di lettura {output_path.name}: {e}")
        return pd.DataFrame()

def concat_parquets(base_folder_path : Path) -> pd.DataFrame:
    parquets_folder_path = base_folder_path / "processed"
    if not os.path.exists(parquets_folder_path):
        logging.error(f"La cartella {parquets_folder_path} non existent")
        return pd.DataFrame()

    all_files = parquets_folder_path.glob('*.parquet')

    if not all_files:
        return pd.DataFrame()

    df = pd.concat((pd.read_parquet(f) for f in all_files), ignore_index=True)

    if 'session_id' in df.columns:
        df.drop(columns=['session_id'], inplace=True)

    df = df.round(2)

    df.drop_duplicates(inplace=True)

    logging.info(f"Number of loaded files: {len(df)}")

    df.to_parquet(parquets_folder_path.parent / "complete_dataset.parquet", index=False)

    return df


def get_main_dataset_from_processed(base_folder_path: Path) -> pd.DataFrame:

        logging.basicConfig(level=logging.DEBUG)
        process_cleaned_dataset(base_folder_path)
        complete = concat_parquets(base_folder_path)
        return complete

def read_main_dataset(base_folder_path: Path) -> pd.DataFrame:
    try:
        m_ds = read_parquet(Path(base_folder_path, "complete_dataset.parquet"))
        return m_ds
    except FileNotFoundError:
        logging.error("please, get main dataset before trying to read it")
        return pd.DataFrame()



if __name__ == "__main__":
    base_folder = Path("C:\\Users\\Sveva\\Documents\\GitHub\\zenodo_dataset")
    json_path = Path("C:\\Users\\Sveva\\Documents\\GitHub\\zenodo_dataset\\cleaned\\2019-05-14.json")
    process_to_parquet(json_path, base_folder / "processed")
    #dataset = get_main_dataset_from_processed(base_folder)
    print(read_main_dataset(base_folder))