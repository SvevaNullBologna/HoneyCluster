import logging
from pathlib import Path

import pandas as pd
from sklearn.preprocessing import StandardScaler
from hdbscan import HDBSCAN

from HoneyClusterData import read_main_dataset

from joblib import dump,load

_ARTIFACTS = "C:\\Users\\Sveva\\Documents\\GitHub\\zenodo_dataset\\artifacts"
_SCALER_PATH = Path(_ARTIFACTS,"scaler_honeypot.joblib")
_MODEL_PATH = Path(_ARTIFACTS,"hdbscan_model.joblib")
_CORE_DATASET_PATH = Path(_ARTIFACTS,"core_dataset.parquet")

def _extraction_of_initial_clustering_subset(base_folder_path: Path, n_samples: int = 200000) -> pd.DataFrame:
    logging.info("Caricamento dataset principale")
    df = read_main_dataset(base_folder_path)

    if df.empty:
        logging.warning("dataset vuoto")
        raise Exception("dataset vuoto")

    # extract core dataset -> decide criteria
    if len(df) < n_samples:
        return df

    df_core = df.sample(n_samples, random_state=42)

    if df_core.empty:
        raise Exception("core dataset vuoto")

    return df_core

def _get_scaler():
    try:
        scaler = load(_SCALER_PATH)
        return scaler
    except FileNotFoundError:
        return None

def _get_model():
    try:
        model = load(_MODEL_PATH)
        return model
    except FileNotFoundError:
        return None

def _save_scaler(scaler: StandardScaler):
    dump(scaler, _SCALER_PATH)

def _save_model(model: HDBSCAN):
    dump(model, _MODEL_PATH)

def _build_scaled_core_model(core_dataset: pd.DataFrame, old_scaler : StandardScaler = None): # scaliamo i dati per evitare che dei valori troppo grandi sovrastino gli altri

    if not old_scaler: # lo scaler impara, quindi riutilizzarlo è MEGLIO.
        scaler = StandardScaler()
        scaled = scaler.fit_transform(core_dataset)
    else:
        scaler = old_scaler
        scaled = scaler.transform(core_dataset)

    _save_scaler(scaler) # salviamo lo scaler in un file

    return scaled # restituiamo i dati scalati

def _clustering(scaled_core, model: HDBSCAN = None):
    if not model:
        model = HDBSCAN(
            min_cluster_size = 50,
            prediction_data = True, # crea una struttura dati extra durante il training
            core_dist_n_jobs = -1 # permette l'utilizzo di più core possibili
        )

    model.fit(scaled_core) # restituisce l'oggetto modello stesso, non le labels come in sklearn

    labels = model.labels_ # le labels si trovano nell'attributo labels

    _save_model(model)

    return labels

def complete_clustering_pipeline(base_folder_path: Path):
    try:
        core_dataset = _extraction_of_initial_clustering_subset(base_folder_path)

        # recupero lo stato precedente se esiste
        scaler = _get_scaler()
        model = _get_model()

        # preparo i dati (e salvo lo scaler aggiornato)
        scaled_dataset = _build_scaled_core_model(core_dataset, scaler) # aggiorna automaticamente lo scaler

        # il dataset core che stiamo usando (è un subset, quindi è importante metterlo da parte?)

        labels = _clustering(scaled_dataset, model) #aggiorna automaticamente il model
        #otteniamo una lista del tipo [1,1, 0, 2, -1]. Attaccanti con numeri uguali corrispondono allo stesso gruppo
        #gli attacchi nuovi avranno -1
        #invece che analizzare milioni di file, possiamo analizzare i 'rappresentanti' dei gruppi

        core_dataset['cluster_id'] = labels # aggiungiamo una colonna chiamata id del cluster e ci mettiamo le label

        if core_dataset.empty:
            raise Exception("resulting dataset vuoto")

        return core_dataset
    except Exception as e:
        print(e)
        return None

def analysis_of_cluster(resulting_dataset: pd.DataFrame):
    return resulting_dataset.groupby('cluster_id').count()



if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    base_folder = Path("C:\\Users\\Sveva\\Documents\\GitHub\\zenodo_dataset")
    result = complete_clustering_pipeline(base_folder)
    #logging.info(f"distribuzione cluster:{result.count()}")
    result = analysis_of_cluster(result)
    result.to_parquet(base_folder/"clustering_core_results.parquet")