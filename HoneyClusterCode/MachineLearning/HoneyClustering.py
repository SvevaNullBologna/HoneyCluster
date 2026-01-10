import logging
from pathlib import Path

import pandas as pd
from sklearn.preprocessing import StandardScaler
from hdbscan import HDBSCAN

from HoneyClusterData import read_main_dataset

from joblib import dump,load

_SCALER_PATH = ""
_MODEL_PATH = ""

def extraction_of_initial_clustering_subset(base_folder_path: Path) -> pd.DataFrame:
    logging.info("Caricamento dataset principale")
    df = read_main_dataset(base_folder_path)


    # extract core dataset -> decide criteria

    return df

def get_scaler():
    try:
        scaler = load(_SCALER_PATH)
        return scaler
    except FileNotFoundError:
        return None

def get_model():
    try:
        model = load(_MODEL_PATH)
        return model
    except FileNotFoundError:
        return None

def save_scaler(scaler: StandardScaler):
    dump(scaler, _SCALER_PATH)

def save_model(model: HDBSCAN):
    dump(model, _MODEL_PATH)

def build_scaled_core_model(base_folder_path: Path, old_scaler : StandardScaler = None): # scaliamo i dati per evitare che dei valori troppo grandi sovrastino gli altri
    core_dataset = extraction_of_initial_clustering_subset(base_folder_path)

    if not old_scaler: # lo scaler impara, quindi riutilizzarlo è MEGLIO.
        scaler = StandardScaler()
        scaled = scaler.fit_transform(core_dataset)
    else:
        scaler = old_scaler
        scaled = scaler.transform(core_dataset)

    save_scaler(scaler) # salviamo lo scaler in un file

    return scaled, scaler, core_dataset # restituiamo i dati scalati, lo scaler e il dataset core che stiamo usando (è un subset, quindi è importante metterlo da parte)

def clustering(scaled_core):
    model = HDBSCAN(
        min_cluster_size = 50,
        prediction_data = True, # crea una struttura dati extra durante il training
        core_dist_n_jobs = -1 # permette l'utilizzo di più core possibili
    )

    labels = model.fit(scaled_core)

    save_model(model)

    return model, labels

def map_full_dataset():
    pass