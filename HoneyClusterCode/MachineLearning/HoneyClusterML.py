import logging
import os.path
from pathlib import Path

from sklearn.preprocessing import StandardScaler
from sklearn.cluster import DBSCAN


import pandas as pd


import joblib # per salvare lo scaler per poterlo riutilizzare

from MachineLearning.HoneyClusterData import read_parquet

ARTIFACTS_DIR = "artifacts"

def _ensure_artifacts_dir():
    os.makedirs(ARTIFACTS_DIR, exist_ok=True)

def _normalize_data(dataset: pd.DataFrame, previous_scaler: StandardScaler = None):
    """
        normalizziamo i dati per evitare che uno sovrasti l'altro per le dimensioni troppo diverse
    """
    session_ids = dataset["session_id"] # li togliamo e mettiamo da parte, perché non vanno normalizzati
    features_only = dataset.drop(columns=["session_id"])

    if previous_scaler is None: # attenzione! Riutilizziamo lo scaler precedente se possibile perché è COLUI CHE IMPARA
        scaler = StandardScaler() # restituiamo lo scaler perché contiene in sè proprio l'evoluzione dell'AI
        scaled_array = scaler.fit_transform(features_only)
    else:
        scaler = previous_scaler
        scaled_array = scaler.transform(features_only)

    return (
        pd.DataFrame(scaled_array, columns=features_only.columns, index=session_ids),
        scaler
    )

def _cluster_data(scaled_dataset: pd.DataFrame, eps: float = 0.7, min_samples: int = 5) :
    """
        eseguiamo il clustering di tipo DBSCAN * nella sezione documenti viene spiegato
    """
    dbscan = DBSCAN(eps=eps, min_samples=min_samples)
    labels = dbscan.fit_predict(scaled_dataset) # se restituisce -1 c'è un problema

    clustered = scaled_dataset.copy()
    clustered["cluster"] = labels

    return clustered, dbscan

def save_data(scaler: StandardScaler, matrix: pd.DataFrame, scaled_matrix: pd.DataFrame, clustered: pd.DataFrame) -> None:
    _ensure_artifacts_dir()

    #salvataggio oggetti intelligenti (modelli)
    joblib.dump(scaler, f"{ARTIFACTS_DIR}/scaler.joblib") # con joblib salviamo l'oggetto intero in un file binario

    #salvataggio dati in file (tabelle)
    matrix.to_parquet(f"{ARTIFACTS_DIR}/original_data.parquet") # formato tabellare più efficiente del CSV. Compresso, veloce e mantiene i tipi di dati.
    scaled_matrix.to_parquet(f"{ARTIFACTS_DIR}/scaled_data.parquet")
    clustered.to_parquet(f"{ARTIFACTS_DIR}/clustered_results.parquet")

    logging.info("tabelle, scaler e dbscan salvati")

def _load_trained_scaler() -> StandardScaler | None:
    try:
        scaler = joblib.load(f"{ARTIFACTS_DIR}/scaler.joblib")
        return scaler
    except FileNotFoundError:
        return None

def load_previous_result():
    try:
        or_data = pd.read_parquet(f"{ARTIFACTS_DIR}/original_data.parquet")
    except FileNotFoundError:
         or_data = None
    try :
        sc_data = pd.read_parquet(f"{ARTIFACTS_DIR}/scaled_data.parquet")
    except FileNotFoundError:
        sc_data = None
    try:
        cl_data = pd.read_parquet(f"{ARTIFACTS_DIR}/clustered_results.parquet")
    except FileNotFoundError:
        cl_data = None

    return or_data, sc_data, cl_data


def clustering(complete_dataset_parquet_path : Path) :
    # cerchiamo di recuperare i vecchi dati salvati

    logging.info("loading ML data")

    _ensure_artifacts_dir() # controlla se esiste la cartella, in caso la crea
    old_scaler = _load_trained_scaler()
    old_matrix, _ , _  = load_previous_result()

    # aggiungiamo alla matrice (se necessario) i dati vecchi

    logging.info("building clustering matrix")

    matrix = read_parquet()

    # normalizzazione dei dati

    logging.info("normalizing data in clustering matrix")

    scaled_matrix, scaler = _normalize_data(matrix, old_scaler)

    # clustering

    logging.info("clustering. Please be patient")

    clustered, _ = _cluster_data(scaled_matrix)

    # salvataggio delle modifiche

    logging.info("saving clustered data")

    save_data(scaler, matrix, scaled_matrix, clustered)

    return clustered


def cluster_analysis(clustered: pd.DataFrame):

    logging.info("running clustering analysis")

    valid_clusters = clustered[clustered["cluster"] != -1 ] # prende gli elementi con label validi
    noise = clustered[clustered["cluster"] == -1] # isola anche il rumore

    summary = (
        valid_clusters
        .groupby("cluster")
        .agg(

        )).sort_values("n_sessions", ascending=False)

    logging.info("Numero cluster individuati: %d", summary.shape[0])
    logging.info("Sessioni rumorose (outlier): %d", len(noise))

    return summary
