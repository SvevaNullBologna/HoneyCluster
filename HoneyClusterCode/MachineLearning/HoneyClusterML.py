import logging
import os.path

from sklearn.preprocessing import StandardScaler
from sklearn.cluster import DBSCAN

import pandas as pd

# from MachineLearning.HoneyClusterData import HoneyClusterSession


import joblib # per salvare lo scaler per poterlo riutilizzare
"""


ARTIFACTS_DIR = "artifacts"

def _ensure_artifacts_dir():
    os.makedirs(ARTIFACTS_DIR, exist_ok=True)

def _build_full_matrix(new_logs: list[ZenodoLog], old_matrix: pd.DataFrame = None)-> pd.DataFrame:
    new_matrix = _build_matrix_from_logs(new_logs)
    if old_matrix is None or old_matrix.empty:
        return new_matrix
    else:
        return pd.concat([old_matrix, new_matrix], ignore_index=True)

def _build_matrix_from_logs(logs: list[ZenodoLog]) -> pd.DataFrame:
    rows = [
        HoneyClusterSession(
            get_date(log.date_of_log),
            session
        ).to_feature_dict()
        for log in logs
        for session in log.sessions
    ]

    return pd.DataFrame(rows)

def _normalize_data(matrix: pd.DataFrame, previous_scaler: StandardScaler = None):
    if previous_scaler is None:
        scaler = StandardScaler() # restituiamo lo scaler perché contiene in sè proprio l'evoluzione dell'AI
        scaled_array = scaler.fit_transform(matrix)
    else:
        scaler = previous_scaler
        scaled_array = scaler.transform(matrix)

    return (
        pd.DataFrame(scaled_array, columns=matrix.columns, index=matrix.index),
        scaler
    )

def _cluster_data(matrix: pd.DataFrame, eps: float = 0.7, min_samples: int = 5) :

    dbscan = DBSCAN(eps=eps, min_samples=min_samples)

    labels = dbscan.fit_predict(matrix)

    clustered = matrix.copy()
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


def clustering(logs: list[ZenodoLog]) :
    # cerchiamo di recuperare i vecchi dati salvati

    logging.info("loading ML data")

    _ensure_artifacts_dir() # controlla se esiste la cartella, in caso la crea
    old_scaler = _load_trained_scaler()
    old_matrix, _ , _  = load_previous_result()

    # aggiungiamo alla matrice (se necessario) i dati vecchi

    logging.info("building clustering matrix")

    matrix = _build_full_matrix(logs, old_matrix)

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
            n_sessions = ("cluster", "count"),
            avg_duration = ("duration", "mean"),
            avg_commands = ("n_commands", "mean"),
            avg_success_ration = ("success_ratio", "mean"),
            avg_command_rate = ("command_rate", "mean"),
            avg_command_diversity = ("command_diversity", "mean"),
            avg_geo_variability = ("geo_variability", "mean")
        )).sort_values("n_sessions", ascending=False)

    logging.info("Numero cluster individuati: %d", summary.shape[0])
    logging.info("Sessioni rumorose (outlier): %d", len(noise))

    return summary
"""