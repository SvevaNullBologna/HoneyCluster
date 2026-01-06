import logging
import os.path

from sklearn.preprocessing import StandardScaler
from sklearn.cluster import DBSCAN

import pandas as pd

from Zenodo.ZenodoDataStructure import ZenodoLog
from Zenodo.ZenodoDataStructure import get_date
from MachineLearning.HoneyClusterData import HoneyClusterSession


import joblib # per salvare lo scaler per poterlo riutilizzare

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

def _cluster_data(matrix: pd.DataFrame, old_dbscan: DBSCAN, eps: float = 0.7, min_samples: int = 5) :

    if old_dbscan is None:
        dbscan = DBSCAN(eps=eps, min_samples=min_samples)

    else:
        dbscan = old_dbscan

    labels = dbscan.fit_predict(matrix)

    clustered = matrix.copy()
    clustered["cluster"] = labels

    return clustered, dbscan

def save_data(scaler: StandardScaler, dbscan: DBSCAN, matrix: pd.DataFrame, scaled_matrix: pd.DataFrame, clustered: pd.DataFrame) -> None:
    #salvataggio oggetti intelligenti (modelli)
    joblib.dump(scaler, "scaler.joblib") # con joblib salviamo l'oggetto intero in un file binario
    joblib.dump(dbscan, "dbscan.joblib")

    #salvataggio dati in file (tabelle)
    matrix.to_parquet("original_data.parquet") # formato tabellare più efficiente del CSV. Compresso, veloce e mantiene i tipi di dati.
    scaled_matrix.to_parquet("scaled_data.parquet")
    clustered.to_parquet("clustered_results.parquet")

    logging.info("tabelle, scaler e dbscan salvati")

def _load_trained_model():
    if os.path.exists("scaler.joblib") and os.path.exists("dbscan.joblib"):
        scaler = joblib.load("scaler.joblib")
        dbscan = joblib.load("dbscan.joblib")
        return scaler, dbscan
    else:
        return None, None

def load_previous_result():
    or_data = pd.read_parquet("original_data.parquet")
    sc_data = pd.read_parquet("scaled_data.parquet")
    cl_data = pd.read_parquet("clustered_results.parquet")
    return or_data, sc_data, cl_data

def clustering(logs: list[ZenodoLog]) :
    matrix = _build_matrix_from_logs(logs)
    old_scaler, old_dbscan = _load_trained_model()
    scaled_matrix, scaler = _normalize_data(matrix, old_scaler)
    clustered, dbscan_model = _cluster_data(matrix, old_dbscan)
    save_data(scaler, dbscan_model, matrix, scaled_matrix, clustered)
    return clustered


def _cluster_analysis(clustered: pd.DataFrame):
    pass