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

def _normalize_data(matrix: pd.DataFrame):
    scaler = StandardScaler() # restituiamo lo scaler perché contiene in sè proprio l'evoluzione dell'AI
    scaled_array = scaler.fit_transform(matrix)

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


def clustering(logs: list[ZenodoLog]) :
    matrix = _build_matrix_from_logs(logs)
    matrix = _normalize_data(matrix)
    clustered = _cluster_data(matrix)
    return clustered


def _cluster_analysis(clustered: pd.DataFrame):
    pass