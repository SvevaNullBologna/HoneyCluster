import logging
import os.path

import numpy as np
from sklearn.neighbors import NearestNeighbors
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import DBSCAN

from joblib import dump
from joblib import load

import pandas as pd

from kneed import KneeLocator # libreria matematica per automatizzare il rilevamento dell'elbow

ARTIFACTS_DIR = "artifacts"

def _ensure_artifacts_dir():
    os.makedirs(ARTIFACTS_DIR, exist_ok=True)

def _toggle_not_normalizable_columns(dataset : pd.DataFrame ,colum_names:list[str]):
    return dataset.drop(colum_names, axis=1)

def _normalize_data(dataset: pd.DataFrame, previous_scaler: StandardScaler = None):
    """
        normalizziamo i dati per evitare che uno sovrasti l'altro per le dimensioni troppo diverse
    """
    session_ids = dataset["session_id"] # li togliamo e mettiamo da parte, perché non vanno normalizzabili (sono stringhe)
    features_only = _toggle_not_normalizable_columns(dataset, ["session_id"])


    if previous_scaler is None: # attenzione! Riutilizziamo lo scaler precedente se possibile perché è COLUI CHE IMPARA
        scaler = StandardScaler() # restituiamo lo scaler perché contiene in sè proprio l'evoluzione dell'AI
        scaled_array = scaler.fit_transform(features_only) # SCALIAMO
    else:
        scaler = previous_scaler
        scaled_array = scaler.transform(features_only) # SCALIAMO

    # restituiamo il dataset normalizzato riaggiungendo session_id e lo scaler utilizzato
    return (
        pd.DataFrame(scaled_array, columns=features_only.columns, index=session_ids),
        scaler
    )

def _stimate_best_eps_and_min_samples(datased: pd.DataFrame):

    n_features = datased.shape[1] # numero di features = numero colonne - indice

    min_samples = n_features + 1

    # k-distance
    neighbors = NearestNeighbors(n_neighbors=min_samples)
    neighbors.fit(datased)

    distances, _ = neighbors.kneighbors(datased)
    k_distances = np.sort(distances[:, -1])

    # Knee detection
    kneedle = KneeLocator(
        range(len(k_distances)),
        k_distances,
        curve="convex",
        direction="increasing"
    )

    if kneedle.knee is not None:
        epsilon = k_distances[kneedle.knee]
    else:
        epsilon = np.percentile(k_distances, 90)
        logging.warning("KneeLocator non ha trovato un punto ottimale, uso il 90° percentile.")


    return float(epsilon), float(min_samples)

def _cluster_data(scaled_dataset: pd.DataFrame, old_dbscan: DBSCAN = None) :
    # eseguiamo il clustering di tipo DBSCAN * nella sezione documenti viene spiegato

    if not old_dbscan:
        eps, min_samples =  _stimate_best_eps_and_min_samples(scaled_dataset)
        dbscan = DBSCAN(eps=eps, min_samples=min_samples)
    else:
        dbscan = old_dbscan

    labels = dbscan.fit_predict(scaled_dataset) # se restituisce -1 allora non appartengono a nessun cluster denso

    clustered = scaled_dataset.copy()
    clustered["cluster"] = labels

    return clustered, dbscan

def _save_data(scaler: StandardScaler, dbscan: DBSCAN, scaled_dataset: pd.DataFrame, clustered: pd.DataFrame) -> None:
    _ensure_artifacts_dir()

    #salvataggio oggetti intelligenti (modelli)
    dump(scaler, f"{ARTIFACTS_DIR}/scaler.joblib") # con joblib salviamo l'oggetto intero in un file binario
    dump(dbscan, f"{ARTIFACTS_DIR}/dbscan.joblib")

    #salvataggio dati in file (tabelle)
    scaled_dataset.to_parquet(f"{ARTIFACTS_DIR}/scaled_data.parquet") # formato tabellare più efficiente del CSV. Compresso, veloce e mantiene i tipi di dati.
    clustered.to_parquet(f"{ARTIFACTS_DIR}/clustered_results.parquet")

    logging.info("tabelle, scaler salvati")

def _load_trained_scaler_and_dbscan():
    try:
        scaler = load(f"{ARTIFACTS_DIR}/scaler.joblib")
    except FileNotFoundError:
        scaler = None
    try:
        dbscan = load(f"{ARTIFACTS_DIR}/dbscan.joblib")
    except FileNotFoundError:
        dbscan = None
    return scaler, dbscan

def _load_previous_result():

    try :
        sc_data = pd.read_parquet(f"{ARTIFACTS_DIR}/scaled_data.parquet")
    except FileNotFoundError:
        sc_data = None
    try:
        cl_data = pd.read_parquet(f"{ARTIFACTS_DIR}/clustered_results.parquet")
    except FileNotFoundError:
        cl_data = None

    return sc_data, cl_data

def run_honey_clustering_pipeline():
    pass

"""
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
"""