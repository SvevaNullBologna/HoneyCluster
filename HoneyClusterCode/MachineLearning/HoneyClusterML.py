import logging
import os.path
from pathlib import Path

import numpy as np
from sklearn.neighbors import NearestNeighbors
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import HDBSCAN

from joblib import dump
from joblib import load

import pandas as pd


from kneed import KneeLocator # libreria matematica per automatizzare il rilevamento dell'elbow

"""
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
                                                                        PRIVATE PIPELINE METHODS
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
"""


ARTIFACTS_DIR = "C:\\Users\\Sveva\\Documents\\GitHub\\zenodo_dataset\\artifacts"

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

"""
def _stimate_best_eps_and_min_samples(datased: pd.DataFrame):

    n_features = datased.shape[1] # numero di features = numero colonne - indice

    min_samples = n_features + 1

    # k-distance
    neighbors = NearestNeighbors(n_neighbors=min_samples, n_jobs=-1)
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


def _cluster_data(scaled_dataset: pd.DataFrame, old_dbscan: HDBSCAN = None) :
    # eseguiamo il clustering di tipo DBSCAN * nella sezione documenti viene spiegato

    if not old_dbscan:
        eps, min_samples =  _stimate_best_eps_and_min_samples(scaled_dataset)
        hdbscan = HDBSCAN(eps=eps, min_samples=min_samples, n_jobs=-1)
    else:
        dbscan = old_dbscan

    labels = dbscan.fit_predict(scaled_dataset) # se restituisce -1 allora non appartengono a nessun cluster denso

    clustered = scaled_dataset.copy()
    clustered["cluster"] = labels

    return clustered, dbscan

"""

def _cluster_data(scaled_dataset: pd.DataFrame, old_hdbscan: HDBSCAN = None) :
    clusterer = HDBSCAN(min_cluster_size=50)

    labels = clusterer.fit_predict(scaled_dataset)
    clustered = scaled_dataset.copy()
    clustered["cluster"] = labels

    return clustered, None

def _save_data(scaler: StandardScaler, dbscan: HDBSCAN, clustered: pd.DataFrame) -> None:
    _ensure_artifacts_dir()

    #salvataggio oggetti intelligenti (modelli)
    dump(scaler, f"{ARTIFACTS_DIR}/scaler.joblib") # con joblib salviamo l'oggetto intero in un file binario
    dump(dbscan, f"{ARTIFACTS_DIR}/dbscan.joblib")

    #salvataggio dati in file (tabelle)
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

"""
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
                                                                    PUBLIC/COMPLETE METHODS
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
"""


def clustering(complete_dataset_parquet_path : Path) :
    logging.info("Starting clustering pipeline...")
    _ensure_artifacts_dir() # controlla se esiste la cartella, in caso la crea

    #recuperiamo il dataset
    logging.info("Loading dataset...")
    df = pd.read_parquet(complete_dataset_parquet_path)

    #normalizzazione
    logging.info("Normalization...")
    old_scaler, old_dbscan = _load_trained_scaler_and_dbscan()
    scaled_dataset, current_scaler = _normalize_data(df, old_scaler)

    #clustering
    logging.info("clustering. Please be patient...")
    clustered_data, current_dbscan = _cluster_data(scaled_dataset, old_dbscan)

    #salvataggio modifiche
    _save_data(old_scaler, old_dbscan, clustered_data)

    return clustered_data



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
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
                                                                        EXECUTION
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
"""

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    clustered = clustering(Path("C:\\Users\\Sveva\\Documents\\GitHub\\zenodo_dataset\\complete_dataset.parquet"))
    summary = cluster_analysis(clustered)
    print(summary)