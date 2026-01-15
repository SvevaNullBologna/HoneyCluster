import logging
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans

from Main.HoneyCluster import HoneyClusterPaths
from Zenodo.ZenodoProcesser import read_main_dataset

from joblib import dump,load


def clustering(honey_paths: HoneyClusterPaths):
    try:

        sample_data = _extraction_of_initial_clustering_subset(honey_paths.complete_dataset_file)

        # recupero lo stato precedente se esiste
        scaler = _get_scaler(honey_paths.scaler)
        model = _get_model(honey_paths.model)

        # preparo i dati (e salvo lo scaler aggiornato)
        scaled_sample_data, scaler = _build_scaled_core_model(sample_data, honey_paths.scaler,
                                                              scaler)  # aggiorna automaticamente lo scaler

        # il dataset core che stiamo usando (è un subset, quindi è importante metterlo da parte?)

        labels = _creating_clusters(scaled_sample_data, honey_paths.model, model)  # aggiorna automaticamente il model
        # otteniamo una lista del tipo [1,1, 0, 2, 1].
        # ogni sessione viene assegnata al centroide più vicino
        # invece che analizzare milioni di file, possiamo analizzare i 'rappresentanti' dei gruppi

        clustered_sample_data = sample_data.copy()
        clustered_sample_data[
            'cluster_id'] = labels  # aggiungiamo una colonna chiamata id del cluster e ci mettiamo le label

        _writing_as_parquet(clustered_sample_data, honey_paths.clustered_result)
        # _writing_as_parquet(scaled_sample_data, honey_paths.clustered_normalized)

    except Exception as e:
        logging.debug(f"errore nel clustering: {e}")



"""
//////////////////////////////////////////PIPELINE CLUSTERING////////////////////////////////
"""

def _extraction_of_initial_clustering_subset(complete_dataset: Path, n_samples: int = 200000) -> pd.DataFrame: # RAISES EXCEPTION!
    logging.info("Caricamento dataset principale")
    df = read_main_dataset(complete_dataset)

    if df.empty:
        logging.warning("dataset vuoto")
        raise Exception("dataset vuoto")

    # il dataset è sbilanciato a causa della grande presenza di attacchi di bot.
    # dei campioni del tutto casuali non basterebbero, quindi, cerchiamo prima gli attacchi significativi e poi quelli dei bot

    df_skilled = df[df['tool_signatures']>0] # se ci sono delle firme significative di tool usati, allora è veramente importante
    df_interactive = df[(df['unique_commands_ratio'] > 0.3) & (df['tool_signatures'] == 0)]
    df_bots = df[df['unique_commands_ratio']<= 0.3] # i bot sono quelli che ripetono sempre gli stessi comandi

    n_skilled_needed = min(len(df_skilled), int(n_samples * 0.2))
    n_interactive_needed = min(len(df_interactive), int(n_samples * 0.3))
    n_bots_needed = n_samples - n_skilled_needed - n_interactive_needed

    subset_skilled = df_skilled.sample(n_skilled_needed, random_state=42)
    subset_interactive = df_interactive.sample(n_interactive_needed, random_state=42)
    subset_bots = df_bots.sample(n_bots_needed, random_state=42)

    df_final = pd.concat([subset_skilled, subset_interactive, subset_bots])

    if df_final.empty:
        raise Exception("core dataset vuoto")

    #mischiamo per non avere i dati ordinati per classe
    return df_final.sample(frac=1, random_state=42).reset_index(drop=True)

def _get_scaler(scaler_path: Path):
    try:
        scaler = load(scaler_path)
        return scaler
    except FileNotFoundError:
        return None

def _get_model(model_path: Path):
    try:
        model = load(model_path)
        return model
    except FileNotFoundError:
        return None

def _save_scaler(scaler: StandardScaler, scaler_path: Path):
    dump(scaler, scaler_path)

def _save_model(model: KMeans, model_path: Path):
    dump(model, model_path)

def _build_scaled_core_model(dataset: pd.DataFrame, scaler_path: Path, old_scaler : StandardScaler = None): # scaliamo i dati per evitare che dei valori troppo grandi sovrastino gli altri

    if not old_scaler: # lo scaler impara, quindi riutilizzarlo è MEGLIO.
        scaler = StandardScaler() # impara media e deviazione standard per ciascuna riga del dataset
        scaled = scaler.fit_transform(dataset)
    else:
        scaler = old_scaler
        scaled = scaler.transform(dataset)

    _save_scaler(scaler, scaler_path) # salviamo lo scaler in un file

    return scaled, scaler # restituiamo i dati scalati

def _creating_clusters(scaled_core, model_path: Path, old_model: KMeans = None):
    if not old_model:
        model = KMeans( #  impara la posizione dei centroidi
            n_clusters=3,
            init="k-means++", #sceglie come centroidi i punti più lontani tra loro
            n_init=10, #evita che l'algoritmo si blocchi in soluzioni non ottimali
            max_iter=300, # max tentativi per esecuzione
            random_state=42 # componente altresì casuale. Rende i risultati riproducibili
        )
        model.fit_predict(scaled_core)
        labels = model.labels_
    else:
        model = old_model
        labels = model.predict(scaled_core) # restituisce l'oggetto modello stesso, non le labels come in sklearn

    _save_model(model, model_path)

    return labels

def _writing_as_parquet(clustered_data: pd.DataFrame, output_path: Path):
    if isinstance(clustered_data, np.ndarray):
        clustered_data = pd.DataFrame(clustered_data)
    clustered_data.to_parquet(output_path)





if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    paths = HoneyClusterPaths(Path("C:\\Users\\Sveva\\Documents\\GitHub\\zenodo_dataset"))
    clustering(paths)

