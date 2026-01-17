import logging
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans

from Main.HoneyCluster import HoneyClusterPaths
from Zenodo.ZenodoProcesser import read_main_dataset

from joblib import dump,load


def clustering(honey_paths: HoneyClusterPaths): # dimostra quanto i bot appiattiscono la nostra ricerca, dato che il loro traffico è l'80%, nonostante un pre-sampling mirato
    try:

        sample_data = _extraction_of_initial_clustering_subset(honey_paths.complete_dataset_file)

        # recupero lo stato precedente se esiste
        scaler = _get_scaler(honey_paths.scaler_path)
        model = _get_model(honey_paths.model_path)

        # preparo i dati (e salvo lo scaler aggiornato)
        scaled_sample_data = _build_scaled_core_model(sample_data, honey_paths.scaler_path,scaler)  # aggiorna automaticamente lo scaler

        # il dataset core che stiamo usando (è un subset, quindi è importante metterlo da parte?)

        labels = _creating_clusters(scaled_sample_data, honey_paths.model_path, model)  # aggiorna automaticamente il model
        # otteniamo una lista del tipo [1,1, 0, 2, 1].
        # ogni sessione viene assegnata al centroide più vicino
        # invece che analizzare milioni di file, possiamo analizzare i 'rappresentanti' dei gruppi

        clustered_sample_data = sample_data.copy()
        clustered_sample_data['cluster_global_id'] = labels  # aggiungiamo una colonna chiamata id del cluster e ci mettiamo le label

        _writing_as_parquet(clustered_sample_data, honey_paths.clustered_result.with_suffix(".parquet"))
        _writing_as_csv(clustered_sample_data, honey_paths.clustered_result.with_suffix(".csv"))

    except Exception as e:
        logging.debug(f"errore nel clustering: {e}")

def expertise_clustering(honey_paths: HoneyClusterPaths):
    # which df shoud I pass? The extracted initial clustering from the function "_extraction_of_initial_clustering_subset"?
    initial_dataset = read_main_dataset(honey_paths.complete_dataset_file)

    if initial_dataset.empty:
        logging.warning("dataset vuoto")
        raise Exception("dataset vuoto")

    try:
        df = _expertise_stage_1(initial_dataset)
        clustered_df = _expertise_stage_2(df,honey_paths)
        _writing_as_parquet(clustered_df, honey_paths.clustered_for_expertise_result.with_suffix(".parquet"))
        _writing_as_csv(clustered_df, honey_paths.clustered_for_time_result.with_suffix(".csv"))
    except Exception as e:
        logging.debug(f"errore nell'expertise clustering: {e}")

def features_clustering(honey_paths: HoneyClusterPaths):
    initial_dataset = read_main_dataset(honey_paths.complete_dataset_file)

    if initial_dataset.empty:
        logging.warning("dataset vuoto")
        raise Exception("dataset vuoto")

    try:
        _feature_clustering_time(initial_dataset,honey_paths)
        _feature_clustering_command(initial_dataset,honey_paths)
        _feature_clustering_behavior(initial_dataset,honey_paths)
    except Exception as e:
        logging.debug(f"errore nell'feature clustering: {e}")

"""
/////////////////////////////////////////////////EXPERTISE CLUSTERING///////////////////////////////////////////////////////////////////////////////////////
"""

def _expertise_stage_1(raw_complete_dataset: pd.DataFrame) -> pd.DataFrame:
    df = raw_complete_dataset.copy()

    # Versione corretta sintatticamente
    df['is_bot'] = (
            (df['inter_command_timing'] < 2) &
            (df['unique_commands_ratio'] < 0.1) &
            (df['command_correction_attempts'] == 0.0)
    )
    logging.info(f"Bot detected: {df['is_bot'].mean() * 100:.2f}%")
    return df

def _expertise_stage_2(df: pd.DataFrame, honey_paths: HoneyClusterPaths) -> pd.DataFrame:
    """ clustering sugli attackers interattivi """

    df_interactive = df[~df['is_bot']].copy()

    feature_cols = df_interactive.drop(
        columns=['cluster_id', 'is_bot'],
        errors='ignore'
    ).select_dtypes(include='number').columns

    scaler = _get_scaler(honey_paths.expertise_scaler_path)
    model = _get_model(honey_paths.expertise_model_path)

    scaled_interactive = _build_scaled_core_model(df_interactive[feature_cols],honey_paths.expertise_scaler_path, scaler)
    labels = _creating_clusters(scaled_interactive, honey_paths.expertise_model_path, model, n_clusters = 2)

    df_interactive['cluster_expertise_id'] = labels
    return df_interactive
"""
/////////////////////////////////////////////////FEATURE CLUSTERING//////////////////////////////////////////////////////////////////////////////////////////
"""

# otteniamo 3 rappresentazioni dello stesso fenomeno
TEMPORAL_FEATURES = ['inter_command_timing', 'session_duration', 'time_of_day_patterns_sin', 'time_of_day_patterns_cos']
COMMAND_FEATURES = ['unique_commands_ratio', 'command_diversity_ratio', 'tool_signatures']
BEHAVIORAL_FEATURES = ['reconnaissance_vs_exploitation_ratio', 'error_rate', 'command_correction_attempts']

def _feature_clustering_time(df: pd.DataFrame, honey_paths: HoneyClusterPaths):
    clustered_df = _feature_clustering(df, TEMPORAL_FEATURES, "temporal", honey_paths)
    _writing_as_parquet(clustered_df, honey_paths.clustered_for_time_result.with_suffix(".parquet"))
    _writing_as_csv(clustered_df, honey_paths.clustered_for_time_result.with_suffix(".csv"))

def _feature_clustering_command(df: pd.DataFrame, honey_paths: HoneyClusterPaths):
    clustered_df = _feature_clustering(df, COMMAND_FEATURES, 'command_based', honey_paths)
    _writing_as_parquet(clustered_df, honey_paths.clustered_for_command_result.with_suffix(".parquet"))
    _writing_as_csv(clustered_df, honey_paths.clustered_for_command_result.with_suffix(".csv"))

def _feature_clustering_behavior(df: pd.DataFrame, honey_paths: HoneyClusterPaths):
    clustered_df = _feature_clustering(df, BEHAVIORAL_FEATURES, 'behavioral', honey_paths)
    _writing_as_parquet(clustered_df, honey_paths.clustered_for_behavior_result.with_suffix(".parquet"))
    _writing_as_csv(clustered_df, honey_paths.clustered_for_behavior_result.with_suffix(".csv"))

def _feature_clustering(dataset: pd.DataFrame, features: list, label_name: str, honey_paths: HoneyClusterPaths):
    if dataset.empty:
        logging.warning("dataset vuoto")
        raise Exception("dataset vuoto")

    scaler_feature_path = honey_paths.scalers_folder.joinpath(label_name + ".joblib")
    scaler = _get_scaler(scaler_feature_path)

    feature_dataset = dataset[features]

    scaled_feature_dataset = _build_scaled_core_model(feature_dataset, scaler_feature_path , scaler )

    model_feature_path = honey_paths.models_folder.joinpath(label_name + ".joblib")
    model = _get_model(model_feature_path)

    labels = _creating_clusters(scaled_feature_dataset, model_feature_path, model, n_clusters = 2)

    dataset[f'cluster_{label_name.strip()}_id'] = labels

    return dataset

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

    return scaled # restituiamo i dati scalati

def _creating_clusters(scaled_core, model_path: Path, old_model: KMeans = None, n_clusters : int = 3, n_init : int = 10, max_iter : int = 300, random_state : int = 42):
    if not old_model:
        model = KMeans( #  impara la posizione dei centroidi
            n_clusters= n_clusters,
            init="k-means++", #sceglie come centroidi i punti più lontani tra loro
            n_init=n_init, #evita che l'algoritmo si blocchi in soluzioni non ottimali
            max_iter= max_iter, # max tentativi per esecuzione
            random_state=random_state # componente altresì casuale. Rende i risultati riproducibili
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

def _writing_as_csv(clustered_data: pd.DataFrame, output_path: Path):
    """
    Scrive il DataFrame come CSV.
    Se il file esiste, accoda senza riscrivere l'header.
    """
    if isinstance(clustered_data, np.ndarray):
        clustered_data = pd.DataFrame(clustered_data)

    output_path = output_path.with_suffix(".csv")
    write_header = not output_path.exists()

    clustered_data.to_csv(
        output_path,
        mode='a',
        header=write_header,
        index=False
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    paths = HoneyClusterPaths(Path("C:\\Users\\Sveva\\Documents\\GitHub\\zenodo_dataset"))
    clustering(paths)
    features_clustering(paths)
    expertise_clustering(paths)


