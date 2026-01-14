import logging
from pathlib import Path

import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans

from MachineLearning.DataDistributionObserver import show_all_features, plot_pca_selected_features
from MachineLearning.HoneyClusterData import HoneyClusterData
from Zenodo.ZenodoProcesser import read_main_dataset

from joblib import dump,load

_ARTIFACTS = "C:\\Users\\Sveva\\Documents\\GitHub\\zenodo_dataset\\artifacts"
_SCALER_PATH = Path(_ARTIFACTS,"scaler_honeypot.joblib")
_MODEL_PATH = Path(_ARTIFACTS,"kmeans_model.joblib")
_CORE_DATASET_PATH = Path(_ARTIFACTS,"core_dataset.parquet")

def create_folders():
    # crea la cartella atifacts se non esiste (ci depositeremo lo scaler e il model)
    _ARTIFACTS_PATH = Path(_ARTIFACTS)
    _ARTIFACTS_PATH.mkdir(parents=True, exist_ok=True)

def _extraction_of_initial_clustering_subset(base_folder_path: Path, n_samples: int = 200000) -> pd.DataFrame: # RAISES EXCEPTION!
    logging.info("Caricamento dataset principale")
    df = read_main_dataset(base_folder_path)

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

def _save_model(model: KMeans):
    dump(model, _MODEL_PATH)

def _build_scaled_core_model(dataset: pd.DataFrame, old_scaler : StandardScaler = None): # scaliamo i dati per evitare che dei valori troppo grandi sovrastino gli altri

    if not old_scaler: # lo scaler impara, quindi riutilizzarlo è MEGLIO.
        scaler = StandardScaler() # impara media e deviazione standard per ciascuna riga del dataset
        scaled = scaler.fit_transform(dataset)
    else:
        scaler = old_scaler
        scaled = scaler.transform(dataset)

    _save_scaler(scaler) # salviamo lo scaler in un file

    return scaled, scaler # restituiamo i dati scalati

def _clustering(scaled_core, old_model: KMeans = None):
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

    _save_model(model)

    return labels

def complete_clustering_pipeline(base_folder_path: Path):
    try:
        create_folders()

        sample_data = _extraction_of_initial_clustering_subset(base_folder_path)

        # recupero lo stato precedente se esiste
        scaler = _get_scaler()
        model = _get_model()

        # preparo i dati (e salvo lo scaler aggiornato)
        scaled_sample_data, scaler = _build_scaled_core_model(sample_data, scaler) # aggiorna automaticamente lo scaler

        # il dataset core che stiamo usando (è un subset, quindi è importante metterlo da parte?)

        labels = _clustering(scaled_sample_data, model) #aggiorna automaticamente il model
        #otteniamo una lista del tipo [1,1, 0, 2, 1].
        #ogni sessione viene assegnata al centroide più vicino
        #invece che analizzare milioni di file, possiamo analizzare i 'rappresentanti' dei gruppi

        clustered_sample_data = sample_data.copy()
        clustered_sample_data['cluster_id'] = labels # aggiungiamo una colonna chiamata id del cluster e ci mettiamo le label

        return clustered_sample_data, scaled_sample_data
    except Exception as e:
        print(e)
        return None, None




def analysis_of_cluster(resulting_dataset: pd.DataFrame, scaled_dataset: pd.DataFrame):
    logging.info("Generazione grafici di analisi")

    #show_all_features(resulting_dataset)

    # Se scaled_dataset è un numpy array, lo trasformiamo qui al volo
    if not isinstance(scaled_dataset, pd.DataFrame):
        # Prendiamo i nomi delle colonne dal dataset originale (escludendo l'id del cluster)
        feature_names = resulting_dataset.drop(columns=['cluster_id'], errors='ignore').columns
        scaled_dataset = pd.DataFrame(scaled_dataset, columns=feature_names)

    TEMPORAL_FEATURES = ['inter_command_timing', 'session_duration', 'time_of_day_patterns_sin',
                         'time_of_day_patterns_cos']
    COMMAND_FEATURES = ['unique_commands_ratio', 'command_diversity_ratio', 'tool_signatures']
    BEHAVIORAL_FEATURES = ['reconnaissance_vs_exploitation_ratio', 'error_rate', 'command_correction_attempts']

    #plot_pca_selected_features(scaled_dataset, TEMPORAL_FEATURES, resulting_dataset['cluster_id'])
    #plot_pca_selected_features(scaled_dataset, COMMAND_FEATURES, resulting_dataset['cluster_id'])
    #plot_pca_selected_features(scaled_dataset, BEHAVIORAL_FEATURES, resulting_dataset['cluster_id'])
    #plot_pca_selected_features(scaled_dataset, ['inter_command_timing', 'session_duration'], resulting_dataset['cluster_id']) # varianza 87%
    #plot_pca_selected_features(scaled_dataset, ['command_diversity_ratio','tool_signatures'], resulting_dataset['cluster_id'])
    # plot_pca_selected_features(scaled_dataset, ['unique_commands_ratio','tool_signatures'], resulting_dataset['cluster_id']) # 55%


    stats = resulting_dataset.groupby('cluster_id').mean()
    stats['count'] = resulting_dataset.groupby('cluster_id').size()
    return stats


if __name__ == "__main__":
    #logging.basicConfig(level=logging.DEBUG)
    base_folder = Path("C:\\Users\\Sveva\\Documents\\GitHub\\zenodo_dataset")
    clustered_dataset, scaled_dataset = complete_clustering_pipeline(base_folder)
    result = analysis_of_cluster(clustered_dataset, scaled_dataset)

