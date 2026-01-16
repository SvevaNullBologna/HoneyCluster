import logging
from pathlib import Path

import matplotlib.pyplot as plt
import seaborn as sns

import pandas as pd
from sklearn.decomposition import PCA

from Main.HoneyCluster import HoneyClusterPaths

from HoneyClustering import TEMPORAL_FEATURES, COMMAND_FEATURES, BEHAVIORAL_FEATURES



def analizing(paths: HoneyClusterPaths):
    datasets = get_all_datasets(paths)
    #show_all_box_plot_features(datasets["global"], "global")
    #show_all_box_plot_features(datasets["expertise"], "expertise")

    get_resulting_analysis_output(datasets, paths)

    #plot_pca(datasets["global"])
    #plot_pca(datasets["expertise"])

    #plot_pca_selected_features(datasets["temporal"], TEMPORAL_FEATURES, "temporal", "temporal features")
    #plot_pca_selected_features(datasets["command_based"], COMMAND_FEATURES, "command_based", "command based features")
    #plot_pca_selected_features(datasets["behavioral"], BEHAVIORAL_FEATURES, "behavioral", "behavioral features")
"""
////////////////////////////////////////PIPELINE FOR ANALIZING//////////////////////////////////////////////////////////////////////////////////
"""

def read_dataset(dataset_path: Path) -> pd.DataFrame:
    try:
        df = pd.read_parquet(dataset_path)
        return df
    except Exception as e:
        logging.debug(f"errore nel file di clustering: {e}")
        return pd.DataFrame()


def get_all_datasets(paths: HoneyClusterPaths) -> dict:
    return {
        "global": read_dataset(paths.clustered_result),
        "expertise": read_dataset(paths.clustered_for_expertise_result),
        "temporal": read_dataset(paths.clustered_for_time_result),
        "command_based": read_dataset(paths.clustered_for_command_result),
        "behavioral": read_dataset(paths.clustered_for_behavior_result)
    }


def show_all_box_plot_features(df: pd.DataFrame, cluster_column_name: str) :
    # Rimuoviamo colonne non numeriche o cluster_id per non fare confusione
    df_plot = df.drop(columns=[f"cluster_{cluster_column_name}_id"], errors="ignore")
    # Selezioniamo solo le colonne numeriche (il boxplot non funziona sulle stringhe)
    df_plot = df_plot.select_dtypes(include=['number'])

    cols = df_plot.columns
    n_features = len(cols)

    # Calcoliamo dinamicamente quante righe servono per avere 3 colonne
    n_cols = 3
    n_rows = (n_features + n_cols - 1) // n_cols

    fig, axes = plt.subplots(nrows=n_rows, ncols=n_cols, figsize=(15, 5 * n_rows))
    fig.suptitle("Distribuzione delle caratteristiche", fontsize=16)

    # Appiattiamo gli assi (indispensabile se n_rows > 1)
    axes = axes.flatten()

    for i, col_name in enumerate(cols):
        sns.boxplot(y=df_plot[col_name], ax=axes[i], color="skyblue")
        axes[i].set_title(col_name)
        axes[i].set_ylabel("")

    # Nascondiamo i quadrati vuoti se n_features < n_rows * n_cols
    for j in range(i + 1, len(axes)):
        axes[j].axis('off')

    plt.tight_layout(rect=[0, 0.03, 1, 0.95])
    plt.show()

def plot_pca(df: pd.DataFrame) -> None:
    pass

def plot_pca_selected_features(df: pd.DataFrame, selected_features: list, cluster_column: str, title: str):
    """
    PCA su colonne selezionate (es. TEMPORAL_FEATURES, COMMAND_FEATURES, BEHAVIORAL_FEATURES)
    Se cluster_column è None, non coloriamo per cluster.
    """
    if df.empty or len(selected_features) == 0:
        return

    X = df[selected_features].dropna()
    labels = None
    if cluster_column:
        cluster_column = f"cluster_{cluster_column}_id"
        labels = df.loc[X.index, cluster_column]

    pca = PCA(n_components=2)
    components = pca.fit_transform(X)

    pca_df = pd.DataFrame(components, columns=['PC1', 'PC2'])
    if labels is not None:
        pca_df['Cluster'] = labels.values

    plt.figure(figsize=(8, 6))
    if labels is not None:
        sns.scatterplot(x='PC1', y='PC2', hue='Cluster', palette='tab10', data=pca_df, s=60, alpha=0.8)
    else:
        sns.scatterplot(x='PC1', y='PC2', data=pca_df, s=60, alpha=0.8, color="skyblue")

    if not title:
        title = "PCA su feature selezionate"
    plt.title(title)
    plt.xlabel(f"PC1 ({pca.explained_variance_ratio_[0]*100:.1f}% varianza)")
    plt.ylabel(f"PC2 ({pca.explained_variance_ratio_[1]*100:.1f}% varianza)")
    plt.legend(title='Cluster') if labels is not None else None
    plt.tight_layout()
    plt.show()


def get_resulting_analysis_output(datasets: dict, paths: HoneyClusterPaths):
    all_data = []
    for dataset_type, dataset in datasets.items():
        if dataset.empty:
            continue

        # Calcoliamo le statistiche ignorando completamente i cluster
        stats_df = _get_resulting_analysis_datas(dataset)

        if not stats_df.empty:
            stats_df['dataset_type'] = dataset_type
            all_data.append(stats_df)

    if not all_data:
        logging.warning("Nessun dato trovato")
        return None

    result_df = pd.concat(all_data, axis=0, ignore_index=True)

    # Solo quello che ti serve veramente
    desired_cols = ['dataset_type', 'feature', 'mean', 'std', 'min', 'median', 'max']
    result_df = result_df[desired_cols]

    # Salvataggio doppio per comodità
    result_df.to_parquet(paths.analysis_result_path)
    result_df.to_csv(paths.analysis_result_path.with_suffix('.csv'), index=False)

    return result_df


def _get_resulting_analysis_datas(df: pd.DataFrame):
    if df.empty:
        return pd.DataFrame()

    # Prendi solo i numeri (escludendo eventuali ID se presenti)
    df_numeric = df.select_dtypes(include=['number']).copy()

    # Rimuoviamo preventivamente qualsiasi colonna che puzzi di ID cluster
    cols_to_drop = [c for c in df_numeric.columns if 'cluster' in c.lower() or 'id' in c.lower()]
    df_numeric = df_numeric.drop(columns=cols_to_drop)

    # Statistiche "flat" (una riga per feature)
    stats = df_numeric.agg(['mean', 'std', 'min', 'median', 'max']).transpose()
    stats.index.name = 'feature'

    return stats.reset_index()

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    honey_paths = HoneyClusterPaths(Path("C:\\Users\\Sveva\\Documents\\GitHub\\zenodo_dataset"))
    analizing(honey_paths)
