import logging
from pathlib import Path

import matplotlib.pyplot as plt
import seaborn as sns

import pandas as pd
from sklearn.decomposition import PCA

from Main.HoneyCluster import HoneyClusterPaths




def analizing(paths: HoneyClusterPaths):
    clusters_df = read_dataset(paths.clustered_result)
    normalized_df = read_dataset(paths.clustered_normalized)

    if clusters_df.empty or normalized_df.empty:
        return

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



#quello che serve a noi:
def plot_feature(df: pd.DataFrame, feature_name: str):
    plt.figure(figsize=(5, 10))
    plt.title(f"Analisi della caratteristica: {feature_name}")
    sns.boxplot(y=df[feature_name], color="skyblue")
    plt.ylabel(feature_name)
    plt.show()

def show_all_features(df: pd.DataFrame):
    # Rimuoviamo colonne non numeriche o cluster_id per non fare confusione
    df_plot = df.drop(columns=["cluster_id"], errors="ignore")
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

def plot_pca_selected_features(df: pd.DataFrame, selected_features: list, labels: pd.Series):
    """
    Esegue la PCA solo sulle feature selezionate e visualizza i cluster.
    """
    # 1. Filtraggio delle feature
    df_subset = df[selected_features]

    # 2. Esecuzione PCA
    pca = PCA(n_components=2)
    pca_result = pca.fit_transform(df_subset)

    # 3. Creazione del plot
    plt.figure(figsize=(12, 8))
    scatter = sns.scatterplot(
        x=pca_result[:, 0],
        y=pca_result[:, 1],
        hue=labels,
        palette='viridis',
        alpha=0.7,
        s=60,
        edgecolor='w'
    )

    # Calcolo della varianza spiegata per i titoli degli assi
    var_exp = pca.explained_variance_ratio_
    plt.title(f"PCA - Feature selezionate: {', '.join(selected_features)}", fontsize=14)
    plt.xlabel(f"PC1 ({var_exp[0]:.2%} varianza spiegata)")
    plt.ylabel(f"PC2 ({var_exp[1]:.2%} varianza spiegata)")

    plt.legend(title="Cluster ID", bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.grid(True, linestyle='--', alpha=0.4)
    plt.tight_layout()
    plt.show()

    # Opzionale: stampa l'importanza delle feature (loadings) per PC1 e PC2
    loadings = pd.DataFrame(
        pca.components_.T,
        columns=['PC1', 'PC2'],
        index=selected_features
    )
    print("\nImportanza delle Feature (Loadings) nei primi due componenti:")
    print(loadings)

def analysis_of_cluster(resulting_dataset: pd.DataFrame, scaled_dataset: pd.DataFrame):
    logging.info("Generazione grafici di analisi")

    show_all_features(resulting_dataset)

    # Se scaled_dataset è un numpy array, lo trasformiamo qui al volo
    if not isinstance(scaled_dataset, pd.DataFrame):
        # Prendiamo i nomi delle colonne dal dataset originale (escludendo l'id del cluster)
        feature_names = resulting_dataset.drop(columns=['cluster_id'], errors='ignore').columns
        scaled_dataset = pd.DataFrame(scaled_dataset, columns=feature_names)


    plot_pca_selected_features(scaled_dataset, TEMPORAL_FEATURES, resulting_dataset['cluster_id'])
    plot_pca_selected_features(scaled_dataset, COMMAND_FEATURES, resulting_dataset['cluster_id'])
    plot_pca_selected_features(scaled_dataset, BEHAVIORAL_FEATURES, resulting_dataset['cluster_id'])
    plot_pca_selected_features(scaled_dataset, ['inter_command_timing', 'session_duration'],
                               resulting_dataset['cluster_id'])  # varianza 87%
    plot_pca_selected_features(scaled_dataset, ['command_diversity_ratio', 'tool_signatures'],
                               resulting_dataset['cluster_id'])
    plot_pca_selected_features(scaled_dataset, ['unique_commands_ratio', 'tool_signatures'],
                               resulting_dataset['cluster_id'])  # 55%

    stats = resulting_dataset.groupby('cluster_id').mean()
    stats['count'] = resulting_dataset.groupby('cluster_id').size()
    return stats


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    paths = HoneyClusterPaths(Path("C:\\Users\\Sveva\\Documents\\GitHub\\zenodo_dataset"))
    pd = read_dataset(paths.clustered_result)
    show_all_features(pd)