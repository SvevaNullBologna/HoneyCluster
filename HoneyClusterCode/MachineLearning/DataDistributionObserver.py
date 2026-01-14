import matplotlib.pyplot as plt
import seaborn as sns

import pandas as pd
from sklearn.decomposition import PCA


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