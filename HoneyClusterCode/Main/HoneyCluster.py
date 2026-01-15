from pathlib import Path

class HoneyClusterPaths:
    def __init__(self, base_path):
        self.base_folder = Path(base_path)
        self.original_folder = self.base_folder / "original"
        self.cleaned_folder = Path(base_path,"cleaned")
        self.cleaned_folder.mkdir(parents=True, exist_ok=True)
        self.processed_folder = Path(base_path,"processed")
        self.processed_folder.mkdir(parents=True, exist_ok=True)
        self.complete_dataset_file = Path(base_path,"complete_dataset.parquet")
        self.artifacts_folder = Path(base_path,"artifacts")
        self.artifacts_folder.mkdir(parents=True, exist_ok=True)
        self.scaler = Path(self.artifacts_folder,"scaler_honeypot.joblib")
        self.model = Path(self.artifacts_folder,"model_honeypot.joblib")
        self.core = Path(self.artifacts_folder,"core_dataset.parquet")
        self.clustering_results_folder = Path(self.artifacts_folder,"clustering_results")
        self.clustering_results_folder.mkdir(parents=True, exist_ok=True)
        self.clustered_result = Path(self.clustering_results_folder,"clustered_result.parquet")
        self.clustered_normalized = Path(self.clustering_results_folder,"clustered_normalized.parquet")