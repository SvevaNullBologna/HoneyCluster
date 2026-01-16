from pathlib import Path

class HoneyClusterPaths:
    def __init__(self, base_path):
        self.base_folder = Path(base_path)
        self.original_folder = self.base_folder / "original"
        """
            ORIGINAL -> CLEANING
        """
        self.cleaned_folder = Path(base_path,"cleaned")
        self.cleaned_folder.mkdir(parents=True, exist_ok=True)
        """
            CLEANING -> PROCESSING
        """
        self.processed_folder = Path(base_path,"processed")
        self.processed_folder.mkdir(parents=True, exist_ok=True)

        """
            PROCESSING -> CLUSTERING
        """
        self.complete_dataset_file = Path(base_path,"complete_dataset.parquet")
        self.artifacts_folder = Path(base_path,"artifacts")
        self.artifacts_folder.mkdir(parents=True, exist_ok=True)
        # SCALERS
        self.scalers_folder = Path(self.artifacts_folder, "scalers")
        self.scalers_folder.mkdir(parents=True, exist_ok=True)
        self.scaler_path = self.scalers_folder / "scaler.joblib"
        self.expertise_scaler_path = self.scalers_folder / "expertise_scaler.joblib"
        # different scalers for different features
        # MODELS
        self.models_folder = Path(self.artifacts_folder, "models")
        self.models_folder.mkdir(parents=True, exist_ok=True)
        self.model_path = self.models_folder / "model.joblib"
        self.expertise_model_path = self.models_folder / "expertise_model.joblib"
        # different models for different features
        # SAMPLED DATASET
        self.core = Path(self.artifacts_folder,"core_dataset.parquet")
        # CLUSTERING RESULTS
        self.clustering_results_folder = Path(self.artifacts_folder,"clustering_results")
        self.clustering_results_folder.mkdir(parents=True, exist_ok=True)
        # COMPLETE CLUSTERING
        self.clustered_result = Path(self.clustering_results_folder,"clustered_result.parquet")
        # BOTS VS EXPERTS
        self.clustered_for_expertise_result = Path(self.clustering_results_folder,"clustered_for_expertise_result.parquet")
        # FEATURES CLUSTERING
        self.clustered_for_time_result = Path(self.clustering_results_folder,"clustered_for_features_result.parquet")
        self.clustered_for_command_result = Path(self.clustering_results_folder,"clustered_for_command_result.parquet")
        self.clustered_for_behavior_result = Path(self.clustering_results_folder, "clustered_for_behavior_result.parquet")

        """
            CLUSTERING -> ANALYSIS 
        """
        self.analysis_result_folder = Path(self.base_folder,"analysis_result")
        self.analysis_result_folder.mkdir(parents=True, exist_ok=True)
        self.analysis_result_path = Path(self.analysis_result_folder,"analysis_result.parquet")