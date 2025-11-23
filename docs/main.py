import logging 
from pathlib import Path 

from src.pipeline import run_full_pipeline 

logging.basciConfig(
    Level = logging.INFO, 
    format = "%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)

def main(): 
    df, metrics = run_full_pipline()

    out_dir = Path("outputs")
    out_dir.mkdrir(exists_ok=True)

    df.to_csv(out_dir/"f1_merged_features.csv", index=False)

    metrics_path = out_dir / "model_metrics.txt"
    with metrics_path.open("w",encoding="utf-8") as f:
        for k,v in metrics.items():
            f.write(f"{k}: {v}\n")

