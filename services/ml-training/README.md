# ML Training

Offline pipeline that trains the XGBoost fraud-scoring model consumed by `FraudScorer`
(`flink-jobs/ml-scoring`) at runtime. This directory is not part of the streaming path — it
produces a model artifact that gets uploaded to MinIO and loaded once when the Flink job starts.

## Pipeline

Run the notebooks in this order:

1. **`notebooks/generate.ipynb`** — synthesizes a labelled transaction dataset (normal traffic
   plus injected fraud patterns) and writes it to `data/data.csv`.
2. **`notebooks/EDA.ipynb`** *(optional)* — exploratory analysis of `data/data.csv`: amount
   distribution by label, hourly transaction volume, fraud rate by location type.
3. **`notebooks/training.ipynb`** — engineers the nine model features, tunes XGBoost
   hyperparameters with Optuna, trains the final classifier, evaluates it (ROC-AUC, PR-AUC,
   F1), and saves the booster to `data/model_detection.json`.
4. **`notebooks/testing.ipynb`** — validates the saved model against a separate labelled dataset
   and reports false positives/negatives.
5. **`export_ml.py`** — uploads `data/model_detection.json` to the `ml-models` bucket in MinIO,
   where `FraudScorer.open()` downloads it at job startup.

## Feature Schema

The model consumes exactly nine features, in this order, and it must stay in sync with
`FeatureExtractor.extract()` in `flink-jobs/ml-scoring`:

`amount`, `hour_of_day`, `is_home`, `is_domestic`, `avg_amount_24h`, `log_amount_ratio_24h`,
`tx_count_1h`, `tx_count_3h`, `time_since_last_tx_sec`

Any change to this list or its order must be mirrored in `FeatureExtractor.java`, or online
scoring will silently misalign feature values with what the model was trained on.

## Setup

```bash
pip install -r requirements.txt
jupyter lab notebooks/
```
