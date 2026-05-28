#!/usr/bin/env python3
import json
import math
import os
import sys
from datetime import timedelta
from pathlib import Path

from flask import Flask, jsonify, send_from_directory
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, DoubleType, IntegerType, StringType, StructField, StructType

os.environ.setdefault("TF_CPP_MIN_LOG_LEVEL", "2")

BASE_DIR = Path(__file__).resolve().parent
CSV_PATH = BASE_DIR / "data_harga_pangan_lampung_model_minimal.csv"
CACHE_PATH = BASE_DIR / "cache.json"
STATIC_DIR = BASE_DIR / "dashboard"
CACHE_VERSION = 2

TARGET_COL = "harga_rupiah"
WEATHER_COLS = ["suhu_rata2_c", "curah_hujan_mm", "kelembapan_rata2_pct"]
CALENDAR_COLS = [
    "hari_ke_minggu",
    "bulan",
    "is_akhir_pekan",
    "is_libur_nasional",
    "jarak_ke_idul_fitri_hari",
    "jarak_ke_natal_hari",
]
BASE_FEATURE_COLS = WEATHER_COLS + CALENDAR_COLS
FORECAST_HORIZON = 7
TEST_RATIO = 0.2
ANOMALY_RATE = 0.03
LSTM_WINDOW = 30
LSTM_EPOCHS = 20
LSTM_BATCH_SIZE = 16

cache = {}
spark = None
app = Flask(__name__, static_folder=str(STATIC_DIR), static_url_path="")


def get_spark():
    global spark
    if spark is None:
        os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
        os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
        os.environ.setdefault("SPARK_LOCAL_HOSTNAME", "localhost")
        spark = (
            SparkSession.builder.appName("DashboardHargaPanganLampung")
            .master("local[*]")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.ui.showConsoleProgress", "false")
            .config("spark.driver.memory", "2g")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("ERROR")
    return spark


def load_csv():
    schema = StructType(
        [
            StructField("tanggal", DateType(), True),
            StructField("kabupaten_kota", StringType(), True),
            StructField("kabkot_id", IntegerType(), True),
            StructField("komoditas", StringType(), True),
            StructField("harga_rupiah", DoubleType(), True),
            StructField("suhu_rata2_c", DoubleType(), True),
            StructField("curah_hujan_mm", DoubleType(), True),
            StructField("kelembapan_rata2_pct", DoubleType(), True),
            StructField("hari_ke_minggu", IntegerType(), True),
            StructField("bulan", IntegerType(), True),
            StructField("is_akhir_pekan", IntegerType(), True),
            StructField("is_libur_nasional", IntegerType(), True),
            StructField("jarak_ke_idul_fitri_hari", IntegerType(), True),
            StructField("jarak_ke_natal_hari", IntegerType(), True),
        ]
    )
    return (
        get_spark()
        .read.option("header", True)
        .option("dateFormat", "yyyy-MM-dd")
        .schema(schema)
        .csv(str(CSV_PATH))
        .where(F.col(TARGET_COL).isNotNull())
        .cache()
    )


def dataset_info(df):
    row = df.agg(
        F.count("*").alias("total_rows"),
        F.countDistinct("kabupaten_kota").alias("total_kabkot"),
        F.countDistinct("komoditas").alias("total_komoditas"),
        F.min("tanggal").alias("tanggal_awal"),
        F.max("tanggal").alias("tanggal_akhir"),
    ).first()
    komoditas = [r["komoditas"] for r in df.select("komoditas").distinct().orderBy("komoditas").collect()]
    return {
        "total_rows": int(row["total_rows"]),
        "total_kabkot": int(row["total_kabkot"]),
        "total_komoditas": int(row["total_komoditas"]),
        "komoditas_list": komoditas,
        "tanggal_awal": row["tanggal_awal"].isoformat(),
        "tanggal_akhir": row["tanggal_akhir"].isoformat(),
    }


def build_daily(df, komoditas):
    return (
        df.where(F.col("komoditas") == komoditas)
        .groupBy("tanggal")
        .agg(
            F.avg("harga_rupiah").alias("harga_rupiah"),
            F.avg("suhu_rata2_c").alias("suhu_rata2_c"),
            F.avg("curah_hujan_mm").alias("curah_hujan_mm"),
            F.avg("kelembapan_rata2_pct").alias("kelembapan_rata2_pct"),
            F.first("hari_ke_minggu").alias("hari_ke_minggu"),
            F.first("bulan").alias("bulan"),
            F.first("is_akhir_pekan").alias("is_akhir_pekan"),
            F.first("is_libur_nasional").alias("is_libur_nasional"),
            F.first("jarak_ke_idul_fitri_hari").alias("jarak_ke_idul_fitri_hari"),
            F.first("jarak_ke_natal_hari").alias("jarak_ke_natal_hari"),
        )
        .orderBy("tanggal")
    )


def add_forecast_features(daily):
    w = Window.orderBy("tanggal")
    w7 = w.rowsBetween(-7, -1)
    w30 = w.rowsBetween(-30, -1)
    featured = (
        daily.withColumn("lag_1", F.lag(TARGET_COL, 1).over(w))
        .withColumn("lag_7", F.lag(TARGET_COL, 7).over(w))
        .withColumn("lag_14", F.lag(TARGET_COL, 14).over(w))
        .withColumn("lag_30", F.lag(TARGET_COL, 30).over(w))
        .withColumn("rolling_7", F.avg(TARGET_COL).over(w7))
        .withColumn("rolling_30", F.avg(TARGET_COL).over(w30))
        .withColumn("trend_1", F.col(TARGET_COL) - F.col("lag_1"))
        .withColumn("trend_7", F.col(TARGET_COL) - F.col("lag_7"))
        .withColumn("row_num", F.row_number().over(w))
    )
    for horizon in range(1, FORECAST_HORIZON + 1):
        featured = featured.withColumn(f"label_h{horizon}", F.lead(TARGET_COL, horizon).over(w))
    return featured


def safe_int(value):
    if value is None or not math.isfinite(float(value)):
        return 0
    return int(round(float(value)))


def regression_metrics(rows, actual_col, pred_col):
    pairs = [
        (float(r[actual_col]), float(r[pred_col]))
        for r in rows
        if r[actual_col] is not None and r[pred_col] is not None and float(r[actual_col]) != 0
    ]
    if not pairs:
        return 0, 0, 0.0
    abs_errors = [abs(a - p) for a, p in pairs]
    sq_errors = [(a - p) ** 2 for a, p in pairs]
    pct_errors = [abs((a - p) / a) * 100 for a, p in pairs]
    mae = round(sum(abs_errors) / len(abs_errors), 0)
    rmse = round(math.sqrt(sum(sq_errors) / len(sq_errors)), 0)
    mape = round(sum(pct_errors) / len(pct_errors), 2)
    return mae, rmse, mape


def metric_from_arrays(actual_values, pred_values):
    pairs = [
        (float(actual), float(pred))
        for actual, pred in zip(actual_values, pred_values)
        if actual is not None and pred is not None and float(actual) != 0
    ]
    if not pairs:
        return 0, 0, 0.0
    abs_errors = [abs(a - p) for a, p in pairs]
    sq_errors = [(a - p) ** 2 for a, p in pairs]
    pct_errors = [abs((a - p) / a) * 100 for a, p in pairs]
    mae = round(sum(abs_errors) / len(abs_errors), 0)
    rmse = round(math.sqrt(sum(sq_errors) / len(sq_errors)), 0)
    mape = round(sum(pct_errors) / len(pct_errors), 2)
    return mae, rmse, mape


def iso_date(value):
    return value.date().isoformat() if hasattr(value, "date") else value.isoformat()


def fallback_forecast(daily):
    rows = daily.orderBy("tanggal").collect()
    if not rows:
        return None

    recent_rows = rows[-60:]
    hist_chart = [{"tanggal": r["tanggal"].isoformat(), "harga": safe_int(r[TARGET_COL])} for r in recent_rows]
    last_date = rows[-1]["tanggal"]
    last_price = safe_int(rows[-1][TARGET_COL])
    recent_prices = [float(r[TARGET_COL]) for r in rows[-7:] if r[TARGET_COL] is not None]
    forecast_price = safe_int(sum(recent_prices) / len(recent_prices)) if recent_prices else last_price

    eval_rows = []
    for idx in range(max(1, len(rows) - 20), len(rows)):
        actual = rows[idx][TARGET_COL]
        pred = rows[idx - 1][TARGET_COL]
        eval_rows.append({"tanggal": rows[idx]["tanggal"], "actual": actual, "prediction": pred})
    mae, rmse, mape = regression_metrics(eval_rows, "actual", "prediction")
    eval_chart = [
        {"tanggal": r["tanggal"].isoformat(), "aktual": safe_int(r["actual"]), "prediksi": safe_int(r["prediction"])}
        for r in eval_rows
    ]
    forecast_data = [
        {"hari_ke": offset, "tanggal": (last_date + timedelta(days=offset)).isoformat(), "harga": forecast_price}
        for offset in range(1, FORECAST_HORIZON + 1)
    ]
    return {
        "evaluasi": {"metrics": {"mae": mae, "rmse": rmse, "mape": mape}, "chart_data": eval_chart},
        "forecast_7_hari": {
            "historikal": hist_chart,
            "forecast": forecast_data,
            "metrics_horizon": {
                "mae": [mae for _ in range(FORECAST_HORIZON)],
                "mape": [mape for _ in range(FORECAST_HORIZON)],
            },
        },
        "jumlah_hari": int(len(rows)),
        "jumlah_sample": int(max(0, len(eval_rows))),
        "model": "Fallback Spark aggregate moving average untuk data pendek",
    }


def train_lstm_forecast(daily):
    total_days = daily.count()
    min_len = LSTM_WINDOW + FORECAST_HORIZON + 20
    if total_days <= min_len:
        return fallback_forecast(daily)

    try:
        import numpy as np
        import tensorflow as tf
        from sklearn.preprocessing import MinMaxScaler
        from tensorflow.keras.callbacks import EarlyStopping
        from tensorflow.keras.layers import LSTM, Dense, Dropout, Input
        from tensorflow.keras.models import Sequential
    except ImportError as exc:
        print(f"fallback: dependency LSTM belum tersedia ({exc})", end=" ", flush=True)
        return fallback_forecast(daily)

    pdf = daily.orderBy("tanggal").toPandas()
    feature_cols = [TARGET_COL] + BASE_FEATURE_COLS
    pdf[feature_cols] = pdf[feature_cols].apply(lambda col: col.astype(float))
    pdf[feature_cols] = pdf[feature_cols].ffill().bfill()

    if len(pdf) <= min_len or pdf[feature_cols].isna().any().any():
        return fallback_forecast(daily)

    values = pdf[feature_cols].to_numpy(dtype="float32")
    feature_scaler = MinMaxScaler()
    target_scaler = MinMaxScaler()
    scaled_features = feature_scaler.fit_transform(values)
    scaled_target = target_scaler.fit_transform(values[:, [0]])

    x_data = []
    y_data = []
    y_dates = []
    for idx in range(LSTM_WINDOW, len(pdf) - FORECAST_HORIZON + 1):
        x_data.append(scaled_features[idx - LSTM_WINDOW : idx])
        y_data.append(scaled_target[idx : idx + FORECAST_HORIZON, 0])
        y_dates.append(pdf.iloc[idx]["tanggal"])

    if len(x_data) < 25:
        return fallback_forecast(daily)

    x_data = np.asarray(x_data, dtype="float32")
    y_data = np.asarray(y_data, dtype="float32")
    split_idx = max(1, int(len(x_data) * (1 - TEST_RATIO)))
    if split_idx >= len(x_data):
        split_idx = len(x_data) - 1

    x_train, y_train = x_data[:split_idx], y_data[:split_idx]
    x_test, y_test = x_data[split_idx:], y_data[split_idx:]
    test_dates = y_dates[split_idx:]

    tf.keras.utils.set_random_seed(42)
    model = Sequential(
        [
            Input(shape=(LSTM_WINDOW, len(feature_cols))),
            LSTM(64, return_sequences=True),
            Dropout(0.2),
            LSTM(32),
            Dropout(0.2),
            Dense(32, activation="relu"),
            Dense(FORECAST_HORIZON),
        ]
    )
    model.compile(optimizer="adam", loss="mse")
    model.fit(
        x_train,
        y_train,
        epochs=LSTM_EPOCHS,
        batch_size=LSTM_BATCH_SIZE,
        validation_split=0.1 if len(x_train) >= 40 else 0.0,
        callbacks=[EarlyStopping(monitor="val_loss", patience=4, restore_best_weights=True)]
        if len(x_train) >= 40
        else None,
        verbose=0,
    )

    pred_test_scaled = model.predict(x_test, verbose=0)
    actual_test = target_scaler.inverse_transform(y_test.reshape(-1, 1)).reshape(y_test.shape)
    pred_test = target_scaler.inverse_transform(pred_test_scaled.reshape(-1, 1)).reshape(pred_test_scaled.shape)

    mae_metrics = []
    mape_metrics = []
    h1_rmse = 0
    for horizon_idx in range(FORECAST_HORIZON):
        mae, rmse, mape = metric_from_arrays(actual_test[:, horizon_idx], pred_test[:, horizon_idx])
        mae_metrics.append(mae)
        mape_metrics.append(mape)
        if horizon_idx == 0:
            h1_rmse = rmse

    eval_chart = [
        {
            "tanggal": iso_date(test_dates[i]),
            "aktual": safe_int(actual_test[i, 0]),
            "prediksi": safe_int(pred_test[i, 0]),
        }
        for i in range(len(test_dates))
    ]

    latest_x = scaled_features[-LSTM_WINDOW:].reshape(1, LSTM_WINDOW, len(feature_cols))
    forecast_scaled = model.predict(latest_x, verbose=0)[0].reshape(-1, 1)
    forecast_values = target_scaler.inverse_transform(forecast_scaled).ravel()
    last_date = pdf.iloc[-1]["tanggal"]
    forecast_data = [
        {
            "hari_ke": offset,
            "tanggal": iso_date(last_date + timedelta(days=offset)),
            "harga": max(0, safe_int(price)),
        }
        for offset, price in enumerate(forecast_values, 1)
    ]

    recent_pdf = pdf.tail(60)
    hist_chart = [
        {"tanggal": iso_date(row["tanggal"]), "harga": safe_int(row[TARGET_COL])}
        for _, row in recent_pdf.iterrows()
    ]

    return {
        "evaluasi": {
            "metrics": {"mae": mae_metrics[0], "rmse": h1_rmse, "mape": mape_metrics[0]},
            "chart_data": eval_chart,
        },
        "forecast_7_hari": {
            "historikal": hist_chart,
            "forecast": forecast_data,
            "metrics_horizon": {"mae": mae_metrics, "mape": mape_metrics},
        },
        "jumlah_hari": int(total_days),
        "jumlah_sample": int(len(x_data)),
        "model": "PySpark preprocessing + TensorFlow/Keras LSTM multivariate",
    }


def train_all_forecasts(df, kom_list):
    print("\n[2/3] Training LSTM forecasts for all commodities ...")
    results = {}
    for i, kom in enumerate(kom_list, 1):
        print(f"  [{i:02d}/{len(kom_list)}] {kom} ...", end=" ", flush=True)
        result = train_lstm_forecast(build_daily(df, kom))
        if result:
            results[kom] = result
            print(f"OK  MAPE={result['evaluasi']['metrics']['mape']}%")
        else:
            print("SKIP - data too short")
    print(f"  Done. {len(results)}/{len(kom_list)} trained.\n")
    return results


def train_isolation_forest_anomaly(df):
    print("[3/3] Training Isolation Forest anomaly detector ...", end=" ", flush=True)
    w = Window.partitionBy("kabupaten_kota", "komoditas").orderBy("tanggal")
    province_avg = Window.partitionBy("tanggal", "komoditas")
    anom = (
        df.withColumn("rata_prov", F.avg(TARGET_COL).over(province_avg))
        .withColumn("deviasi_pct", (F.col(TARGET_COL) - F.col("rata_prov")) / F.col("rata_prov") * 100)
        .withColumn("lag1", F.lag(TARGET_COL, 1).over(w))
        .withColumn("delta_pct", (F.col(TARGET_COL) - F.col("lag1")) / F.col("lag1") * 100)
        .fillna({"deviasi_pct": 0.0, "delta_pct": 0.0})
    )

    feature_cols = [
        TARGET_COL,
        "deviasi_pct",
        "delta_pct",
        "suhu_rata2_c",
        "curah_hujan_mm",
        "kelembapan_rata2_pct",
        "is_akhir_pekan",
        "is_libur_nasional",
        "jarak_ke_idul_fitri_hari",
        "jarak_ke_natal_hari",
    ]

    try:
        import pandas as pd
        from sklearn.ensemble import IsolationForest
        from sklearn.preprocessing import StandardScaler
    except ImportError as exc:
        raise RuntimeError(
            "Dependency anomaly detection belum tersedia. Install scikit-learn dan pandas."
        ) from exc

    select_cols = [
        "tanggal",
        "kabupaten_kota",
        "komoditas",
        TARGET_COL,
        "deviasi_pct",
        "delta_pct",
        "curah_hujan_mm",
        "is_libur_nasional",
    ] + feature_cols
    pdf = anom.select(*dict.fromkeys(select_cols)).toPandas()
    pdf[feature_cols] = pdf[feature_cols].apply(lambda col: pd.to_numeric(col, errors="coerce"))
    pdf[feature_cols] = pdf[feature_cols].fillna(0.0)

    scaler = StandardScaler()
    features = scaler.fit_transform(pdf[feature_cols])
    model = IsolationForest(
        n_estimators=200,
        contamination=ANOMALY_RATE,
        random_state=42,
        n_jobs=-1,
    )
    labels = model.fit_predict(features)
    scores = model.score_samples(features)
    pdf["is_anom"] = (labels == -1).astype(int)
    pdf["score"] = scores

    total_data = int(len(pdf))
    total_anom = int(pdf["is_anom"].sum())
    top_pdf = pdf[pdf["is_anom"] == 1].sort_values("score", ascending=True).head(50)
    top_data = [
        {
            "tanggal": iso_date(row["tanggal"]),
            "kabupaten_kota": row["kabupaten_kota"],
            "komoditas": row["komoditas"],
            "harga_rupiah": safe_int(row[TARGET_COL]),
            "deviasi_pct": round(float(row["deviasi_pct"]), 1),
            "delta_pct": round(float(row["delta_pct"]), 1),
            "curah_hujan_mm": round(float(row["curah_hujan_mm"]), 1),
            "is_libur_nasional": int(row["is_libur_nasional"]),
            "score": round(float(row["score"]), 4),
        }
        for _, row in top_pdf.iterrows()
    ]
    daily_pdf = (
        pdf.groupby("tanggal", as_index=False)["is_anom"]
        .sum()
        .sort_values("tanggal")
        .rename(columns={"is_anom": "jumlah"})
    )
    daily_list = [
        {"tanggal": iso_date(row["tanggal"]), "jumlah": int(row["jumlah"])}
        for _, row in daily_pdf.iterrows()
    ]
    print(f"Done. {total_anom} anomalies\n")
    return {
        "total_anomali": total_anom,
        "total_data": total_data,
        "pct_anomali": round(total_anom / total_data * 100, 2),
        "top_anomalies": top_data,
        "daily_counts": daily_list,
        "model": "PySpark feature engineering + scikit-learn Isolation Forest",
    }


def train_all(force=False):
    global cache
    if not force and CACHE_PATH.exists():
        print("Loading from cache.json ...", end=" ", flush=True)
        with open(CACHE_PATH, encoding="utf-8") as f:
            cache = json.load(f)
        if cache.get("cache_version") != CACHE_VERSION:
            print("STALE - retraining")
            cache = {}
        else:
            print("OK")
            i = cache["dataset_info"]
            n_pred = len(cache.get("predictions", {}))
            n_anom = cache["anomaly"]["total_anomali"]
            print(f"  {i['total_rows']:,} rows | {i['total_kabkot']} kab/kota | {i['total_komoditas']} commodities")
            print(f"  {n_pred} LSTM forecasts | {n_anom:,} anomalies cached")
            print()
            return

    if force and CACHE_PATH.exists():
        CACHE_PATH.unlink()

    print("=" * 55)
    print("  Dashboard Monitoring Harga Pangan Lampung")
    print("=" * 55)
    print("\n[1/3] Loading dataset with Spark ...", end=" ", flush=True)
    df = load_csv()
    cache["cache_version"] = CACHE_VERSION
    cache["dataset_info"] = dataset_info(df)
    info = cache["dataset_info"]
    print(f"OK\n      {info['total_rows']:,} rows | {info['total_kabkot']} kab/kota | {info['total_komoditas']} commodities")
    print(f"      {info['tanggal_awal']} ~ {info['tanggal_akhir']}")

    cache["predictions"] = train_all_forecasts(df, info["komoditas_list"])
    cache["anomaly"] = train_isolation_forest_anomaly(df)

    print("Saving cache.json ...", end=" ", flush=True)
    with open(CACHE_PATH, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False)
    print("OK")

    print("=" * 55)
    print("  Server ready! Open http://localhost:5000")
    print("=" * 55)


@app.route("/")
def index():
    return send_from_directory(str(STATIC_DIR), "index.html")


@app.route("/api/dataset-info")
def api_dataset_info():
    return jsonify(cache.get("dataset_info", {}))


@app.route("/api/predict/<komoditas>")
def api_predict(komoditas):
    result = cache.get("predictions", {}).get(komoditas)
    if not result:
        return jsonify({"error": f"Komoditas '{komoditas}' not found"}), 404
    return jsonify(result)


@app.route("/api/anomaly")
def api_anomaly():
    return jsonify(cache.get("anomaly", {}))


if __name__ == "__main__":
    force = "--force" in sys.argv or "--retrain" in sys.argv
    train_all(force=force)
    app.run(host="0.0.0.0", port=5000, debug=False)
