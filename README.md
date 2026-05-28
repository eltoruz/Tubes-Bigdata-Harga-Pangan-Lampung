# Sistem Analitik Big Data untuk Monitoring Harga Komoditas Pangan Pokok dan Deteksi Anomali Distribusi di Provinsi Lampung Berbasis Data Primer Disperindag       

Project ini merupakan tugas besar Mahadata/Big Data untuk membangun prototipe sistem monitoring, prediksi harga, dan deteksi anomali harga komoditas pangan di Provinsi Lampung. Sistem menggunakan data harga pangan harian dari 15 kabupaten/kota, diperkaya dengan data cuaca dan fitur kalender, kemudian diproses melalui pipeline PySpark sebelum masuk ke model analitik.

## Anggota Kelompok

| Nama | NIM |
|---|---|
| Zaky Ahmad Makarim | 122140182 |
| Rachel Olivia M | 122140181 |
| Luthfianya Isyathun Rodiyyah | 122140185 |
| Novia Listiani | 122140192 |
| Andika Rahman Pratama | 123140090 |
| Muhammad Bintang Al-Fasya | 123140098 |
| Rifael Eurico Sitorus | 123140077 |
| Yonatanoel Petra Hutabarat | 123140100 |

## Ringkasan Project

Sistem ini dibuat untuk membantu pemantauan harga pangan tingkat provinsi melalui dua modul utama:

1. **Prediksi harga 7 hari ke depan**
   Menggunakan model LSTM multivariate dengan input harga historis, data cuaca, dan fitur kalender.

2. **Deteksi anomali harga**
   Menggunakan Isolation Forest untuk menandai data harga yang tidak biasa, misalnya harga yang menyimpang dari rata-rata provinsi atau perubahan harga harian yang ekstrem.

Dashboard web disediakan untuk menampilkan ringkasan dataset, grafik prediksi harga, metrik evaluasi model, jumlah anomali, dan tabel anomali teratas.

## Implementasi Big Data

Fokus big data pada project ini berada pada **pipeline pemrosesan data berbasis PySpark**, bukan hanya pada model machine learning. Model LSTM dan Isolation Forest digunakan sebagai analytics layer setelah data diproses dengan Spark.

Implementasi big data yang dilakukan:

- **Schema enforcement**
  Dataset CSV dibaca menggunakan schema eksplisit PySpark agar tipe data seperti tanggal, harga, cuaca, dan fitur kalender konsisten.

- **Distributed-style data processing**
  Data diproses menggunakan Spark DataFrame API. Walaupun dijalankan pada mode lokal untuk kebutuhan tugas besar, struktur implementasinya mengikuti pola pemrosesan Spark yang dapat dipindahkan ke cluster.

- **Data integration**
  Dataset akhir menggabungkan tiga jenis data:
  - data harga pangan Disperindag,
  - data cuaca harian,
  - fitur kalender.

- **Agregasi time series**
  Data harga dari 15 kabupaten/kota diagregasi menjadi rata-rata harian tingkat provinsi per komoditas untuk kebutuhan prediksi harga.

- **Window-based feature engineering**
  PySpark Window digunakan untuk membentuk fitur historis seperti perubahan harga harian, deviasi harga terhadap rata-rata provinsi, dan fitur berbasis urutan tanggal.

- **Batch pipeline**
  Pipeline saat ini berjalan secara batch dari CSV. Pada rancangan produksi, pipeline ini dapat dikembangkan menjadi Kafka ingestion dan Spark Structured Streaming.

- **Serving layer**
  Hasil pemrosesan dan model disajikan melalui Flask API dan dashboard web.

Dengan demikian, alur sistemnya adalah:

```text
CSV Harga + Cuaca + Kalender
        ↓
PySpark schema validation, cleaning, aggregation, feature engineering
        ↓
LSTM forecasting + Isolation Forest anomaly detection
        ↓
Flask API
        ↓
Dashboard web
```

## Dataset

File dataset utama:

```text
data_harga_pangan_lampung_model_minimal.csv
```

Karakteristik dataset:

- Total data: 106.807 baris
- Jumlah kolom: 14
- Periode: 4 Mei 2025 sampai 4 Mei 2026
- Wilayah: 15 kabupaten/kota di Provinsi Lampung
- Komoditas: 26 komoditas pangan
- Fitur tambahan: suhu rata-rata, curah hujan, kelembapan, hari dalam minggu, bulan, akhir pekan, libur nasional, jarak ke Idul Fitri, dan jarak ke Natal

## Teknologi yang Digunakan

- Python
- Flask
- PySpark
- TensorFlow/Keras
- scikit-learn
- Pandas
- NumPy
- Chart.js
- HTML, CSS, JavaScript

## Struktur Project

```text
.
├── dashboard/
│   ├── index.html
│   ├── style.css
│   └── app.js
├── dashboard_server.py
├── data_harga_pangan_lampung_model_minimal.csv
├── requirements.txt
├── README.md
└── .gitignore
```

Catatan:

- `cache.json` tidak dimasukkan ke repository karena merupakan hasil training/cache lokal.
- `.venv/` tidak dimasukkan ke repository karena merupakan environment lokal.
- File laporan final HTML juga tidak dimasukkan ke repository sesuai konfigurasi `.gitignore`.

## Cara Menjalankan

1. Buat virtual environment:

```bash
python3 -m venv .venv
```

2. Aktifkan virtual environment:

```bash
source .venv/bin/activate
```

3. Install dependency:

```bash
pip install -r requirements.txt
```

4. Jalankan server:

```bash
python dashboard_server.py
```

5. Buka dashboard:

```text
http://127.0.0.1:5000
```

Pada run pertama, sistem akan membaca dataset, menjalankan pipeline PySpark, melatih model LSTM, menjalankan Isolation Forest, lalu menyimpan hasil ke `cache.json`. Run berikutnya akan lebih cepat karena hasil sudah dibaca dari cache.

## Endpoint API

| Endpoint | Fungsi |
|---|---|
| `/api/dataset-info` | Menampilkan ringkasan dataset |
| `/api/predict/<komoditas>` | Menampilkan hasil prediksi 7 hari untuk komoditas tertentu |
| `/api/anomaly` | Menampilkan hasil deteksi anomali |

## Model Analitik

### LSTM Forecasting

Model LSTM digunakan untuk memprediksi harga rata-rata provinsi selama 7 hari ke depan. Input model berasal dari data hasil preprocessing PySpark, termasuk harga historis, cuaca, dan fitur kalender.

Metrik evaluasi:

- MAE
- RMSE
- MAPE

### Isolation Forest

Isolation Forest digunakan untuk mendeteksi data harga yang menyimpang. Fitur yang digunakan meliputi:

- harga aktual,
- deviasi terhadap rata-rata provinsi,
- perubahan harga harian,
- suhu,
- curah hujan,
- kelembapan,
- fitur akhir pekan/libur,
- jarak ke hari raya.

Karena dataset tidak memiliki label anomali manual, hasil deteksi digunakan sebagai indikasi awal yang perlu ditinjau lebih lanjut.

## Rancangan Pengembangan Produksi

Untuk pengembangan lanjutan, sistem dapat diperluas menjadi arsitektur big data produksi:

- Kafka untuk ingestion data harian dari Disperindag, cuaca, dan kalender.
- Spark Structured Streaming untuk pemrosesan data otomatis.
- Delta Lake/Parquet untuk penyimpanan data historis.
- Database/API service untuk serving layer.
- Dashboard monitoring real-time.

## Catatan

Project ini merupakan prototipe akademik. Fokus utamanya adalah menunjukkan integrasi data multi-sumber, pemrosesan PySpark, feature engineering, prediksi harga, dan deteksi anomali dalam satu sistem dashboard end-to-end.
