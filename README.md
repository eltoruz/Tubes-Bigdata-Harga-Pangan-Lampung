# Sistem Analitik Big Data Monitoring Harga Pangan Lampung

Project ini merupakan tugas besar Mahadata/Big Data untuk membangun prototipe sistem monitoring, prediksi harga, dan deteksi anomali harga komoditas pangan di Provinsi Lampung. Sistem menggunakan data harga pangan harian dari 15 kabupaten/kota, diperkaya dengan data cuaca dan fitur kalender, kemudian diproses melalui pipeline PySpark sebelum masuk ke model analitik.

## Daftar Isi

- [Anggota Kelompok](#anggota-kelompok)
- [Ringkasan Project](#ringkasan-project)
- [Arsitektur Big Data](#arsitektur-big-data)
- [Implementasi Big Data](#implementasi-big-data)
- [Dataset](#dataset)
- [Teknologi yang Digunakan](#teknologi-yang-digunakan)
- [Struktur Project](#struktur-project)
- [Cara Menjalankan](#cara-menjalankan)
- [Endpoint API](#endpoint-api)
- [Model Analitik](#model-analitik)
- [Rancangan Pengembangan Produksi](#rancangan-pengembangan-produksi)

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

Sistem ini dibuat untuk membantu pemantauan harga pangan tingkat provinsi melalui dashboard web. Dashboard menampilkan ringkasan dataset, grafik prediksi harga, metrik evaluasi model, jumlah anomali, dan tabel anomali teratas.

Modul utama:

| Modul | Fungsi | Metode |
|---|---|---|
| Prediksi harga | Memprediksi harga rata-rata provinsi 7 hari ke depan | LSTM multivariate |
| Deteksi anomali | Menandai harga yang menyimpang dari pola umum | Isolation Forest |
| Dashboard monitoring | Menyajikan hasil analitik secara interaktif | Flask + Chart.js |

## Arsitektur Big Data

Project ini mengacu pada **Lambda Architecture**, yaitu pola arsitektur big data yang memisahkan pemrosesan data historis, pemrosesan data baru/real-time, dan penyajian hasil analitik.

Penerapan Lambda Architecture pada project ini:

| Layer | Implementasi Prototipe | Rancangan Produksi |
|---|---|---|
| Data Sources | CSV harga pangan, cuaca, kalender | Data harian Disperindag, API cuaca, kalender nasional |
| Ingestion Layer | Pembacaan CSV lokal | Kafka topic untuk harga, cuaca, kalender, dan alert |
| Batch Layer | PySpark batch processing | Spark batch job terjadwal |
| Speed Layer | Belum real-time penuh | Spark Structured Streaming |
| Analytics Layer | LSTM dan Isolation Forest | Training/serving model terjadwal |
| Serving Layer | Flask API dan dashboard web | API service, database/cache, dashboard monitoring |

Alur arsitektur prototipe:

```text
Data harga + cuaca + kalender
        ↓
PySpark batch processing
        ↓
Schema validation, aggregation, feature engineering
        ↓
LSTM forecasting + Isolation Forest anomaly detection
        ↓
Flask API
        ↓
Dashboard monitoring
```

Dengan pendekatan ini, sistem tetap dapat dijalankan sebagai prototipe lokal, tetapi rancangan arsitekturnya sudah mengarah ke pipeline big data produksi menggunakan Kafka dan Spark.

## Implementasi Big Data

Fokus big data pada project ini berada pada **pipeline pemrosesan data berbasis PySpark**, bukan hanya pada model machine learning. Model LSTM dan Isolation Forest digunakan sebagai analytics layer setelah data diproses dengan Spark.

Implementasi big data yang dilakukan:

| Aspek | Implementasi |
|---|---|
| Schema enforcement | Dataset CSV dibaca menggunakan schema eksplisit PySpark agar tipe data konsisten |
| Distributed-style processing | Data diproses dengan Spark DataFrame API dan dapat dipindahkan ke cluster |
| Data integration | Data harga, cuaca, dan kalender digabung dalam satu dataset analitik |
| Agregasi time series | Harga 15 kabupaten/kota diagregasi menjadi rata-rata provinsi per komoditas |
| Window-based feature engineering | PySpark Window digunakan untuk perubahan harga, deviasi provinsi, dan fitur berbasis waktu |
| Batch pipeline | Pipeline berjalan dari CSV secara batch pada prototipe |
| Serving layer | Hasil pemrosesan disajikan melalui Flask API dan dashboard web |

Karakteristik 5V big data:

| 5V | Implementasi pada Project |
|---|---|
| Volume | 106.807 baris data harga pangan |
| Variety | Data harga pangan, cuaca, dan kalender |
| Velocity | Batch harian pada prototipe, Kafka streaming pada rancangan produksi |
| Veracity | Validasi schema, pengecekan null, dan deteksi anomali |
| Value | Prediksi harga dan indikasi anomali untuk monitoring pangan |

## Dataset

File dataset utama:

```text
data_harga_pangan_lampung_model_minimal.csv
```

Karakteristik dataset:

| Karakteristik | Nilai |
|---|---|
| Total data | 106.807 baris |
| Jumlah kolom | 14 |
| Periode | 4 Mei 2025 sampai 4 Mei 2026 |
| Wilayah | 15 kabupaten/kota di Provinsi Lampung |
| Komoditas | 26 komoditas pangan |
| Fitur tambahan | Cuaca dan kalender |

Kolom dataset:

| Kolom | Keterangan |
|---|---|
| `tanggal` | Tanggal pencatatan harga |
| `kabupaten_kota` | Nama kabupaten/kota |
| `kabkot_id` | Kode wilayah |
| `komoditas` | Nama komoditas pangan |
| `harga_rupiah` | Harga komoditas dalam rupiah |
| `suhu_rata2_c` | Suhu rata-rata harian |
| `curah_hujan_mm` | Curah hujan harian |
| `kelembapan_rata2_pct` | Kelembapan rata-rata |
| `hari_ke_minggu` | Indeks hari dalam minggu |
| `bulan` | Bulan pencatatan |
| `is_akhir_pekan` | Penanda akhir pekan |
| `is_libur_nasional` | Penanda libur nasional |
| `jarak_ke_idul_fitri_hari` | Jarak tanggal terhadap Idul Fitri |
| `jarak_ke_natal_hari` | Jarak tanggal terhadap Natal |

## Teknologi yang Digunakan

| Teknologi | Peran |
|---|---|
| Python | Bahasa utama backend dan pipeline |
| PySpark | Big data processing, agregasi, dan feature engineering |
| Flask | API dan web server |
| TensorFlow/Keras | Model LSTM untuk prediksi harga |
| scikit-learn | Isolation Forest untuk deteksi anomali |
| Pandas/NumPy | Konversi dan manipulasi data untuk model |
| Chart.js | Visualisasi grafik dashboard |
| HTML/CSS/JavaScript | Antarmuka dashboard |

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
- File laporan final HTML tidak dimasukkan ke repository sesuai konfigurasi `.gitignore`.

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

Untuk memaksa training ulang:

```bash
python dashboard_server.py --force
```

## Endpoint API

| Endpoint | Fungsi |
|---|---|
| `/api/dataset-info` | Menampilkan ringkasan dataset |
| `/api/predict/<komoditas>` | Menampilkan hasil prediksi 7 hari untuk komoditas tertentu |
| `/api/anomaly` | Menampilkan hasil deteksi anomali |

## Model Analitik

### LSTM Forecasting

Model LSTM digunakan untuk memprediksi harga rata-rata provinsi selama 7 hari ke depan. Input model berasal dari data hasil preprocessing PySpark, termasuk harga historis, cuaca, dan fitur kalender.

Alur prediksi:

```text
Data harian per komoditas
        ↓
Agregasi rata-rata provinsi dengan PySpark
        ↓
Normalisasi fitur
        ↓
Window time series
        ↓
LSTM multivariate
        ↓
Prediksi harga H+1 sampai H+7
```

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

Alur deteksi anomali:

```text
Data harga kabupaten/kota
        ↓
Hitung rata-rata provinsi dan perubahan harian dengan PySpark
        ↓
Bentuk fitur anomali
        ↓
Standardisasi fitur
        ↓
Isolation Forest
        ↓
Daftar anomali teratas dan grafik jumlah anomali per hari
```

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
