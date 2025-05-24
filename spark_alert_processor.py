import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType

try:
    from configs import kafka_config
except ImportError:
    print("Помилка: Файл 'configs.py' не знайдено. Будь ласка, створіть його з вашими налаштуваннями Kafka.")
    exit()

servers_config = kafka_config.get('bootstrap_servers')

KAFKA_BOOTSTRAP_SERVERS = None
if isinstance(servers_config, list):
    KAFKA_BOOTSTRAP_SERVERS = ",".join(servers_config)
elif isinstance(servers_config, str):
    KAFKA_BOOTSTRAP_SERVERS = servers_config # Використовуємо рядок як є
else:
    print("Помилка: 'bootstrap_servers' у configs.py має бути рядком або списком.")
    exit()

SECURITY_PROTOCOL = kafka_config.get('security_protocol', 'PLAINTEXT')
SASL_MECHANISM = kafka_config.get('sasl_mechanism')
USERNAME = kafka_config.get('username')
PASSWORD = kafka_config.get('password')

INPUT_KAFKA_TOPIC = "sergeyp_building_sensors"
OUTPUT_KAFKA_TOPIC = "sergeyp_alerts"
ALERTS_CSV_PATH = "alerts_conditions.csv"
CHECKPOINT_LOCATION = "/tmp/spark_checkpoint_alerts_v3"

if not KAFKA_BOOTSTRAP_SERVERS:
    print("Помилка: 'bootstrap_servers' не вказано у 'configs.py'.")
    exit()

kafka_options = {
    "kafka.bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
}

kafka_options["kafka.security.protocol"] = SECURITY_PROTOCOL
kafka_options["kafka.sasl.mechanism"] = SASL_MECHANISM
jaas_config = (
    f"org.apache.kafka.common.security.plain.PlainLoginModule required "
    f"username=\"{USERNAME}\" password=\"{PASSWORD}\";"
)
kafka_options["kafka.sasl.jaas.config"] = jaas_config

spark = (SparkSession.builder
         .appName("SensorAlertsProcessorWithConfigs")
         .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5")
         .getOrCreate()
         )

spark.sparkContext.setLogLevel("WARN")
print("Spark Session створено.")

try:
    alerts_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(ALERTS_CSV_PATH)
    print("Умови алертів успішно завантажено:")
    alerts_df.show(truncate=False)
except Exception as e:
    print(f"Помилка читання CSV файлу ({ALERTS_CSV_PATH}): {e}")
    spark.stop()
    exit()

print(f"Читання даних з Kafka топіку: {INPUT_KAFKA_TOPIC}")
raw_stream = (spark.readStream.format("kafka")
              .option("subscribe", INPUT_KAFKA_TOPIC)
              .option("startingOffsets", "latest")
              .options(**kafka_options).load())

schema = StructType([
    StructField("sensor_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("temperature", FloatType(), True),
    StructField("humidity", FloatType(), True)
])

parsed_stream = raw_stream.select(
    F.from_json(F.col("value").cast("string"), schema).alias("data")
).select("data.*")

parsed_stream = parsed_stream \
    .withColumn("timestamp", F.to_timestamp(F.col("timestamp"))) \
    .withWatermark("timestamp", "10 seconds")

print("Застосування віконної агрегації...")
windowed_agg = parsed_stream \
    .groupBy(
        F.window("timestamp", "1 minute", "30 seconds")
    ) \
    .agg(
        F.avg("temperature").alias("avg_temperature"),
        F.avg("humidity").alias("avg_humidity")
    )

try:
    alerts_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(ALERTS_CSV_PATH)

    print("Умови алертів успішно завантажено (оригінал):")
    alerts_df.show(truncate=False)

    alerts_df = alerts_df \
        .withColumnRenamed("humidity_min", "min_humidity") \
        .withColumnRenamed("humidity_max", "max_humidity") \
        .withColumnRenamed("temperature_min", "min_temp") \
        .withColumnRenamed("temperature_max", "max_temp") \
        .withColumnRenamed("code", "alert_code") \
        .withColumnRenamed(alerts_df.columns[-1], "alert_message")

except Exception as e:
    print(f"Помилка читання CSV файлу ({ALERTS_CSV_PATH}): {e}")
    spark.stop()
    exit()

print("Пошук алертів...")

lit_neg_999 = F.lit(-999)
temp_min_ok = (F.col("avg_temperature") >= F.col("min_temp")) | (F.col("min_temp") == lit_neg_999)
temp_max_ok = (F.col("avg_temperature") <= F.col("max_temp")) | (F.col("max_temp") == lit_neg_999)
hum_min_ok = (F.col("avg_humidity") >= F.col("min_humidity")) | (F.col("min_humidity") == lit_neg_999)
hum_max_ok = (F.col("avg_humidity") <= F.col("max_humidity")) | (F.col("max_humidity") == lit_neg_999)

alert_conditions = temp_min_ok & temp_max_ok & hum_min_ok & hum_max_ok

triggered_alerts = windowed_agg.crossJoin(alerts_df) \
    .filter(alert_conditions) \
    .select(
        F.col("window.start").cast("string").alias("window_start"),
        F.col("window.end").cast("string").alias("window_end"),
        F.round("avg_temperature", 2).alias("avg_temperature"),
        F.round("avg_humidity", 2).alias("avg_humidity"),
        F.col("alert_code"),
        F.col("alert_message").alias("message")
    )

output_stream = triggered_alerts \
    .select(F.to_json(F.struct("*")).alias("value"))

print(f"Запис алертів у Kafka топік: {OUTPUT_KAFKA_TOPIC}")
query = (output_stream.writeStream
         .format("kafka")
         .option("topic", OUTPUT_KAFKA_TOPIC)
         .option("checkpointLocation", CHECKPOINT_LOCATION)
         .options(**kafka_options)
         .start())

print("Потік запущено. Очікування на термінацію (Ctrl+C)...")
query.awaitTermination()