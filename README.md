# Обзор
Работа по представлению финансовых данных в виде японских свечей используя библиотеку PySpark и технологию MapReduce.

# Как запускать из гугл-коллаба?
1) Загрузите дата-сет в директорию гугл-коллаба
2) Опционально загрузите XML файл для конфига
3) В верхней панели нажмите "Выполнить все" (или можете выполнять секции поочередно -- так даже нагляднее будет из-за постоянных df.show())
4) Далее программа сначала попросит Вас вписать название дата-сета. Если просто нажать ENTER, то по умолчанию выберется название "fin_sample.csv", которого в директории по умолчанию нет
5) Потом попросят ввести название XML файла для конфига. Если просто нажать ENTER, то по умолчанию выберутся стандартные настройки свечей
6) Немного подождав, в директории коллаба появится директория "AnswerSparkTask", где и будут выходные CSV файлы в виде японских свечей

# Как оно работает?
```python3
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as F
import os
import shutil
import xml
```
Импортируются необходимые библиотеки PySpark (SparkSession, types, functions) и стандартные библиотеки Python (os, shutil, xml). PySpark используется для работы с распределенными данными, а остальные библиотеки - для файловых операций и парсинга XML. Обратите внимания на псевдонимы библиотек, дальше нам это понадобится.

```python3
spark = SparkSession \
        .builder \
        .appName("SparkTask") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()
input_csv = "fin_sample.csv"
path = input("Enter PATH to CSV file or just press ENTER for std df: ")
if (path):
    input_csv = path
```
Создается сессия Spark. Строка .config(...) нужна для совместимости с более старыми форматами времени при парсинге (преобразование форматных строк в timestamp, например). Пользователю предлагается ввести путь к CSV файлу с данными, или используется файл по умолчанию (fin_sample.csv).

```python3
schema = StructType([
    StructField("#SYMBOL", StringType(), True),
    StructField("SYSTEM", StringType(), True),
    StructField("MOMENT", StringType(), True),
    StructField("ID_DEAL", LongType(), True),
    StructField("PRICE_DEAL", DoubleType(), True),
    StructField("VOLUME", LongType(), True),
    StructField("OPEN_POS", LongType(), True),
    StructField("DIRECTION", StringType(), True),
])
df = spark.read.csv(input_csv, header=True, schema=schema)
```
Определяется схема данных для CSV файла, и данные считываются в датафрейм (df). header=True указывает, что первая строка файла содержит заголовки. На самом деле можно определять схему и автоматически, но я на всякий случай все перепроверил.

```python3
options = {}
options["candle.width"] = 300000
options["candle.date.from"] = 19000101
options["candle.date.to"] = 20200101
options["candle.time.from"] = 1000
options["candle.time.to"] = 1800
xml_file_path = input(
    "Enter PATH to XML config file or just press ENTER for std options: "
)
if (xml_file_path):
    tree = xml.etree.ElementTree.parse(xml_file_path)
    root = tree.getroot()
    for prop in root.findall('property'):
        name = prop.find('name').text
        value = prop.find('value').text
        options[name] = value
```
Определяются стандартные параметры для построения свечей. Пользователю предлагается ввести путь к XML файлу с конфигурацией, который может переопределить стандартные параметры. XML файл парсится, и значения из него записываются в словарь options.

```python3
df = df.withColumn("MOMENT", F.to_timestamp("MOMENT", "yyyyMMddHHmmssSSS"))
df = df.filter(
    (F.hour("MOMENT") * 100 + F.minute("MOMENT") >= options["candle.time.from"]) &
    (F.hour("MOMENT") * 100 + F.minute("MOMENT") <= options["candle.time.to"])
)
```
Столбец MOMENT преобразуется в формат TIMESTAMP для корректной работы с временными окнами. Затем df фильтруется по времени, чтобы оставить только сделки, попадающие в заданный временной диапазон дня (options["candle.time.from"] и options["candle.time.to"]). Это можно рассматривать как один из этапов Map.


```python3
window_spec = F.window("MOMENT", f"{options['candle.width']} milliseconds")
candlestick_df = df.groupBy("#SYMBOL", window_spec).agg(
    F.first("PRICE_DEAL").alias("OPEN"),
    F.max("PRICE_DEAL").alias("HIGH"),
    F.min("PRICE_DEAL").alias("LOW"),
    F.last("PRICE_DEAL").alias("CLOSE"),
    F.sum("VOLUME").alias("Volume")
)
candlestick_df = candlestick_df.select(
    F.col("#SYMBOL"),
    F.col("window.start").alias("MOMENT"),
    "OPEN",
    "HIGH",
    "LOW",
    "CLOSE"
)
candlestick_df = candlestick_df.orderBy("#SYMBOL", "MOMENT")
candlestick_df = candlestick_df.withColumn(
    "MOMENT",
    F.date_format("MOMENT", "yyyyMMddHHmmssSSS")
)
candlestick_df = candlestick_df.withColumn("OPEN", F.round(F.col("OPEN"), 1))
candlestick_df = candlestick_df.withColumn("HIGH", F.round(F.col("HIGH"), 1))
candlestick_df = candlestick_df.withColumn("LOW", F.round(F.col("LOW"), 1))
candlestick_df = candlestick_df.withColumn("CLOSE", F.round(F.col("CLOSE"), 1))
```
Это основная часть, где происходит построение свечей.

window_spec: Определяется временное окно, по которому будут группироваться сделки для каждой свечи. Ширина окна берется из options["candle.width"].

candlestick_df = df.groupBy(...).agg(...): Это ключевой шаг, где применяется группировка (groupBy) по символу акции (#SYMBOL) и временному окну (window_spec). Для каждой группы (т.е. для каждого символа и каждого временного окна) выполняются агрегации (agg):

F.first("PRICE_DEAL").alias("OPEN"): Берется цена первой сделки в окне как цена открытия свечи.
F.max("PRICE_DEAL").alias("HIGH"): Находится максимальная цена сделки в окне как максимум свечи.
F.min("PRICE_DEAL").alias("LOW"): Находится минимальная цена сделки в окне как минимум свечи.
F.last("PRICE_DEAL").alias("CLOSE"): Берется цена последней сделки в окне как цена закрытия свечи.
F.sum("VOLUME").alias("Volume"): Суммируется объем всех сделок в окне как объем свечи.
Далее выбираются нужные столбцы, время свечи форматируется обратно в строку, и цены округляются до одного знака после запятой.

Тут .groupBy() означает этап Shuffle, мы можем различные группы раскидать по кластерам. А .agg() считается этапом Reduce — сворачиваем временное окно в коллекцию скалярных значений (OPEN, HIGH, LOW, CLOSE).

```python3
unique_symbols = candlestick_df \
                 .select("#SYMBOL") \
                 .distinct() \
                 .rdd \
                 .flatMap(lambda x: x) \
                 .collect()
output_dir = "AnswerSparkTask"
if os.path.exists(output_dir):
    shutil.rmtree(output_dir)
os.makedirs(output_dir)
for symbol in unique_symbols:
    symbol_df = candlestick_df.filter(F.col("#SYMBOL") == symbol)
    output_path = os.path.join(output_dir, f"{symbol}.csv")
    symbol_df.write.csv(output_path, header=True, mode="overwrite")
    print(f"Saved candlestick data for {symbol} to {output_path}")
```
Результаты записываются в отдельные CSV файлы для каждого уникального символа.
Получается список уникальных символов. Создается выходная директория, если она не существует, или очищается, если уже есть.
В цикле для каждого символа фильтруется df, оставляя только данные по этому символу, и затем эти данные записываются в отдельный CSV файл в созданной директории.
Также записываются логи (какая-то отладочная информация).

```python3
spark.stop()
print("Program has finished correctly")
```
Сессия Spark останавливается.

# Credits
Работу выполнил: Эркин Кадыров

Группа: ЛТП ИИТ

Дата: 09.08.2025
