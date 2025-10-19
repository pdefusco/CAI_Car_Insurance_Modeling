#****************************************************************************
# (C) Cloudera, Inc. 2020-2023
#  All rights reserved.
#
#  Applicable Open Source License: GNU Affero General Public License v3.0
#
#  NOTE: Cloudera open source products are modular software products
#  made up of hundreds of individual components, each of which was
#  individually copyrighted.  Each Cloudera open source product is a
#  collective work under U.S. Copyright Law. Your license to use the
#  collective work is as provided in your written agreement with
#  Cloudera.  Used apart from the collective work, this file is
#  licensed for your use pursuant to the open source license
#  identified above.
#
#  This code is provided to you pursuant a written agreement with
#  (i) Cloudera, Inc. or (ii) a third-party authorized to distribute
#  this code. If you do not have a written agreement with Cloudera nor
#  with an authorized and properly licensed third party, you do not
#  have any rights to access nor to use this code.
#
#  Absent a written agreement with Cloudera, Inc. (“Cloudera”) to the
#  contrary, A) CLOUDERA PROVIDES THIS CODE TO YOU WITHOUT WARRANTIES OF ANY
#  KIND; (B) CLOUDERA DISCLAIMS ANY AND ALL EXPRESS AND IMPLIED
#  WARRANTIES WITH RESPECT TO THIS CODE, INCLUDING BUT NOT LIMITED TO
#  IMPLIED WARRANTIES OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY AND
#  FITNESS FOR A PARTICULAR PURPOSE; (C) CLOUDERA IS NOT LIABLE TO YOU,
#  AND WILL NOT DEFEND, INDEMNIFY, NOR HOLD YOU HARMLESS FOR ANY CLAIMS
#  ARISING FROM OR RELATED TO THE CODE; AND (D)WITH RESPECT TO YOUR EXERCISE
#  OF ANY RIGHTS GRANTED TO YOU FOR THE CODE, CLOUDERA IS NOT LIABLE FOR ANY
#  DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, PUNITIVE OR
#  CONSEQUENTIAL DAMAGES INCLUDING, BUT NOT LIMITED TO, DAMAGES
#  RELATED TO LOST REVENUE, LOST PROFITS, LOSS OF INCOME, LOSS OF
#  BUSINESS ADVANTAGE OR UNAVAILABILITY, OR LOSS OR CORRUPTION OF
#  DATA.
#
# #  Author(s): Paul de Fusco
#***************************************************************************/

from pyspark.sql import SparkSession
from dbldatagen import DataGenerator, FakerTextProvider
import random

spark = SparkSession.builder \
    .appName("LasVegasAccidentsSyntheticData") \
    .getOrCreate()

row_count = 10000

# High-risk intersection centers (lat, lon)
hotspots = [
    (36.1256, -115.2440),  # Rainbow & Flamingo
    (36.1446, -115.2077),  # Sahara & Decatur
    (36.1585, -115.2065),  # Charleston & Decatur
    (36.1025, -115.1676),  # Tropicana & Koval
    (36.1458, -115.1372),  # Sahara & Maryland Pkwy
    (36.1230, -115.0635),  # Boulder Hwy & Nellis & Flamingo
]

# For weighted geospatial points
def get_skewed_latitude():
    base = random.choices(hotspots, weights=[5,5,5,3,3,4])[0][0]
    return round(random.gauss(base, 0.002), 6)

def get_skewed_longitude():
    base = random.choices(hotspots, weights=[5,5,5,3,3,4])[0][1]
    return round(random.gauss(base, 0.002), 6)

# Gaussian financial cost generators
def cost_generator(at_fault):
    if at_fault == 1:
        return round(max(100, random.gauss(1500, 500)), 2)  # Lower avg cost
    else:
        return round(max(500, random.gauss(5000, 1500)), 2)  # Higher avg cost

# Register UDFs for Spark
from pyspark.sql.functions import udf, col
from pyspark.sql.types import DoubleType, FloatType

spark.udf.register("get_lat", get_skewed_latitude, DoubleType())
spark.udf.register("get_lon", get_skewed_longitude, DoubleType())

# Step 1: Define the data generator
df_spec = DataGenerator(spark, rows=row_count, partitions=4) \
    .withIdOutput() \
    .withColumn("age", "integer", minValue=18, maxValue=80, random=True) \
    .withColumn("gender", "string", values=['Male', 'Female', 'Other'], random=True) \
    .withColumn("license_status", "string", values=["Valid", "Suspended", "Expired"], random=True) \
    .withColumn("vehicle_make", "string", values=["Fauxda", "Chevrolux", "Yotota", "Bord", "Mazdai"], random=True) \
    .withColumn("vehicle_model", "string", values=["Z-Force", "Cruizer", "Nimbus", "Ravix", "Tronx"], random=True) \
    .withColumn("vehicle_year", "integer", minValue=1995, maxValue=2023, random=True) \
    .withColumn("at_fault", "integer", values=[0, 1], weights=[0.4, 0.6], random=True) \
    .withColumn("accident_type", "string",
                values=["Head-on collision",
                        "T-bone (side-impact) collision",
                        "Rear-end collision",
                        "Sideswipe collision",
                        "Odd-angle collision"],
                random=True) \
    .withColumn("coverage_type", "string",
                values=["Liability", "Collision", "Comprehensive", "Uninsured Motorist"],
                random=True) \
    .withColumn("latitude", "double", expr="get_lat()") \
    .withColumn("longitude", "double", expr="get_lon()")

# Step 2: Generate initial DataFrame
df = df_spec.build()

# Step 3: Add financial cost column based on 'at_fault'
from pyspark.sql.functions import pandas_udf, IntegerType
import pandas as pd

@pandas_udf("float")
def generate_cost_udf(at_fault_series: pd.Series) -> pd.Series:
    return at_fault_series.apply(cost_generator)

df = df.withColumn("cost", generate_cost_udf(col("at_fault")))

# Show a few rows
df.show(10, truncate=False)
