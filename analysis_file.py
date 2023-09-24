#!/usr/bin/env python
# coding: utf-8

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from argparse import ArgumentParser

spark = SparkSession.builder.appName("onepiece").getOrCreate()

"""
This Python/Pyspark script takes the input file, cleans the dataset and outputs 2 datasets:
1. Actual data in partitioned format
2. Yearwise summary
3. Overall summary
"""
parser = ArgumentParser()
parser.add_argument("--input_file", type=str, help= "Input file to be processed")
parser.add_argument("--output_folder", type=str, help= "Folder where the output files to be processed")
args = parser.parse_args()

input_file = args.input_file
output_folder = args.output_folder

# input_file = './onepieceanime/one_piece.csv'
# output_folder = "./outputs/"


df = spark.read.csv(input_file,header=True, inferSchema=True)

print("Episodes Count:", df.count())
df.printSchema()

# We observe that the columns like rank, trend, total_votes are imported as string
# We need to convert them to integer columns

# Cleaning the 3 columns to int
for colname in ["rank", "trend", "total_votes"]:
    df = df.withColumn(colname, F.regexp_replace(F.col(colname), ",", ""))
    df = df.withColumn(colname, df[colname].cast("int"))


df.filter(F.col("total_votes").isNull()).show()

# df.filter(F.col("rank").isNull()).show()
# df.filter(F.col("trend").isNull()).show()
# df.filter(F.col("trend").isNull()).count()


# Yearwise summary
df.createOrReplaceTempView("df_sql")
yr_wise_query = """
select 
    start,
    count(episode) as no_episodes,
    sum(total_votes) as total_votes,
    sum(total_votes) / count(episode) as avg_votes_per_episode,
    avg(average_rating) as avg_rating
from df_sql
group by start
"""
spark.sql(yr_wise_query).show(10,False)


year_wise_output = output_folder + "year_wise_summary"
spark.sql(yr_wise_query).write.mode("overwrite").parquet(year_wise_output)

# Overall summary
overall_summary = output_folder + "overall_summary"
overall_summary_qry = """
select 
    count(episode) as no_episodes,
    sum(total_votes) as total_votes,
    sum(total_votes) / count(episode) as avg_votes_per_episode,
    avg(average_rating) as avg_rating
from df_sql
"""

spark.sql(overall_summary_qry).write.mode("overwrite").parquet(overall_summary)


# Actual data - partitioned by year
raw_partitioned_file = output_folder + "raw_data"
df.write.partitionBy("start").mode("overwrite").parquet(raw_partitioned_file)

spark.stop()