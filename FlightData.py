# -*- coding: utf-8 -*-
"""
Created on Sun May  3 15:28:18 2020

@author: kocha
"""

import pyspark
from pyspark import SparkConf
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
SparkConf().getAll()

flightData2015 = spark.read.option("inferSchema", "true").option("header", "true").csv("file:///C:/spark/data/flight-data/csv/2015-summary.csv")

print(flightData2015.take(3))