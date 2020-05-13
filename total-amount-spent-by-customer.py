# -*- coding: utf-8 -*-
"""
Created on Tue May 12 19:10:13 2020

@author: kocha
"""
from pyspark import SparkConf, SparkContext
from decimal import Decimal

conf = SparkConf().setMaster("local").setAppName("TotalSpendbyCustomer")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    customerID  = int(fields[0])
    orderAmount = Decimal(fields[2])
    return (customerID, orderAmount)
  
lines = sc.textFile("file:///SparkCourse/customer-orders.csv")
parsedLines = lines.map(parseLine)
customerOrderAmounts = parsedLines.map(lambda x: (x[0], x[1]))
totalCustomerSpendRDD = customerOrderAmounts.reduceByKey(lambda x, y: (x + y))
totalCustomerSpendRDDSorted = totalCustomerSpendRDD.map(lambda x: (x[1], x[0])).sortByKey(ascending = False)
results = totalCustomerSpendRDDSorted.map(lambda x: (x[1], x[0])).collect()

for result in results:
    print(str(result[1]) + "  " + str(result[0]))