# -*- coding: UTF-8 -*-

programName = 'try rdd'
import time
import re


from pyspark. sql import SparkSession
spark = SparkSession.builder.appName(programName).getOrCreate()

file_location = file_location = '/home/studentbda/nyProject/Logs/'
log_file = 'spark_out.log'
error_msg = 'error_spark _out.log'
log_rdd = spark.sparkContext.textFile(file_location+log_file)
only_errors_rdd = log_rdd.filter(lambda line:"ERROR" in line)
print ('\nfollowing lines are error caught in the log file')
only_errors_rdd.foreach(lambda line: print(line))
print ('\nonly_errors_rad type = ', type(only_errors_rdd))
print ('\nonly_errors_rdd = ', only_errors_rdd)
error_number = only_errors_rdd.count() #Action TYPE
print ('\nerror number type = ', type (error_number))
print ('\nerror_number = ', error_number)
error_size = only_errors_rdd.map(lambda s: len(s))
print ('\nerror size type = ', type (error_size))
print ('\nerror size = ', error_size)
total_error_size = error_size.reduce(lambda a,b : a+b)
print ('Intotal error size type = ', type(total_error_size))
print ('\ntotal_error size = ',total_error_size)
wordcounts = only_errors_rdd\
    .filter (lambda line: len (line) > 0)\
    .flatMap (lambda line: re.split('\w+', line))\
    .filter (lambda word: len (word) > 0)\
    .map (lambda word: (word. lower () ,1))\
    .reduceByKey (lambda v1, v2: v1 + sv2)\
    .map(lambda x: (x[1], x[0])) \
    .sortByKey(ascending=False)\
    .persist() #Action TYPE
print ('Inwordcounts in error messages = ', wordcounts)