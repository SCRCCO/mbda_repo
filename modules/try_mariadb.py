# -*- coding: UTF-8 -*-

'''
Written on: October 8th, 2019
Author: Roberto PatriziThis program checks if MariaDB Server is correctlyaccessed by PySpark environment.It read the yser table in mysql DB, readingname, host, plugin and password for each user.
'''

programName = 'try_mariadb'

import time

print('\n#------------------------------------------------------------#'
		, '\n    program ', programName, ' starts at '
        , time.strftime('%Y-%m-%d %H:%M:%S')      
        , '\n#------------------------------------------------------------#')

#---------------------------------------------#                 
#           database parameters               #
#---------------------------------------------#
dbName = 'mysql'
dbHost = 'localhost'
dbPort = '3306'
dbUrl = 'jdbc:mysql://' + dbHost + ':' + dbPort + '/' + dbName
dbUser = 'studentbda'

# set this variable with your own password rather then 'xxxxxxxxxx'
dbPassword = 'xxxxxxxxxx'

# import pyspark packages
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame

# create a local Spark session
spark = SparkSession \
    .builder \
    .appName(programName) \
    .getOrCreate()

print('\nspatk version = ' + spark.version, '\n')

# read data from mariadb table mysql.user
jdbcDF = spark \
	.read \
	.format("jdbc") \
	.option('url', dbUrl) \
	.option("dbtable", 'user') \
	.option("user", dbUser) \
	.option("password", dbPassword ) \
	.load()

# select several columns from jdbc dataframe
jdbcDF \
	.select('user'
		, 'host'
		, 'plugin'
		, 'password') \
	.show()

# stop spark core
spark.stop()

print('#------------------------------------------------------------#'
		, '\n    program ', programName, ' ends at '
        , time.strftime('%Y-%m-%d %H:%M:%S')      
        , '\n#------------------------------------------------------------#')
