"""
Intrusion Detection System
"""

# Import all relevant Spark libraries
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql import Row
from pyspark.sql.functions import desc
from termcolor import colored

# Instantiate spark context
sc = pyspark.SparkContext(appName="sparkSQL")
ss = SparkSession(sc)
sqlContext = SQLContext(sc)

sqlContext.setConf("spark.sql.retainGroupColumns", "true")

# Read input data set
data = "/user/ajinkyapadwad/spark-2.2.0/ece579_f17/recitation4/problems/kddcup.data_10_percent_corrected"
raw = sc.textFile(data).cache()

# Extract into CSV format
csv_data = raw.map(lambda l: l.split(","))

# Create Rows
row_data = csv_data.map(lambda p: Row(
	duration 		=	int(p[0]), 
	protocol_type	=	p[1],
	service			=	p[2],
	flag			=	p[3],
	src_bytes		=	int(p[4]),
	dst_bytes		=	int(p[5])
	)
)

# Create DataFrame of the row data
mainTable = sqlContext.createDataFrame(row_data)

# Create a table from the DataFrame
mainTable.registerTempTable("KDDdata")

# Select network interactions with more than 2 second duration and no transfer from destination
intrusionsTCP  = sqlContext.sql("SELECT duration, dst_bytes FROM KDDdata WHERE protocol_type = 'tcp'  AND duration > 2000 AND dst_bytes = 0")
intrusionsUDP  = sqlContext.sql("SELECT duration, dst_bytes FROM KDDdata WHERE protocol_type = 'udp'  AND duration > 2000 AND dst_bytes = 0")
intrusionsICMP = sqlContext.sql("SELECT duration, dst_bytes FROM KDDdata WHERE protocol_type = 'icmp' AND duration > 2000 AND dst_bytes = 0")
intrusionsAll  = sqlContext.sql("SELECT duration, protocol_type FROM KDDdata WHERE ( duration > 2000 AND dst_bytes = 0 ) ")

totalTCP  = intrusionsTCP.count()
totalUDP  = intrusionsUDP.count()
totalICMP = intrusionsICMP.count()
totalAll  = intrusionsAll.count()

# allIntetion > 2000 AND dst_bytes = 0").groupby('protocol_type').count().sort(desc('count')).show(15)

print ""
print colored("TCP  INTRUSIONS : %d" %  totalTCP,  "cyan")
print colored("UDP INTRUSIONS : %d" %  totalUDP, "cyan")
print colored("ICMP  INTRUSIONS : %d" %  totalICMP,  "cyan")
print colored("ALL  INTRUSIONS : %d" %  totalAll,  "cyan")
print ""

# mainTable.select("protocol_type", "duration", "dst_bytes").filter(mainTable.duration>2000).filter(mainTable.dst_bytes == 0).groupby('protocol_type').count().show(3)
intrusionsAll.show(10)

# Then group the filtered elements by protocol_type and show the total count in each group.
# Refer - https://spark.apache.org/docs/latest/sql-programming-guide.html#dataframegroupby-retains-grouping-columns

# kdd_df.select("protocol_type", "duration", "dst_bytes").filter(kdd_df.duration>2000)#.more query...

# def transform_label(label):
# 	'''
# 	Create a function to parse input label
# 	such that if input label is not normal 
# 	then it is an attack
# 	'''
	


# row_labeled_data = csv_data.map(lambda p: Row(
# 	duration=int(p[0]), 
# 	protocol_type=p[1],
# 	service=p[2],
# 	flag=p[3],
# 	src_bytes=int(p[4]),
# 	dst_bytes=int(p[5]),
# 	label=transform_label(p[41])
# 	)
# )
# kdd_labeled = sqlContext.createDataFrame(row_labeled_data)

# '''
# Write a query to select label, 
# group it and then count total elements
# in that group
# '''
# # query

# kdd_labeled.select("label", "protocol_type", "dst_bytes").groupBy("label", "protocol_type", kdd_labeled.dst_bytes==0).count().show()