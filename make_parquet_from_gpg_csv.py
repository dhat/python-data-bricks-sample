#!/usr/bin/python
#
# First part imports gpg csv, decodes it,
# saves to csv and imports to a spark df
# then saves it to a parquet file
# 
import os
import sys
import shutil
import gnupg
# gnupg.__version__
from pprint import pprint
from pyspark.sql import *
from pyspark import SparkContext
sc = SparkContext()
sqlContext = SQLContext(sc)

# Get gpg keys
#root_dir = os.path.dirname(os.path.realpath("./"))
root_dir = os.getcwd()
program_subdir = "python-data-bricks-sample"
full_program_dir = os.path.join(root_dir, program_subdir)
gpg = gnupg.GPG(gnupghome=full_program_dir)
key_data = open(os.path.join(full_program_dir, 'slim.shady.pub.asc')).read()
import_result = gpg.import_keys(key_data)
key_data = open(os.path.join(full_program_dir, 'slim.shady.sec.asc')).read()
import_result = gpg.import_keys(key_data)
#pprint(import_result.results)

# Get and decode csv
save_csv = os.path.join(full_program_dir, 'out.csv')
with open(os.path.join(full_program_dir, 'titanic.csv.gpg'), 'rb') as f:
    status = gpg.decrypt_file(f, output=save_csv)
if not status.ok:
    print("Failed decode gpg file")
    sys.exit(-1)

# Export to clear csv
df = sqlContext.read.load('file://' + save_csv,
                          format='com.databricks.spark.csv',
                          header='true',
                          inferSchema='true')

# Check if output exists and remove and replace it if it does
titanic_parquet = os.path.join(full_program_dir, "titanic.parquet")
if os.path.exists(titanic_parquet):
    shutil.rmtree(titanic_parquet)
# write to parquet
df.write.parquet(titanic_parquet)

# Second part:
# Now import parquet and set it up
pardata = sqlContext.read.parquet(titanic_parquet)
pardata.registerTempTable("titanic")

# perform lookups
avgage = sqlContext.sql("SELECT Avg(CAST(Age AS Double)), " +
                        " Count(*) FROM titanic")
avgcount = sqlContext.sql("SELECT CAST(Count(Age) AS Double) " +
                          "FROM titanic WHERE Age NOT IN ('NA')")
# need to know how many items to limit in order to get 75 percent
getvals = int(0.75 * avgcount.first()[0])
top75pctavg = sqlContext.sql("SELECT Avg(Age), Count(*) FROM (" +
                             " SELECT CAST(Age AS DOUBLE) " +
                             " From titanic WHERE Age NOT IN (\'NA\') " +
                             " ORDER BY Age DESC LIMIT " + str(getvals) + ")")

# Output answers:
print("Average age is " + str(avgage.first()[0]))
print("Top 75 percent Average age is " + str(top75pctavg.first()[0]))

