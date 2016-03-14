#~/spark/spark-1.6.1/bin/pyspark --packages com.databricks:spark-csv_2.10:1.4.0
clickstreamRaw = sqlContext.read \
    .format("com.databricks.spark.csv") \
    .options(header="true", delimiter="\t", mode="DROPMALFORMED", inferSchema="true") \
    .load("data/2015_02_clickstream.tsv")
  
# Converts the file to Parquet, an efficient data storage format. 
clickstreamRaw.write \
    .mode("overwrite") \
    .format("parquet") \
    .save("data/wiki-clickstream")
  
clicks = sqlContext.read.parquet("data/wiki-clickstream").cache()

# Calculate the number of clicks versus the number of Wikipedia clicks
all_clicks = clicks.selectExpr("sum(n) AS clicks").first().clicks
wiki_clicks = clicks.where("prev_id IS NOT NULL").selectExpr("sum(n) AS clicks").first().clicks
float(wiki_clicks) / all_clicks * 100

# Make clicks available as a SQL table.
clicks.registerTempTable("clicks")

spring_clicks = clicks.where("curr_title IN('Spring')")
donald_trump_clicks = clicks.where("curr_title IN('Donald_Trump')")

donald_trump_clicks.registerTempTable("donaldtrump")
sqlContext.sql("Select * from donaldtrump LIMIT 10").show()

sqlContext.sql(
"""
    SELECT *
    FROM clicks
    WHERE 
      curr_title = 'Donald_Trump' AND
      prev_id IS NOT NULL AND prev_title != 'Main_Page'
    ORDER BY n DESC
    LIMIT 20
"""
).show()

sqlContext.sql(
"""
    SELECT *
    FROM clicks
    WHERE 
      curr_title = 'Fifty_Shades_of_Grey' AND
      prev_id IS NOT NULL AND prev_title != 'Main_Page'
    ORDER BY n DESC
    LIMIT 20
"""
).show()

#Create a Property Graph
uniqueArticlesDF = clicks.select("curr_id", "curr_title").distinct()

#How many articles in total?
uniqueArticlesDF.count()

for x in uniqueArticlesDF.rdd.take(3):
  print("0 -> "+str(x[0])+", 1 -> "+str(x[1]))
  
articlesRDD = uniqueArticlesDF.rdd\
    .map( lambda row: (row[0], row[1]))

clickstreamWithoutNullsDF = clicks.na.drop()
clickstreamWithoutNulls_3cols_DF = clickstreamWithoutNullsDF \
    .select("prev_id", "curr_id", "n").distinct()


edgesRDD = clickstreamWithoutNulls_3cols_DF.rdd\
    .map(lambda row: \
    #Map to the edge tuple type
    Edge(row[0], row[1], row[2])\
)

#The edgesRDD contains (referrer, resource, weight) tuples:









