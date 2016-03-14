//Print the Spark Context
sc

//Create an RDD from a text file
val rdd = sc.textFile("intro/data/boa-constrictor")
val silverstein = rdd.collect

//Apply a transformation on an RDD
val BoaConstrictor = rdd.filter(line => line.contains("Boa"))

//Collect is an action
BoaConstrictor.collect

/*
  Word count splits each line by space
  Maps each word into a tuple of word and the frequency per word, which is 
  one while emitted. Then reduces by key, in this case word, and adds the values by key
  (x & y are values)
*/

val words = rdd.flatMap(line => line.split(" "))
val counts = words.map(word => (word,1)).reduceByKey{case (x,y) => x+y}
counts.collect

val Boa = counts.filter(pair => pair._1.equals("Boa"))
Boa.collect

//Perform the same analysis using Spark SQL
val df = sqlContext.read.text("intro/data/boa-constrictor")

//We can now query against this table
df.registerTempTable("df")
sqlContext.sql("select * from df where value like '%Boa%'").show()

//Show the entire line
sqlContext.sql("select * from df where value like '%Boa%'").take(10)

//Create the counts RDD as a dataframe  
val words = df.flatMap(row => row.toString.split(" "))
    .map(word => (word,1))
words.registerTempTable("words")

//Write SQL queries directly against this table
sqlContext.sql("""
    SELECT _1, count(*)
        FROM words 
        WHERE _1 = 'Boa'
        GROUP BY _1 
        ORDER BY count(*) DESC
""").show()

sqlContext.sql("""
    SELECT _1, sum(_2)
        FROM words 
        WHERE _1 = 'Boa'
        GROUP BY _1 
        ORDER BY count(_2) DESC
""").show()


