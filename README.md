# Analyzing Wikipedia Event Streams using Spark 2.0 and GraphX
Data sources, installation instructions, and demonstration of Analyzing Wikipedia Event Streams using Spark 2.0 and GraphX.

###Clone GitHub Repository
	git clone https://github.com/mydpy/rva-data-hackers-spark-demo
If you do not have git, you can download the repository as a .zip file from the right-hand navigation panel. 

###Download Wikipedia Clickstream Data
[Read about Clickstreams](https://en.wikipedia.org/wiki/Clickstream) if you are unfamiliar with them. 
[Download](https://ndownloader.figshare.com/files/3289898) the February 2015 Englush Clickstream data from the [Wikimedia Foundation](https://datahub.io/dataset/wikipedia-clickstream). 

Unzip the file (optional) and place into the data folder in the project. 

    gunzip 2015_02_clickstream.tsv.gz  #Note - this is optional 
    mv 2015_02_clickstream.tsv spark-wiki-cs/data

###Installing Spark
####[Option 1] Install Stable Spark Release from Source or Precompiled Binaries

* Visit the [Apache Spark Homepage](http://spark.apache.org/downloads.html)
* Choose a Spark release: 1.6.1 (Mar 09 2016)
* Choose a package type: Source Code [can build with several Hadoop versions]. Choose a precompiled version if you do not want to compile the binary files.
* Choose a download type: Select Apache Mirror
* Click on your favorite mirror to download Spark

Move the downloaded file onto your desired location and run the following commands: 

    mkdir ~/spark
    mv spark-1.6.1.tgz ~/spark
    cd ~/spark
    gunzip spark-1.6.1.tgz
    tar -xvf spark-1.6.1.tar
    cd spark-1.6.1
    build/mvn -DskipTests clean package

####[Option 2] Spark Latest Source (unreleased 2.0.0-SHAPSHOT as of 3/15/2016)
Pull the source tree from github and build. Requires Maven 3.3.9 (latest). 

	git clone https://github.com/apache/spark.git
    build/mvn -DskipTests clean package		

###Confirm installation
Run either the Python or Scala shell to confirm Spark is installed: 

>bin/pyspark

    Welcome to
          ____              __
         / __/__  ___ _____/ /__
        _\ \/ _ \/ _ `/ __/  '_/
       /__ / .__/\_,_/_/ /_/\_\   version 2.0.0-SNAPSHOT
          /_/
    Using Python version 2.7.10 (default, Jul 14 2015 19:46:27)
    SparkContext available as sc, SQLContext available as sqlContext.
    >>>

>bin/spark-shell 

    Welcome to
          ____              __
         / __/__  ___ _____/ /__
        _\ \/ _ \/ _ `/ __/  '_/
       /___/ .__/\_,_/_/ /_/\_\   version 2.0.0-SNAPSHOT
          /_/
         
    Using Scala version 2.11.7 (Java HotSpot(TM) 64-Bit Server VM, Java 1.8.0_45)
    Type in expressions to have them evaluated.
    Type :help for more information.
    scala> 
