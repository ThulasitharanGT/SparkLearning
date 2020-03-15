from pyspark.sql import SparkSession
def main() :
	print("Hello World!")
def pySparkCodeFunction() :
	spark=SparkSession.builder.appName('abc').getOrCreate()
	df = spark.read.option("header", "true").csv("/temp/sdss6949386.csv")
	df.show(n=2)
if __name__ == "__main__" :
	main()
	pySparkCodeFunction()
