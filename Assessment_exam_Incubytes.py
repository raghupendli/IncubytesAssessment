import pyspark.sql.SparkSession

spark = SparkSession.builder \
		.master("local") \
		.appName("Task") \
		.getOrCreate()

##Dataframe with customer data

jdbcDF = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql:dbserver") \
    .option("dbtable", "schema.tablename") \
    .option("user", "username") \
    .option("password", "password") \
    .load()

country_list=list(jdbcDF['Country'].distinct())

emptydf=spark.emptyDataFrame

##create tables for all the countries

for i in country_list:

	emptydf.write \
    .option("createTableColumnTypes", "Name varchar(15), Cust_I int,Open_Dt Datetime,Consul_Dt Datetime,VAC_ID varchar(15),DR_Name varchar(15),State varchar(15),Country varchar(15),DOB date,FLAG varchar2(15)") \
    .jdbc("jdbc:postgresql:dbserver", f'Table_{i}',
          properties={"user": "username", "password": "password"})
		  
df_filecontent = spark.read.csv("filepath", sep="|",header=True)
	
df_filecontent.registertempTable("full_staging_data")
sqlContext.cacheTable("full_staging_data")

for i in country_list:
	val data_f'{i}'=spark.sql("select * from database.stagingtable where Country=f'{i}'")
	
	data_f'{i}'.write \
    .jdbc("jdbc:postgresql:dbserver", f'Table_{i}',
          properties={"user": "username", "password": "password"})

	





