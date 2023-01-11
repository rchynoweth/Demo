import time 

time.sleep(10)

print("Hello World!!")


spark.sql("USE overwatch_target")


df = spark.sql("SELECT * FROM job LIMIT 10")

display(df)


print("-------------- row count == {}".format(df.count()))