source_json_donut = """
{
 "id": "0001",
 "type": "donut",
 "name": "Cake",
 "ppu": 0.55,
 "batters":
  {
   "batter":
    [
     { "id": "1001", "type": "Regular" },
     { "id": "1002", "type": "Chocolate" },
     { "id": "1003", "type": "Blueberry" }
    ]
  },
 "topping":
  [
   { "id": "5001", "type": "None" },
   { "id": "5002", "type": "Glazed" },
   { "id": "5005", "type": "Sugar" },
   { "id": "5007", "type": "Powdered Sugar" },
   { "id": "5006", "type": "Chocolate with Sprinkles" },
   { "id": "5003", "type": "Chocolate" },
   { "id": "5004", "type": "Maple" }
  ]
}
"""

dbutils.fs.put("/tmp/source_donut.json", source_json_donut, True)
source_df = spark.read.option("multiline", "true").json("/tmp/source_donut.json")

rawDF = spark.read.json("/tmp/source_donut.json", multiLine = "true")
rawDF.show()

from pyspark.sql.functions import *
from pyspark.sql.types import *

sampleDF = rawDF.withColumnRenamed("id", "key")

batters = (sampleDF.select("key",explode("batters.batter").alias("batter_n"))
          .select("key","batter_n.*")
           .withColumnRenamed("id","batter_id")
           .withColumnRenamed("type","batter_type"))

batters.show()
toppingDF = rawDF.withColumnRenamed("id","top_key")
toppings = (toppingDF.select("top_key",explode("topping").alias("topping_n"))
          .select("top_key","topping_n.*")
          .withColumnRenamed("id","top_id")
          .withColumnRenamed("type","top_type"))
toppings.show()
