source_json = """
{
    "persons": [
        {
            "name": "John",
            "age": 30,
            "cars": [
                {
                    "name": "Ford",
                    "models": [
                        "Fiesta",
                        "Focus",
                        "Mustang"
                    ]
                },
                {
                    "name": "BMW",
                    "models": [
                        "320",
                        "X3",
                        "X5"
                    ]
                }
            ]
        },
        {
            "name": "Peter",
            "age": 46,
            "cars": [
                {
                    "name": "Huyndai",
                    "models": [
                        "i10",
                        "i30"
                    ]
                },
                {
                    "name": "Mercedes",
                    "models": [
                        "E320",
                        "E63 AMG"
                    ]
                }
            ]
        }
    ]
}
"""

dbutils.fs.put("/tmp/source.json", source_json, True)
source_df = spark.read.option("multiline", "true").json("/tmp/source.json")

from pyspark.sql.functions import *
from pyspark.sql.types import *

persons = source_df.select(explode("persons").alias("persons"))
persons_cars = persons.select(
   col("persons.name").alias("persons_name")
 , col("persons.age").alias("persons_age")
 , col("persons.cars").alias("persons_cars")
)
display(persons_cars)

persons_cars.createOrReplaceTempView("personTable")
display(spark.sql("select cars.name,models,persons_age,persons_name from personTable pt lateral view explode(persons_cars) as cars lateral view explode(cars.models) as models"))
