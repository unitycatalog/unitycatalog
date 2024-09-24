# Unity Catalog and Spark

This page explains how to create Unity Catalog tables and volumes with Apache Spark™.

Spark is a great engine when your data is organized in Unity Catalog and vice versa.  

* Neatly organizing data in tables and volumes in the Unity Catalog hierarchy makes it a lot easier to write Spark code.  
* Make it easier to decouple business logic from file paths.  
* Provides easy access to different file formats without end users needing to know how the data is stored.

Let’s examine how to use Spark with Unity Catalog and discuss the advantages in more detail.

## Create a managed Unity Catalog table with Spark

Let’s create a managed Unity Catalog table with Spark.

```python
df = spark.createDataFrame([  
    (1, "socks"),   
    (2, "chips"),  
    (3, "air conditioner"),  
    (4, "tea"),  
], ["transaction_id", "item_name"])

df.write.format("parquet").saveAsTable("stores.us_east.transactions")
```

This table uses the three-level Unity Catalog namespacing: `catalog_name.schema_name.table_name`.

Confirm that we’re able to read the table:

```
spark.table("stores.us_east.transactions").show()

+--------------+---------------+  
|transaction_id|      item_name|  
+--------------+---------------+  
|             1|          socks|  
|             2|          chips|  
|             3|air conditioner|  
|             4|            tea|  
+--------------+---------------+
```

Now, let’s create another managed table:

```python
voided = spark.createDataFrame([(1,), (4,)], ["transaction_id"])

voided.write.format("parquet").saveAsTable("stores.us_east.voided")
```

Here’s a visualization of the tables.  

![UC Spark 1](../assets/images/uc_spark1.png)

Suppose the transactions table represents all the transactions, and the voided table contains the transactions that were subsequently canceled.  Compute all the non-voided transactions.

```python
transactions = spark.table("stores.us_east.transactions")  
voided = spark.table("stores.us_east.voided")

transactions.join(  
    voided,   
    transactions.transaction_id == voided.transaction_id,   
    "leftanti"  
).show()
```

```
+--------------+---------------+  
|transaction_id|      item_name|  
+--------------+---------------+  
|             2|          chips|  
|             3|air conditioner|  
+--------------+---------------+
```

Now, let’s look at how to create external Unity Catalog tables.

## Create an external Unity Catalog table with Spark

Here’s how to create an external Unity Catalog table with Spark:

```python
letters = spark.createDataFrame([  
    (1, "a"),   
    (2, "b"),  
    (3, "c"),  
], ["id", "letter"])

letters.write.format("parquet").saveAsTable("stores.whatever.letters", path="/tmp/letters")
```

When you manually specify the path, as in the example above, Spark creates an unmanaged table.

Let’s look at a visualization of our current tables:

![UC Spark 2](../assets/images/uc_spark2.png)

See this page to learn more about the difference between external and managed Unity Catalog tables.

## Spark write data to path

You can also use Spark to write data to a path and not make any Unity Catalog entries.

```python
people = spark.createDataFrame([  
    (1, "li"),   
    (2, "chung"),  
], ["id", "first_name"])

people.write.format("parquet").save("/tmp/people")
```

As you can see in the example above, writing data with Spark doesn’t necessarily create a new entry in the Unity Catalog. This writes data to a path but doesn’t create any associated entries in the Unity Catalog, burdening the user with managing the credentials and access to the data themselves.

## Advantage of Unity Catalog for Spark

Unity Catalog has many advantages for Spark users.

**Decoupling data storage location from business logic**

You don’t want to hardcode file paths in your code because then the location of your data becomes coupled with your business logic.  Here’s an example of bad code:

```python
people = spark.read.format("parquet").load("/tmp/people")  
...
```

This code would break if the location of the underlying data ever changes.

```python
people = spark.table("stores.us_east.people")
```

This allows for the underlying table to be moved without breaking the business logic.

**Making it easier to organize and find datasets**

When datasets are registered in Unity Catalog, you can use the CLI or UI to easily find all the data assets.  Let’s take a look at the Unity Catalog UI:

![UC Spark 3](../assets/images/uc_spark3.png)

We can easily see the schemas, tables, volumes, and functions that are registered in Unity Catalog!

**Allows for ergonomic implementations of common design patterns, like the Medallion architecture**

The Medallion architecture is a conceptual data organization framework that’s sometimes useful when building ETL pipelines, as described in [this blog post](https://delta.io/blog/delta-lake-medallion-architecture/).

Unity Catalog is a great way to organize data assets for pipelines that use the Medallion architecture.  You can create bronze, silzer, and gold schemas to clearly bucket the different data assets.

**Flexibility to use managed or external tables**

It’s easy to make either external or managed Unity Catalog tables with Spark.

If you want to manage the path yourself, you can use external tables.

If you want Unity Catalog to manage the paths, you can use managed tables.

In either case, Unity Catalog still allows for the decoupling of business logic and the underlying storage paths.

**Easy access to different types of data assets and file formats**

When you’re reading data from paths, you need to understand the underlying data storage format.  Here are a couple of examples:

```python
people = spark.read.format("parquet").load("/tmp/people")  
countries = spark.read.format("csv").load("/tmp/countries")
```

Your business logic needs to know that the people dataset is stored in Parquet files and the countries dataset is stored in CSV files.

Unity Catalog provides a much better user experience because you don’t need to know how the underlying data is stored and can write code like this:

```python
people = spark.table("customers.us_east.people")  
countries = spark.table("users.demographics.countries")
```
