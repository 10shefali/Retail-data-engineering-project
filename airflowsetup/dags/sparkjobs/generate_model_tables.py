{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 3,
   "id": "75d41f3b",
   "metadata": {},
   "outputs": [],
   "source": [
    "from pyspark.sql import SparkSession\n",
    "from pyspark.sql.functions import col"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 4,
   "id": "b3849df0",
   "metadata": {},
   "outputs": [],
   "source": [
    "spark=SparkSession.builder.appName('Dimension Product ETL').getOrCreate()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 6,
   "id": "a324fe02",
   "metadata": {},
   "outputs": [],
   "source": [
    "df= spark.read.csv(r\"C:\\Users\\Shefali\\Documents\\Retail_project\\dataset\\cleaned_data.csv\",header= True,inferSchema=True)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 7,
   "id": "ed3ebbbb",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+---------+---------+--------------------+--------+-------------------+---------+----------+--------------+-----------+-----+----+\n",
      "|InvoiceNo|StockCode|         Description|Quantity|        InvoiceDate|UnitPrice|CustomerID|       Country|TotalAmount|Month|Year|\n",
      "+---------+---------+--------------------+--------+-------------------+---------+----------+--------------+-----------+-----+----+\n",
      "|   536365|   85123A|WHITE HANGING HEA...|       6|2010-12-01 08:26:00|     2.55|   17850.0|United Kingdom|       15.3|   12|2010|\n",
      "|   536365|    71053| WHITE METAL LANTERN|       6|2010-12-01 08:26:00|     3.39|   17850.0|United Kingdom|      20.34|   12|2010|\n",
      "|   536365|   84406B|CREAM CUPID HEART...|       8|2010-12-01 08:26:00|     2.75|   17850.0|United Kingdom|       22.0|   12|2010|\n",
      "|   536365|   84029G|KNITTED UNION FLA...|       6|2010-12-01 08:26:00|     3.39|   17850.0|United Kingdom|      20.34|   12|2010|\n",
      "|   536365|   84029E|RED WOOLLY HOTTIE...|       6|2010-12-01 08:26:00|     3.39|   17850.0|United Kingdom|      20.34|   12|2010|\n",
      "+---------+---------+--------------------+--------+-------------------+---------+----------+--------------+-----------+-----+----+\n",
      "only showing top 5 rows\n",
      "\n"
     ]
    }
   ],
   "source": [
    "df.show(5)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 8,
   "id": "a8db4c46",
   "metadata": {},
   "outputs": [],
   "source": [
    "#making dimesnion table for product\n",
    "dim_product = df.select('StockCode','Description').dropDuplicates()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 14,
   "id": "1ef56a32",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+---------+--------------------+\n",
      "|StockCode|         Description|\n",
      "+---------+--------------------+\n",
      "|   84279P|CHERRY BLOSSOM  D...|\n",
      "|    85015|SET OF 12  VINTAG...|\n",
      "|    21249|WOODLAND  HEIGHT ...|\n",
      "|    21002|ROSE DU SUD DRAWS...|\n",
      "|    84987|SET OF 36 TEATIME...|\n",
      "+---------+--------------------+\n",
      "only showing top 5 rows\n",
      "\n"
     ]
    },
    {
     "data": {
      "text/plain": [
       "4161"
      ]
     },
     "execution_count": 14,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "dim_product.show(5)\n",
    "dim_product.count()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 10,
   "id": "8a308ff3",
   "metadata": {},
   "outputs": [],
   "source": [
    "#date dimension table\n",
    "dim_date = df.select('InvoiceDate','Month','Year')"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 11,
   "id": "9d3a3bd7",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+-------------------+-----+----+\n",
      "|        InvoiceDate|Month|Year|\n",
      "+-------------------+-----+----+\n",
      "|2010-12-01 08:26:00|   12|2010|\n",
      "|2010-12-01 08:26:00|   12|2010|\n",
      "|2010-12-01 08:26:00|   12|2010|\n",
      "|2010-12-01 08:26:00|   12|2010|\n",
      "|2010-12-01 08:26:00|   12|2010|\n",
      "+-------------------+-----+----+\n",
      "only showing top 5 rows\n",
      "\n"
     ]
    }
   ],
   "source": [
    "dim_date.show(5)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 12,
   "id": "36056f23",
   "metadata": {},
   "outputs": [],
   "source": [
    "#customer dimension table\n",
    "dim_customer = df.select('CustomerId','Country').dropDuplicates()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 15,
   "id": "58e42e57",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+----------+---------------+\n",
      "|CustomerId|        Country|\n",
      "+----------+---------------+\n",
      "|   12720.0|        Germany|\n",
      "|   17428.0| United Kingdom|\n",
      "|   15224.0| United Kingdom|\n",
      "|   12731.0|         France|\n",
      "|   12727.0|         France|\n",
      "|   12539.0|          Spain|\n",
      "|   12431.0|      Australia|\n",
      "|   12427.0|        Germany|\n",
      "|   12421.0|          Spain|\n",
      "|   12433.0|         Norway|\n",
      "|   14439.0|         Greece|\n",
      "|   12793.0|       Portugal|\n",
      "|   12356.0|       Portugal|\n",
      "|   17404.0|         Sweden|\n",
      "|   12725.0|          Italy|\n",
      "|   12422.0|      Australia|\n",
      "|   14442.0|Channel Islands|\n",
      "|   12744.0|      Singapore|\n",
      "|   12359.0|         Cyprus|\n",
      "|   12779.0|         Poland|\n",
      "+----------+---------------+\n",
      "only showing top 20 rows\n",
      "\n"
     ]
    },
    {
     "data": {
      "text/plain": [
       "4355"
      ]
     },
     "execution_count": 15,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "dim_customer.show()\n",
    "dim_customer.count()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 16,
   "id": "439b2b8f",
   "metadata": {},
   "outputs": [],
   "source": [
    "#Sales fact table\n",
    "fact_sales = df.select('InvoiceNo','StockCode','Quantity','UnitPrice','InvoiceDate','TotalAmount','CustomerId')"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 17,
   "id": "1606830d",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+---------+---------+--------+---------+-------------------+-----------+----------+\n",
      "|InvoiceNo|StockCode|Quantity|UnitPrice|        InvoiceDate|TotalAmount|CustomerId|\n",
      "+---------+---------+--------+---------+-------------------+-----------+----------+\n",
      "|   536365|   85123A|       6|     2.55|2010-12-01 08:26:00|       15.3|   17850.0|\n",
      "|   536365|    71053|       6|     3.39|2010-12-01 08:26:00|      20.34|   17850.0|\n",
      "|   536365|   84406B|       8|     2.75|2010-12-01 08:26:00|       22.0|   17850.0|\n",
      "|   536365|   84029G|       6|     3.39|2010-12-01 08:26:00|      20.34|   17850.0|\n",
      "|   536365|   84029E|       6|     3.39|2010-12-01 08:26:00|      20.34|   17850.0|\n",
      "|   536365|    22752|       2|     7.65|2010-12-01 08:26:00|       15.3|   17850.0|\n",
      "|   536365|    21730|       6|     4.25|2010-12-01 08:26:00|       25.5|   17850.0|\n",
      "|   536366|    22633|       6|     1.85|2010-12-01 08:28:00|       11.1|   17850.0|\n",
      "|   536366|    22632|       6|     1.85|2010-12-01 08:28:00|       11.1|   17850.0|\n",
      "|   536367|    84879|      32|     1.69|2010-12-01 08:34:00|      54.08|   13047.0|\n",
      "|   536367|    22745|       6|      2.1|2010-12-01 08:34:00|       12.6|   13047.0|\n",
      "|   536367|    22748|       6|      2.1|2010-12-01 08:34:00|       12.6|   13047.0|\n",
      "|   536367|    22749|       8|     3.75|2010-12-01 08:34:00|       30.0|   13047.0|\n",
      "|   536367|    22310|       6|     1.65|2010-12-01 08:34:00|        9.9|   13047.0|\n",
      "|   536367|    84969|       6|     4.25|2010-12-01 08:34:00|       25.5|   13047.0|\n",
      "|   536367|    22623|       3|     4.95|2010-12-01 08:34:00|      14.85|   13047.0|\n",
      "|   536367|    22622|       2|     9.95|2010-12-01 08:34:00|       19.9|   13047.0|\n",
      "|   536367|    21754|       3|     5.95|2010-12-01 08:34:00|      17.85|   13047.0|\n",
      "|   536367|    21755|       3|     5.95|2010-12-01 08:34:00|      17.85|   13047.0|\n",
      "|   536367|    21777|       4|     7.95|2010-12-01 08:34:00|       31.8|   13047.0|\n",
      "+---------+---------+--------+---------+-------------------+-----------+----------+\n",
      "only showing top 20 rows\n",
      "\n"
     ]
    }
   ],
   "source": [
    "fact_sales.show()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 19,
   "id": "60867836",
   "metadata": {},
   "outputs": [],
   "source": [
    "fact_sales.toPandas().to_csv(r\"C:\\Users\\Shefali\\Documents\\Retail_project\\dataset\\curated\\fact_sales.csv\",index = False)\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 20,
   "id": "60490354",
   "metadata": {},
   "outputs": [],
   "source": [
    "dim_product.toPandas().to_csv(r\"C:\\Users\\Shefali\\Documents\\Retail_project\\dataset\\curated\\dim_product.csv\",index = False)\n",
    "dim_customer.toPandas().to_csv(r\"C:\\Users\\Shefali\\Documents\\Retail_project\\dataset\\curated\\dim_customer.csv\",index = False)\n",
    "dim_date.toPandas().to_csv(r\"C:\\Users\\Shefali\\Documents\\Retail_project\\dataset\\curated\\dim_date.csv\",index = False)\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "94042852",
   "metadata": {},
   "outputs": [],
   "source": []
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3 (ipykernel)",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.9.13"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 5
}
