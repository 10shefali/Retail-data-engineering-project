{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 1,
   "id": "ae428dc4",
   "metadata": {},
   "outputs": [],
   "source": [
    "from pyspark.sql import SparkSession\n",
    "from pyspark.sql.functions import col"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 2,
   "id": "8d31efef",
   "metadata": {},
   "outputs": [],
   "source": [
    "#Create a Spark Session\n",
    "spark = SparkSession.builder.appName(\"Online Retail ETL\").getOrCreate()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 3,
   "id": "4bbbd399",
   "metadata": {},
   "outputs": [],
   "source": [
    "#Read the data\n",
    "df = spark.read.csv(\"retail_dataset.csv\", header = True, inferSchema = True)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 4,
   "id": "bb97d782",
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "541909"
      ]
     },
     "execution_count": 4,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "df.count()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 5,
   "id": "d528a871",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "root\n",
      " |-- InvoiceNo: string (nullable = true)\n",
      " |-- StockCode: string (nullable = true)\n",
      " |-- Description: string (nullable = true)\n",
      " |-- Quantity: integer (nullable = true)\n",
      " |-- InvoiceDate: timestamp (nullable = true)\n",
      " |-- UnitPrice: double (nullable = true)\n",
      " |-- CustomerID: double (nullable = true)\n",
      " |-- Country: string (nullable = true)\n",
      "\n"
     ]
    }
   ],
   "source": [
    "#explore Schema\n",
    "df.printSchema()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 6,
   "id": "2fa607be",
   "metadata": {},
   "outputs": [],
   "source": [
    "#Cleaning the data\n",
    "DF = df.filter(col('Quantity')>0)\\\n",
    "    .filter(col('UnitPrice')>0)\\\n",
    "    .dropna(subset=['Quantity','InvoiceNo','UnitPrice','Description'])"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 7,
   "id": "f4fd653d",
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "530104"
      ]
     },
     "execution_count": 7,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "DF.count()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 8,
   "id": "dda6fe19",
   "metadata": {},
   "outputs": [],
   "source": [
    "#adding new column\n",
    "DF=DF.withColumn(\"TotalAmount\",col('Quantity')*col('UnitPrice'))"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 9,
   "id": "1df3d765",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+---------+---------+--------------------+--------+-------------------+---------+----------+--------------+------------------+\n",
      "|InvoiceNo|StockCode|         Description|Quantity|        InvoiceDate|UnitPrice|CustomerID|       Country|       TotalAmount|\n",
      "+---------+---------+--------------------+--------+-------------------+---------+----------+--------------+------------------+\n",
      "|   536365|   85123A|WHITE HANGING HEA...|       6|2010-12-01 08:26:00|     2.55|   17850.0|United Kingdom|15.299999999999999|\n",
      "|   536365|    71053| WHITE METAL LANTERN|       6|2010-12-01 08:26:00|     3.39|   17850.0|United Kingdom|             20.34|\n",
      "|   536365|   84406B|CREAM CUPID HEART...|       8|2010-12-01 08:26:00|     2.75|   17850.0|United Kingdom|              22.0|\n",
      "|   536365|   84029G|KNITTED UNION FLA...|       6|2010-12-01 08:26:00|     3.39|   17850.0|United Kingdom|             20.34|\n",
      "|   536365|   84029E|RED WOOLLY HOTTIE...|       6|2010-12-01 08:26:00|     3.39|   17850.0|United Kingdom|             20.34|\n",
      "+---------+---------+--------------------+--------+-------------------+---------+----------+--------------+------------------+\n",
      "only showing top 5 rows\n",
      "\n"
     ]
    }
   ],
   "source": [
    "DF.show(5)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 10,
   "id": "0cec299a",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+---------+---------+--------------------+--------+-------------------+---------+----------+--------------+-----------+\n",
      "|InvoiceNo|StockCode|         Description|Quantity|        InvoiceDate|UnitPrice|CustomerID|       Country|TotalAmount|\n",
      "+---------+---------+--------------------+--------+-------------------+---------+----------+--------------+-----------+\n",
      "|   536365|   85123A|WHITE HANGING HEA...|       6|2010-12-01 08:26:00|     2.55|   17850.0|United Kingdom|       15.3|\n",
      "|   536365|    71053| WHITE METAL LANTERN|       6|2010-12-01 08:26:00|     3.39|   17850.0|United Kingdom|      20.34|\n",
      "|   536365|   84406B|CREAM CUPID HEART...|       8|2010-12-01 08:26:00|     2.75|   17850.0|United Kingdom|       22.0|\n",
      "|   536365|   84029G|KNITTED UNION FLA...|       6|2010-12-01 08:26:00|     3.39|   17850.0|United Kingdom|      20.34|\n",
      "|   536365|   84029E|RED WOOLLY HOTTIE...|       6|2010-12-01 08:26:00|     3.39|   17850.0|United Kingdom|      20.34|\n",
      "|   536365|    22752|SET 7 BABUSHKA NE...|       2|2010-12-01 08:26:00|     7.65|   17850.0|United Kingdom|       15.3|\n",
      "|   536365|    21730|GLASS STAR FROSTE...|       6|2010-12-01 08:26:00|     4.25|   17850.0|United Kingdom|       25.5|\n",
      "|   536366|    22633|HAND WARMER UNION...|       6|2010-12-01 08:28:00|     1.85|   17850.0|United Kingdom|       11.1|\n",
      "|   536366|    22632|HAND WARMER RED P...|       6|2010-12-01 08:28:00|     1.85|   17850.0|United Kingdom|       11.1|\n",
      "|   536367|    84879|ASSORTED COLOUR B...|      32|2010-12-01 08:34:00|     1.69|   13047.0|United Kingdom|      54.08|\n",
      "|   536367|    22745|POPPY'S PLAYHOUSE...|       6|2010-12-01 08:34:00|      2.1|   13047.0|United Kingdom|       12.6|\n",
      "|   536367|    22748|POPPY'S PLAYHOUSE...|       6|2010-12-01 08:34:00|      2.1|   13047.0|United Kingdom|       12.6|\n",
      "|   536367|    22749|FELTCRAFT PRINCES...|       8|2010-12-01 08:34:00|     3.75|   13047.0|United Kingdom|       30.0|\n",
      "|   536367|    22310|IVORY KNITTED MUG...|       6|2010-12-01 08:34:00|     1.65|   13047.0|United Kingdom|        9.9|\n",
      "|   536367|    84969|BOX OF 6 ASSORTED...|       6|2010-12-01 08:34:00|     4.25|   13047.0|United Kingdom|       25.5|\n",
      "|   536367|    22623|BOX OF VINTAGE JI...|       3|2010-12-01 08:34:00|     4.95|   13047.0|United Kingdom|      14.85|\n",
      "|   536367|    22622|BOX OF VINTAGE AL...|       2|2010-12-01 08:34:00|     9.95|   13047.0|United Kingdom|       19.9|\n",
      "|   536367|    21754|HOME BUILDING BLO...|       3|2010-12-01 08:34:00|     5.95|   13047.0|United Kingdom|      17.85|\n",
      "|   536367|    21755|LOVE BUILDING BLO...|       3|2010-12-01 08:34:00|     5.95|   13047.0|United Kingdom|      17.85|\n",
      "|   536367|    21777|RECIPE BOX WITH M...|       4|2010-12-01 08:34:00|     7.95|   13047.0|United Kingdom|       31.8|\n",
      "+---------+---------+--------------------+--------+-------------------+---------+----------+--------------+-----------+\n",
      "only showing top 20 rows\n",
      "\n"
     ]
    }
   ],
   "source": [
    "#rounding in pyspark\n",
    "from pyspark.sql.functions import round\n",
    "DF=DF.withColumn('TotalAmount' , round(col('TotalAmount'),2))\n",
    "DF.show()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 11,
   "id": "613d04c7",
   "metadata": {},
   "outputs": [],
   "source": [
    "from pyspark.sql.functions  import month, year\n",
    "DF = DF.withColumn('Month', month(col('InvoiceDate')))\\\n",
    "    .withColumn('Year', year(col('InvoiceDate')))"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 12,
   "id": "e13a4002",
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
    "DF.show(5)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 13,
   "id": "d5350fbd",
   "metadata": {},
   "outputs": [],
   "source": [
    "#“How much revenue did we make each month?”\n",
    "from pyspark.sql.functions import sum\n",
    "monthly_revenue = DF.groupBy('Year','Month')\\\n",
    "    .agg(round(sum('TotalAmount'),2).alias('Monthly Revenue'))\\\n",
    "    .orderBy('Year','Month')"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 14,
   "id": "017ea854",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+----+-----+---------------+\n",
      "|Year|Month|Monthly Revenue|\n",
      "+----+-----+---------------+\n",
      "|2010|   12|      823746.14|\n",
      "|2011|    1|      691364.56|\n",
      "|2011|    2|      523631.89|\n",
      "|2011|    3|      717639.36|\n",
      "|2011|    4|      537808.62|\n",
      "|2011|    5|      770536.02|\n",
      "|2011|    6|       761739.9|\n",
      "|2011|    7|      719221.19|\n",
      "|2011|    8|      759138.38|\n",
      "|2011|    9|     1058590.17|\n",
      "+----+-----+---------------+\n",
      "only showing top 10 rows\n",
      "\n"
     ]
    }
   ],
   "source": [
    "monthly_revenue.show(10)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 17,
   "id": "318225d3",
   "metadata": {},
   "outputs": [],
   "source": [
    "#saving the cleaned data in csv format\n",
    "pdf = DF.toPandas()\n",
    "pdf.to_csv('cleaned_data', index= False)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": None,
   "id": "27dad46b",
   "metadata": {},
   "outputs": [],
   "source": [
    "\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": None,
   "id": "32b1aa10",
   "metadata": {},
   "outputs": [],
   "source": [
    "\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": None,
   "id": "51587705",
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
