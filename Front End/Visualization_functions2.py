import requests
import pandas as pd
import numpy as np
import findspark
import pandas as pd
import numpy as np
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import*
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, DoubleType
from pyspark import SparkContext
from pyspark.sql.functions import monotonically_increasing_id
import matplotlib as mpl
import matplotlib.pyplot as plt
#from secrets import username, password

spark = SparkSession.builder.appName("Visualizations_App2").getOrCreate()

def trans_value_rate():
    query1= "(WITH total_transactions AS \
             (SELECT SUM(transaction_value) AS 'sum_total' \
              FROM cdw_sapp_credit_card), sum_trans_value AS \
            (SELECT transaction_type, SUM(transaction_value) AS 'SUM1' \
             FROM cdw_sapp_credit_card GROUP BY transaction_type) \
             SELECT sum_trans_value.transaction_type,  \
             ROUND((sum_trans_value.SUM1 *100/ total_transactions.sum_total),2) AS Rate \
             FROM total_transactions \
             INNER JOIN sum_trans_value) as trans_value"

    strans_rate = spark.read.format("jdbc").options(driver="com.mysql.cj.jdbc.Driver",\
                                     user="root",\
                                     password="password",\
                                     url="jdbc:mysql://localhost:3306/capstone_project",\
                                     dbtable=query1).load()

    #strans_rate.show()
    pdtrans_rate = strans_rate.toPandas()
    pdtrans_rate.set_index('transaction_type', inplace = True)
    #pdtrans_rate
    pdtrans_rate.plot(kind='bar', figsize=(10, 6), color = 'blue')
    xlabel = plt.xlabel('TRANSACTION TYPE')
    #plt.xlabel('Branch_code with Health_care transaction')
    xlabel.set_position((0.5, 0.1))
    plt.ylabel('Rate of Transaction')
    plt.title('Transaction_Type with high rate of Transaction')
    plt.xticks(rotation=45)
    plt.show()
    spark.stop()
    

#trans_value_rate()

def cust_state():
    query2= "(SELECT cust_state, COUNT(*) AS \
            'Customers_count' FROM cdw_sapp_customer \
             GROUP BY cust_state ORDER BY COUNT(*) desc LIMIT 5) as cust_count"

    scust_count = spark.read.format("jdbc").options(driver="com.mysql.cj.jdbc.Driver",\
                                     user="root",\
                                     password="password",\
                                     url="jdbc:mysql://localhost:3306/capstone_project",\
                                     dbtable=query2).load()

    #strans_rate.show()
    pdcust_count = scust_count.toPandas()
    pdcust_count.set_index('cust_state', inplace = True)
    #pdtrans_rate
    pdcust_count.plot(kind='bar', figsize=(10, 6), color = 'blue')
    xlabel = plt.xlabel('Customer Sate')
    #plt.xlabel('Branch_code with Health_care transaction')
    xlabel.set_position((0.5, 0.1))
    plt.ylabel('No of Customers')
    plt.title('State with high number of customers')
    plt.xticks(rotation=45)
    plt.show()
    spark.stop()

#cust_state()

"""def cust_state():
    query2= "(SELECT cust_state, COUNT(*) AS \
            'Customers_count' FROM cdw_sapp_customer \
             GROUP BY cust_state ORDER BY COUNT(*) desc LIMIT 5) as cust_count"

    scust_count = spark.read.format("jdbc").options(driver="com.mysql.cj.jdbc.Driver",\
                                     user="root",\
                                     password="password",\
                                     url="jdbc:mysql://localhost:3306/capstone_project",\
                                     dbtable=query2).load()

    #scust_count.show()
    pdcust_count = scust_count.toPandas()
    pdcust_count.set_index('cust_state', inplace = True)
    #pdcust_count
    pdcust_count.plot(kind='bar', figsize=(10, 6), color = 'blue')
    xlabel = plt.xlabel('Customer Sate')
    #plt.xlabel('Branch_code with Health_care transaction')
    xlabel.set_position((0.5, 0.1))
    plt.ylabel('No of Customers')
    plt.title('State with high number of customers')
    plt.xticks(rotation=45)
    plt.show()
    spark.stop()"""

def cust_hight():
    query3 = "(SELECT a.FIRST_NAME, a.LAST_NAME, SUM(b.transaction_value) `total_transaction` FROM \
              cdw_sapp_customer a INNER JOIN cdw_sapp_credit_card b WHERE a.ssn = b.cust_ssn group BY a.first_name, a.last_name ORDER BY  `total_transaction` limit 10) as cust"

    df_top10 = spark.read.format("jdbc").options(driver="com.mysql.cj.jdbc.Driver",\
                                     user="root",\
                                     password="password",\
                                     url="jdbc:mysql://localhost:3306/capstone_project",\
                                     dbtable=query3).load()

    #df_top10.show()
    p_df = df_top10.toPandas()
    p_df.set_index(["FIRST_NAME", "LAST_NAME"], inplace = True)
    #p_df
    p_df.plot(kind='bar', figsize=(10, 6), rot=90, color = 'blue')
    plt.ylim(0, 100)
    plt.xlabel('Customer Name')
    plt.ylabel('Transaction Value')
    plt.title('Customers with highest transaction amount')
    plt.xticks(rotation=45)
    plt.show()
    spark.stop()

#cust_hight()


