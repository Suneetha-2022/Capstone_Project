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
import re
import mysql
import mysql.connector as mdb1

#1.
#display the transactions made by customers living in a given zip code for a given month and year. 
# Order by day in descending order.

def customer_transactions():
    con = mdb1.connect(
          host = "localhost",
          user  = "root",
          password = "password",
          database = "capstone_project"
    
        )
    print("connected to database...")
    cur = con.cursor()
    month = input("Enter month number '[1-12]': ")
    year  = input("Enter year 'YYYY': ")
    zipcode = input("Enter zipcode 'xxxxx': ")
    mpattern = r'\d{1,2}'
    ypattern = r'\d{4}'
    zpattern = r'\d{5}'
    if not re.match(mpattern, month):
        print("Invalid month. Please enter a number between 1 and 12.")
    elif not re.match(ypattern, year):
        print("Invalid year. Please enter a four-digit year.")
    elif not re.match(zpattern, zipcode):
        print("Invalid ZIP code. Please enter a five-digit ZIP code.")
    else:
        print("Valid input. Thank you!")
        
    
    st = "SELECT  cc.YEAR, cc.MONTH, sc.CUST_ZIP, COUNT(cc.transaction_id) FROM cdw_sapp_credit_card cc \
          INNER JOIN  cdw_sapp_customer sc \
          WHERE cc.cust_ssn = sc.ssn and cc.MONTH = {} AND cc.YEAR = {} AND sc.CUST_ZIP = {}"
    cur.execute(st.format(month, year, zipcode))

    result = cur.fetchall()
    #print(result)
    for rows in result:
        print(rows)
    con.close()

customer_transactions()


#Used to display the number and total values of transactions for a given type.

def value_transactions():
    con2 = mdb1.connect(
          host = "localhost",
          user  = "root",
          password = "password",
          database = "capstone_project"
    
        )
    print("connected to database...")
    cur2 = con2.cursor()
    T_type = input("Please enter Transaction Type: ")
    type_pattern = r'^[a-zA-Z]+$'
    if not re.match(type_pattern, T_type):
        print("Invalid input. Please enter correct input")
    else:
        print("Valid input. Thank you!")
        
    
    st2 = "SELECT COUNT(transaction_id), round(SUM(transaction_value),2) FROM cdw_sapp_credit_card \
          WHERE transaction_type = '{}'"
    cur2.execute(st2.format(T_type))

    result2 = cur2.fetchall()
    #print(result)
    for rows in result2:
        print(rows)
    con2.close()

value_transactions()
