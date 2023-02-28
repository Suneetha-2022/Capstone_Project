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
    #Connect to database    
    con = mdb1.connect(
          host = "localhost",
          user  = "root",
          password = "password",
          database = "capstone_project"
    
        )
    print("connected to database...")
    cur = con.cursor()

    #Regex patterns for month, year and zipcode    
    mpattern = r'^(0?[1-9]|1[0-2])$'
    ypattern = r'^[0-9]{4}$'
    zpattern = r'^[0-9]{5}$'

    #Taking input from console   
    while True:
        month = input("Enter month number in between 1 and 12: ")
        if re.match(mpattern, month):
            print("Valid input")
            break
        else:
            print("Invalid month, please enter number in between 1 and 12")

   
    while True:
        year = input("Enter year in 'yyyy' format: ")
        if re.match(ypattern, year):
            print("Valid input")
            break
        else:
            print("Invalid year, please enter 4 digit number in 'yyyy' format: ")

    while True:
        zipcode = input("Enter 5 digit zipcode: ")
        if re.match(zpattern, zipcode):
            print("Valid input")
            break
        else:
            print("Invalid zipcode, please enter 5 digit zipcode: ")
        
    print('Here are the results')

    #Query the dataset

    st = "SELECT  cc.YEAR, cc.MONTH, cc.DAY, sc.CUST_ZIP, cc.transaction_id FROM cdw_sapp_credit_card cc \
          INNER JOIN  cdw_sapp_customer sc \
          WHERE cc.cust_ssn = sc.ssn and cc.MONTH = {} AND cc.YEAR = {} AND sc.CUST_ZIP = {} \
          order by cc.DAY desc"
    cur.execute(st.format(month, year, zipcode))

    result = cur.fetchall()
    #Convert result list to dataframe    
    df_ct = pd.DataFrame(result, columns=['YEAR', 'Month', 'DAY', 'CUST_ZIP', 'TRANSACTION_ID'])
    if df_ct.empty:
        print('Data unavailable for given input')
    else:
        print(df_ct)
    con.close()

#customer_transactions()


#2.Used to display the number and total values of transactions for a given type.

def value_transactions():
    #Connecting to database
    con2 = mdb1.connect(
          host = "localhost",
          user  = "root",
          password = "password",
          database = "capstone_project"
    
        )
    print("connected to database...")
    cur2 = con2.cursor()
    
    #Taking input from console    
    type_pattern = r'^[a-zA-Z]+$'
    
    while True:
        T_type = input("Please enter Transaction Type: ")
        if re.match(type_pattern, T_type):
            print("Valid input")
            break
        else:
            print("Invalid input, try again")

    
    print('Here are the results')  
    #Query to process data

    st2 = "SELECT COUNT(transaction_id), round(SUM(transaction_value),2) FROM cdw_sapp_credit_card \
          WHERE transaction_type = '{}'"
    cur2.execute(st2.format(T_type))

    result2 = cur2.fetchall()

    #Convert result list to dataframe   
    if result2 == [(0, None)]:
        print('Data unavailable for given input')
    else:
        df_vt = pd.DataFrame(result2, columns=['TRANSACTION_COUNT', 'TRANSACTION_VALUE'])
        print(df_vt)
    con2.close()

#value_transactions()

#3.display the number and total values of transactions for branches in a given state.
def state_transactions():
    #Connecting to database   
    con3 = mdb1.connect(
          host = "localhost",
          user  = "root",
          password = "password",
          database = "capstone_project"
    
        )
    print("connected to database...")
    cur3 = con3.cursor()

    #Accept input from console    
    state_pattern = r'^[A-Z]{2}$'
    while True:
        state = input("Please enter State 'XX': ")
        if re.match(state_pattern, state):
            print("Valid input")
            break
        else:
            print("Invalid input, try again")

    
    print('Here are the results')  
    
    st3 = "SELECT  COUNT(cc.transaction_id), round(SUM(cc.transaction_value),2), \
           b.BRANCH_CODE, b.branch_state FROM cdw_sapp_credit_card cc INNER JOIN  cdw_sapp_branch b \
           WHERE cc.branch_code = b.branch_code AND b.branch_state = '{}' GROUP BY cc.branch_code "
    cur3.execute(st3.format(state))

    result3 = cur3.fetchall()

    #Convert result list to dataframe    
    df_st = pd.DataFrame(result3, columns=['TRANSACTION_COUNT', 'TRANSACTION_VALUE', 'BRANCH_CODE', 'BRANCH_STATE'])
    if df_st.empty:
        print('Data unavailable for given input')
    else:
        print(df_st)
    con3.close()

#state_transactions()

#4.check the existing account details of a customer.
def customer_info():
    #Connecting to database    
    con4 = mdb1.connect(
          host = "localhost",
          user  = "root",
          password = "password",
          database = "capstone_project"
    
        )
    print("connected to database...")
    cur4 = con4.cursor()
    
    fname_pattern = r'^[A-Z][a-z]*$'
    lname_pattern = r'^[A-Z][a-z]*$'

    #Accept input from Console   
    while True:
        f_name = input("Please enter customer first_name(first letter capital): ")
        if re.match(fname_pattern, f_name):
            print("Valid input")
            break
        else:
            print("Invalid input, try again")
    
    while True:
        l_name = input("Please enter customer last_name(last letter capital): ")
        if re.match(lname_pattern, l_name):
            print("Valid input")
            break
        else:
            print("Invalid input, try again")
        
    print("Here are the results")

    #Query to process data     
    st4 = "SELECT CREDIT_CARD_NO, CUST_CITY, CUST_COUNTRY, CUST_EMAIL, CUST_PHONE, CUST_STATE, CUST_ZIP, \
           SSN, FULL_STREET_ADDRESS FROM cdw_sapp_customer WHERE first_name = '{}' AND last_name = '{}'"
 
    cur4.execute(st4.format(f_name, l_name))

    result4 = cur4.fetchall()

    #Convert result list to dataframe    
    df_ci = pd.DataFrame(result4, columns=['CREDIT_CARD_NO', 'CUST_CITY', 'CUST_COUNTRY', 'CUST_EMAIL', 'CUST_PHONE', \
                                           'CUST_STATE', 'CUST_ZIP', 'SSN', 'FULL_STREET_ADDRESS'])
    if df_ci.empty:
        print('Data unavailable for given input')
    else:
        print(df_ci)
    con4.close()

#customer_info()

#5.Used to modify the existing account details of a customer.
def m_cust_city():
    #Connecting to database    
    con51 = mdb1.connect(
          host = "localhost",
          user  = "root",
          password = "password",
          database = "capstone_project"
    
        )
    print("connected to database...")
    cur51 = con51.cursor()
    
    ccity_pattern = r'^[a-zA-Z\s]+$'
    czip_pattern = r'^\d{5}$'
    fad_pattern = r'^[0-9a-zA-Z\s\-\.\_]{0,250}$'
    ccard_pattern = r'^\d{16}$'
    #Accept input from Console   
    while True:
        c_city = input("Please enter customer city: ")
        if re.match(ccity_pattern, c_city):
            print("Valid input")
            break
        else:
            print("Invalid input, try again")
    
    while True:
        c_zip = input("Please enter 5 digit customer zip code: ")
        if re.match(czip_pattern, c_zip):
            print("Valid input")
            break
        else:
            print("Invalid input, try again")
        
    while True:
        c_fad = input("Please enter customer full address: ")
        if re.match(fad_pattern, c_fad):
            print("Valid input")
            break
        else:
            print("Invalid input, try again")
    
    while True:
        c_card = input("Please enter 16 digit customer credit number: ")
        if re.match(ccard_pattern, c_card):
            print("Valid input")
            break
        else:
            print("Invalid input, try again")

    
    
    
    print("Here are the updated results")

    #Query to process data     
    st51 = "UPDATE cdw_sapp_customer set cust_city = '{}', cust_zip = {}, full_street_address = '{}'  \
             WHERE credit_card_no = '{}'"
    st51=st51.format(c_city, c_zip, c_fad, c_card)
    
    cur51.execute(st51.format(c_city, c_zip, c_fad, c_card))

    con51.commit()
    sq2 = "Select credit_card_no, cust_city, cust_country, cust_email, \
           cust_phone, cust_state, cust_zip, full_street_address from cdw_sapp_customer where credit_card_no = '{}'"
    cur51.execute(sq2.format(c_card))
    
    result51 = cur51.fetchall()

    #Convert result list to dataframe    
    df_mcc = pd.DataFrame(result51, columns=['CREDIT_CARD_NO', 'CUST_CITY', 'CUST_COUNTRY', 'CUST_EMAIL', 'CUST_PHONE', \
                                            'CUST_STATE', 'CUST_ZIP', 'FULL_STREET_ADDRESS'])
    
    
    print(df_mcc)
    con51.close()
    

#m_cust_city()



#6.generate a monthly bill for a credit card number for a given month and year.
def monthly_bill():
    #Connecting to database    
    con6 = mdb1.connect(
          host = "localhost",
          user  = "root",
          password = "password",
          database = "capstone_project"
    
        )
    print("connected to database...")
    cur6 = con6.cursor()
    
    mn_pattern = r'^(0?[1-9]|1[0-2])$'
    yr_pattern = r'^[0-9]{4}$'

    #Accepting input from console    
    while True:
        month_num = input("Please enter month number from 1 to 12: ")
        if re.match(mn_pattern, month_num):
            print("Valid input")
            break
        else:
            print("Invalid input, try again")
    
    while True:
        year_num = input("Please enter 4 digit year number: ")
        if re.match(yr_pattern, year_num):
            print("Valid input")
            break
        else:
            print("Invalid input, try again")
        
    print("Here are the results")
    
    #Query to process data
    st6 = "SELECT MONTH, YEAR, round(sum(transaction_value), 2) AS 'AMOUNT'FROM cdw_sapp_credit_card \
           GROUP BY MONTH, YEAR HAVING MONTH = {} AND YEAR = {}"
 
    cur6.execute(st6.format(month_num, year_num))

    result6 = cur6.fetchall()

    #Convert result list to dataframe        
    df_mb = pd.DataFrame(result6, columns=['MONTH', 'YEAR', 'AMOUNT'])
    if df_mb.empty:
        print('Data unavailable for given input')
    else:
        print(df_mb)
    con6.close()

#monthly_bill()

def trans_dates():
    #Connecting to database    
    con7 = mdb1.connect(
          host = "localhost",
          user  = "root",
          password = "password",
          database = "capstone_project"
    
        )
    print("connected to database...")
    cur7 = con7.cursor()
    
    d_pattern = r'^[0-9]{8}$'
    

    #Accepting input from console    
    while True:
        date1 = input("Please enter first date in 'yyyymmdd' format: ")
        if re.match(d_pattern, date1):
            print("Valid input")
            break
        else:
            print("Invalid input, try again")
    
    while True:
        date2 = input("Please enter second date in 'yyyymmdd' format (must be greater than previous date): ")
        if re.match(d_pattern, date2):
            print("Valid input")
            break
        else:
            print("Invalid input, try again")
        
    print("Here are the results")

    st7 = "SELECT round(SUM(transaction_value), 2) AS total, '{date1}' AS date_1, '{date2}' AS date_2,\
           CASE WHEN '{date2}' > '{date1}' THEN 'Valid date format, if None data is not available' ELSE 'Not_Valid dates second date should be greater' END AS date_values\
           FROM cdw_sapp_credit_card WHERE timeid >= {date1} AND timeid <= {date2}"

 
    cur7.execute(st7.format(date1 = date1, date2 = date2))

    result7 = cur7.fetchall()
    
    #Convert result list to dataframe        
    df_date = pd.DataFrame(result7, columns=['Total transaction value', 'DATE_1', 'DATE_2', 'Valid Dates or not'])
    if df_date.empty:
        print('Data unavailable for given input')
    else:
        print(df_date)
    con7.close()

#trans_dates()

