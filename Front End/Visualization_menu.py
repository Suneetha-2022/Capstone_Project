import pyinputplus as pyip
import pyspark
from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
from Visualization_functions import*
from Visualization_functions2 import*

menu= pyip.inputMenu(['Transaction type has a high rate of transactions', 
                    'State with high number of customers', 
                    'Customer with highest transaction amount', 
                    'Percentage of self-employed applicants', 
                    'Percentage of rejection for married male applicants', 
                    'Top three months with the largest transaction data',
                    'Branch which processed the highest healthcare transactions', 
                    'Exit'], numbered=True)



if menu == "Transaction type has a high rate of transactions":
       trans_value_rate()
elif menu == "State with high number of customers":
       cust_state()
elif menu == "Customer with highest transaction amount": 
       cust_hight()
elif menu == "Percentage of self-employed applicants":
       self_emp()
elif menu == "Percentage of rejection for married male applicants":
       mm_plot()
elif menu == 'Top three months with the largest transaction data':
      top_trans()
elif menu == 'Branch which processed the highest healthcare transactions':
      branch_high()
elif menu == "Exit":
      exit()
else:
    print("Please make a valid selection")
