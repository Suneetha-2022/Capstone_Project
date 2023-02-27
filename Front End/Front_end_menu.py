import pyinputplus as pyip
import pyspark
from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
from Front_end_functions import*


menu= pyip.inputMenu(['Display customer transactions', 'CustomerTotal for given Transaction Type', 'Count and Value for All Branches of a state', 
                    'Check customer account details', 'Cutomer monthly transactions', 'Transaction_value b/w two dates', 
                    'Exit'], numbered=True)



if menu == "Display customer transactions":
    customer_transactions()
elif menu == "CustomerTotal for given Transaction Type":
     value_transactions()
elif menu == "Count and Value for All Branches of a state": 
     state_transactions()
elif menu == "Check customer account details":
     customer_info()
elif menu == "Cutomer monthly transactions":
     monthly_bill()
elif menu == 'Transaction_value b/w two dates':
     trans_dates()
elif menu == "Exit":
     exit()
else:
    print("Please make a valid selection")


