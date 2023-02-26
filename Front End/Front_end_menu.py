import pyinputplus as pyip
import pyspark
from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
from Front_end_functions import*

print("Calculator Menu")
#print("1. Add")
#print("2. Subtract")
#print("3. Multiply")
#print("4. Divide")


#while True: 
menu= pyip.inputMenu(['Add', 'Subtract', 'car_spark', 'line_plot1', 'Exit'], numbered=True)


#selection= pyip.inputInt("Choose a numeric selection from above: ", min=1)
if menu == "Add":
    add()
    
elif menu == "Subtract":
    sub()
elif menu == "car_spark": 
     car_spark()
elif menu == "line_plot1":
     plot1()
        
elif menu == "Exit":
     exit()  
else:
    print("Please make a valid selection")


