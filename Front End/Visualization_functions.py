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

spark = SparkSession.builder.appName("Visualizations_App").getOrCreate()

def branch_high():
    query1= "(SELECT a.BRANCH_CODE, b.transaction_type, round(SUM(b.transaction_value), 2) `total_transaction` \
        FROM cdw_sapp_branch a INNER JOIN \
        cdw_sapp_credit_card b WHERE a.branch_code = b.branch_code and \
        b.TRANSACTION_TYPE = 'Healthcare' group BY a.branch_code \
        ORDER BY  total_transaction DESC limit 4) as bank_high"

    sdf_branch = spark.read.format("jdbc").options(driver="com.mysql.cj.jdbc.Driver",\
                                     user="root",\
                                     password="password",\
                                     url="jdbc:mysql://localhost:3306/capstone_project",\
                                     dbtable=query1).load()

    #sdf_branch.show()
    pdf_branch = sdf_branch.toPandas()
    pdf_branch.set_index(['BRANCH_CODE', 'transaction_type'], inplace = True)
    #pdf_branch
    pdf_branch.plot(kind='bar', figsize=(10, 6), color = 'blue')
    xlabel = plt.xlabel('Branch_code with Health_care transaction')
    #plt.xlabel('Branch_code with Health_care transaction')
    xlabel.set_position((0.5, 0.1))
    plt.ylabel('Transaction Amount')
    plt.title('Branch with highest Healthcare Transaction in dollar')
    plt.xticks(rotation=45)
    plt.show()
    spark.stop()

#branch_high()

def mm_plot(): 
    query2= "(WITH total_applications AS \
        (SELECT Married, Gender, COUNT(*) AS total \
        FROM cdw_sapp_loan_application \
        WHERE Gender = 'Male' \
	    GROUP BY Married), \
        rejected_applications AS ( \
        SELECT Married, Gender, COUNT(*) AS SUM1 \
        FROM cdw_sapp_loan_application \
        WHERE application_status = 'N' AND Gender = 'Male' \
        GROUP BY Married) \
        SELECT rejected_applications.Married, rejected_applications.Gender, \
        round((rejected_applications.SUM1 / total_applications.total) * 100, 2) AS rejected_percentage \
        FROM total_applications \
        INNER JOIN rejected_applications ON rejected_applications.Married = total_applications.Married) as Reject_mm"

    sdf_mm = spark.read.format("jdbc").options(driver="com.mysql.cj.jdbc.Driver",\
                                     user="root",\
                                     password="password",\
                                     url="jdbc:mysql://localhost:3306/capstone_project",\
                                     dbtable=query2).load()

    #sdf_mm.show()
    pdf_mm = sdf_mm.toPandas()
    pdf_mm.set_index(['Married', 'Gender'], inplace = True)
    pdf_mm['rejected_percentage'].dtype
    pdf_mm['rejected_percentage'] = pdf_mm['rejected_percentage'].astype(float)
    #pdf_mm
    pdf_mm.plot(kind='bar', figsize=(10, 6), color = 'blue')
    plt.xticks(rotation = 45)
    xlabel = plt.xlabel('Male with Married(Yes/No)')
    #plt.xlabel('Male with Married(Yes/No) ')
    xlabel.set_position((0.5, -0.1))
    plt.ylabel('Percentage of Rejection')
    plt.title('Loan Rejection Percentage for Male Married Applicants')
    plt.show()
    spark.stop()

#mm_plot()

def self_emp():
    query3 = "(WITH    \
        total_applications AS (SELECT self_employed, COUNT(*) AS total \
         FROM cdw_sapp_loan_application \
        GROUP BY self_employed),  \
        approved_applications AS  \
        (SELECT self_employed, COUNT(*) AS SUM1 \
         FROM cdw_sapp_loan_application \
         WHERE application_status = 'Y' \
         GROUP BY self_employed)  \
       SELECT approved_applications.self_employed, \
       round((approved_applications.SUM1 / total_applications.total) * 100, 2) AS approval_percentage \
        FROM total_applications \
        INNER JOIN approved_applications ON approved_applications.self_employed = total_applications.self_employed) as self_e"

    sdf_selfe = spark.read.format("jdbc").options(driver="com.mysql.cj.jdbc.Driver",\
                                     user="root",\
                                     password="password",\
                                     url="jdbc:mysql://localhost:3306/capstone_project",\
                                     dbtable=query3).load()

    #sdf_selfe.show()
    #Convert spark dataframe to pandas dataframe
    pdf_selfe = sdf_selfe.toPandas()
    pdf_selfe.set_index('self_employed', inplace = True)
    print(pdf_selfe['approval_percentage'].dtype)
    pdf_selfe["approval_percentage"] = pdf_selfe["approval_percentage"].astype(float)
    #pdf_selfe
    pdf_selfe.plot(kind='bar', figsize=(10, 6), color = 'blue')
    plt.xlabel('Self_Employed')
    plt.ylabel('Approval Percentage')
    plt.title('Loan Approval Percentage for Self_Employed')
    plt.show()
    spark.stop()

#self_emp()

def top_trans():
    query4 = "(SELECT COUNT(transaction_id), MONTH FROM cdw_sapp_credit_card GROUP BY MONTH \
         ORDER BY COUNT(transaction_id) DESC LIMIT 3) as top_three"

    sdf_top = spark.read.format("jdbc").options(driver="com.mysql.cj.jdbc.Driver",\
                                     user="root",\
                                     password="password",\
                                     url="jdbc:mysql://localhost:3306/capstone_project",\
                                     dbtable=query4).load()

    #sdf_top.show()
    #Convert spark dataframe to pandas dataframe
    pdf_top = sdf_top.toPandas()
    pdf_top.set_index('MONTH', inplace = True)
    #pdf_top
    pdf_top.plot(kind='bar', figsize=(10, 6), color = 'blue')
    plt.xlabel('Months with High Transaction data')
    plt.ylabel('Number of transactions')
    plt.title('Top 3 months with high Transaction data')
    plt.show()
    spark.stop()

#top_trans()


