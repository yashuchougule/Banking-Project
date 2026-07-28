# Databricks notebook source
import spark


spark

# COMMAND ----------

# MAGIC %md
# MAGIC **Read the data from silver layer into the delta format**

# COMMAND ----------

# MAGIC %md
# MAGIC **Read customer file from silver layer**

# COMMAND ----------

customer_gold_df = spark.read.format("delta").load("abfss://silver@bankstorageaccount01.dfs.core.windows.net/customers/2026-07-02/customers.delta")
display(customer_gold_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Apply Business Transformations on customer file**

# COMMAND ----------

from pyspark.sql.functions import *
customer_gold_df = customer_gold_df.withColumn("Customer_Tenure", round(months_between(current_date(), col("KYC_Date")) / 12, 1))\
    .withColumn("Active_KYC_Flag", when(col("KYC_Status")=="Verified", "Active").otherwise("Inactive"))\
    .withColumn("Customer_Location", concat_ws(", ", col("City"), col("State")))\
    .withColumn("Phone_Number", concat(lit("******"),substring(col("Customer_Phone"), 7, 4)))
    
display(customer_gold_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Read account file from silver layer**

# COMMAND ----------

account_gold_df = spark.read.format("delta").load("abfss://silver@bankstorageaccount01.dfs.core.windows.net/account/2026-07-02/account.delta")
display(account_gold_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Apply Business Transformations on account file**

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
account_gold_df = account_gold_df.withColumn("Account_Age", round(months_between(current_date(), col("Account_Open_Date")) / 12, 1))\
    .withColumn("Income_Category", when(col('Annual_Income')<=500000,'Low Income')
                .when((col("Annual_Income")>500000) & (col("Annual_Income")<=1200000),'Medium Income')
                .when(col("Annual_Income")>1200000,"High Income"))\
    .withColumn("Customer_Segment",when((col("Annual_Income") >= 1000000) &(col("Credit_Score") >= 750), "Premium"
    ).when((col("Annual_Income") >= 500000) &(col("Annual_Income") < 1000000) &(col("Credit_Score") >= 700),
    "Gold").otherwise("Regular"))\
    .withColumn("Has_Nominee_Flag",when(col("Nominee_Name").isNull() | (trim(col("Nominee_Name")) == ""),
    "No").otherwise("Yes"))\
    .withColumn("Credit_Score_Category", when(col("Credit_Score") > 790, "Excellent")
    .when((col("Credit_Score") >= 771) & (col("Credit_Score") <= 790), "Good")
    .when((col("Credit_Score") >= 731) & (col("Credit_Score") <= 770), "Fair")
    .when((col("Credit_Score") >= 681) & (col("Credit_Score") <= 730), "Average")
    .when(col("Credit_Score") < 681, "Poor"))
    
display(account_gold_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Read atm_pos file from silver layer**

# COMMAND ----------

atm_pos_gold_df = spark.read.format("delta").load("abfss://silver@bankstorageaccount01.dfs.core.windows.net/atm_pos/2026-07-02/atm_pos.delta")
display(atm_pos_gold_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Apply Business Transformations on atm_pos file**

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
atm_pos_gold_df = (atm_pos_gold_df.withColumn("High_Value_Flag",when(col("Amount") > 50000, 1).otherwise(0))\
    .withColumn("Failed_Transaction_Flag",when(col("Response_code") != "00", 1).otherwise(0))\
    .withColumn("Low_Balance_Alert",when(col("Available_Balance") < 1000, 1).otherwise(0))\
    .withColumn("Transaction_Hour",hour(col("Transaction_Time")))\
    .withColumn("Year",year(col("Transaction_Date")))\
    .withColumn("Month",month(col("Transaction_Date")))
    # Customer + Day summary
    .groupBy("Customer_Id","Transaction_Date","City","State","Device_Type")
                .agg(count("Transaction_Id").alias("Daily_Transaction_Count"),
                sum("Amount").alias("Daily_Transaction_Amount"),
                avg("Amount").alias("Average_Transaction_Amount"),
                sum(when(col("Transaction_Type") == "ATM Withdrawal",col("Amount")).otherwise(0)).alias("Total_ATM_Withdrawals"),
                sum(when(col("Transaction_Type") == "POS Purchase",col("Amount")).otherwise(0)).alias("Total_POS_Purchases"),
                sum("High_Value_Flag").alias("High_Value_Transactions"),
                sum("Failed_Transaction_Flag").alias("Failed_Transactions"),
                sum("Low_Balance_Alert").alias("Low_Balance_Alerts")))
                
display(atm_pos_gold_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Read credit_card file from silver layer**

# COMMAND ----------

credit_card_gold_df = spark.read.format("delta").load("abfss://silver@bankstorageaccount01.dfs.core.windows.net/credit_card/2026-07-02/credit_card.delta")
display(credit_card_gold_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Apply Business Transformations on Credit Card file**

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

credit_card_gold_df = (credit_card_gold_df.withColumn("Credit_Utilization_Pct",
round((col("Outstanding_Balance") / col("Credit_Limit")) * 100, 2))
.withColumn("High_Utilization_Flag",when(col("Credit_Utilization_Pct") >= 80, 1).otherwise(0))
.withColumn("Over_Limit_Flag",when(col("Outstanding_Balance") > col("Credit_Limit"), 1).otherwise(0))
.groupBy("Customer_Id").agg(sum("Credit_Limit").alias("Total_Credit_Limit"),
                        sum("Outstanding_Balance").alias("Total_Outstanding_Balance"),
                        sum("Available_Limit").alias("Total_Available_Limit"),
                        round(avg("Credit_Utilization_Pct"),2).alias("Avg_Credit_Utilization_Pct"),
                        sum("Reward_Points").alias("Total_Reward_Points"),
                        sum("Minimum_Due").alias("Total_Minimum_Due"),
                        sum(when(col("Card_Status")=="Active",1).otherwise(0)).alias("Active_Cards"),
                        sum("High_Utilization_Flag").alias("High_Utilization_Cards"),
                        sum("Over_Limit_Flag").alias("Over_Limit_Cards"),
                        max("Interest_Rate").alias("Max_Interest_Rate"),
                        count("Card_Number").alias("Total_Cards")))
                        
display(credit_card_gold_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Read Loan file from silver layer**

# COMMAND ----------

loan_gold_df = spark.read.format("delta").load("abfss://silver@bankstorageaccount01.dfs.core.windows.net/loan/2026-07-02/loan.delta")
display(loan_gold_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Apply Business Transformations on Loan file**

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

loan_gold_df =(loan_gold_df.withColumn("Loan_Age_Months",round(months_between(current_date(),col("Disbursement_Date")),0))
    .withColumn("Outstanding_Percentage",round((col("Outstanding_Amount") / col("Loan_Amount")) * 100,2))
    .withColumn("Paid_amount",col("Loan_Amount") - col("Outstanding_Amount"))
    .withColumn("Loan_Completion_Percentage",round((col("Paid_Amount") / col("Loan_Amount")) * 100,2))
    .withColumn("High_Value_Loan_Flag",when(col("Loan_Amount") >= 1000000,"Yes").otherwise("No"))
    .withColumn("Overdue_Flag",when(col("Next_Due_Date") < current_date(),"Yes").otherwise("No"))
    .withColumn( "Collateral_Status",when(col("Collateral_Flag")=="Yes","Secured Loan").otherwise("Unsecured Loan"))
    .withColumn("Credit_Score_Category",when(col("Credit_Score_At_Approval") >= 750,"Excellent")
        .when(col("Credit_Score_At_Approval") >= 650,"Good").otherwise("Poor"))
    .withColumn("Loan_Risk_Level",when((col("Risk_Category")=="High") |(col("Outstanding_Percentage") > 80),
    "High").when(col("Outstanding_Percentage") > 50,"Medium").otherwise("Low"))
    .withColumn("EMI_Category",when(col("EMI_Amount") >= 50000,"High EMI").when(col("EMI_Amount") >= 20000,"Medium EMI").otherwise("Low EMI")))
display(loan_gold_df)

loan_type_gold_df = loan_gold_df.groupBy("Loan_Type").agg(count("Loan_Id").alias("Total_Loans"),
                        sum("Loan_Amount").alias("Total_Loan_Amount"),
                        sum("Outstanding_Amount").alias("Total_Outstanding_Amount"),
                        avg("Loan_Interest_Rate").alias("Avg_Interest_Rate"))
display(loan_type_gold_df)

customer_type_gold_df = loan_gold_df.groupBy("Customer_Id").agg(count("Loan_Id").alias("Number_Of_Loans"),
                            sum("Loan_Amount").alias("Total_Loan_Amount"),
                            sum("Outstanding_Amount").alias("Total_Outstanding"),
                            avg("Credit_Score_At_Approval").alias("Avg_Credit_Score"))
display(customer_type_gold_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Read KYC File from silver layer**

# COMMAND ----------

kyc_gold_df = spark.read.format("delta").load("abfss://silver@bankstorageaccount01.dfs.core.windows.net/kyc/2026-07-02/kyc.delta")
display(kyc_gold_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Apply Business Transformations on KYC file**

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

kyc_gold_df = (kyc_gold_df.withColumn("Document_Age_Months",months_between(current_date(),col("Issue_Date")
).cast("int"))
.withColumn("KYC_Expiry_Flag",when(col("Expiry_Date") < current_date(),"Expired").otherwise("Active"))
.withColumn("Days_To_Expiry",datediff(col("Expiry_Date"),current_date()))
.withColumn("KYC_Validity_Status",when(col("Expiry_Date") < current_date(),"Invalid").when(datediff(col("Expiry_Date"), current_date()) <= 30,"Expiring Soon").otherwise("Valid"))
.withColumn("Verified_Customer_Flag",when((col("Verification_Status") == "Verified") &(col("Address_Match") == "Y") &(col("Name_Match") == "Y"),"Yes").otherwise("No"))
.withColumn("Address_Verification_Flag",when(col("Address_Match") == "Y","Matched").otherwise("Not Matched"))
.withColumn("Name_Verification_Flag",when(col("Name_Match") == "Y","Matched").otherwise("Not Matched"))
.withColumn("KYC_Risk_Category",when((col("Verification_Status") != "Verified") |(col("Address_Match") != "Y") |(col("Name_Match") != "Y"),"High Risk")
.when(datediff(col("Expiry_Date"), current_date()) <= 30,"Medium Risk").otherwise("Low Risk"))
.withColumn("Masked_Document_Number",when(col("Document_Number").isNotNull(),concat(lit("XXXXXX"),substring(col("Document_Number"), -4, 4)))))
display(kyc_gold_df)

# kyc document summary
kyc_status_summary_df = kyc_gold_df.groupBy("Verification_Status").agg(count("Customer_Id").alias("Customer_Count"))    
display(kyc_status_summary_df)
# kyc document summary
kyc_document_summary_df = kyc_gold_df.groupBy("Document_Type").agg(count("KYC_Id").alias("Total_Documents"))
display(kyc_document_summary_df)
# kyc risk category summary
kyc_risk_category_summary= kyc_gold_df.groupBy("KYC_Risk_Category").agg(count("Customer_Id").alias("Customer_Count"))



# COMMAND ----------

# MAGIC %md
# MAGIC **Read digital_logs file from silver layer**

# COMMAND ----------

digital_logs_gold_df = spark.read.format("delta").load("abfss://silver@bankstorageaccount01.dfs.core.windows.net/digital_logs/2026-07-02/digital_logs.delta")
display(digital_logs_gold_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Apply Business Transformations on digital_logs file**

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

digital_logs_gold_df = (digital_logs_gold_df.withColumn("Session_Duration_Minutes",(unix_timestamp(col("Logout_Time")) - unix_timestamp(col("Login_Time"))) / 60)
.withColumn("Login_Hour",hour(col("Login_Time")))
.withColumn("Login_Success_Flag",when(col("Status") == "Success", 1).otherwise(0))
.withColumn("Failed_Login_Flag",when(col("Status") == "Failed", 1).otherwise(0))
.withColumn("Suspicious_Login_Flag",when((col("Status") == "Failed") |(col("Failure_Reason").isNotNull()),
"Yes").otherwise("No"))
.withColumn("Device_Category",when(col("Device_Type").isin("Mobile", "Tablet"),"Mobile Device")
.when(col("Device_Type") == "Desktop","Desktop").otherwise("Other"))
.withColumn("Browser_Category",when(col("Browser").isin("Chrome","Edge","Firefox","Safari"),"Supported Browser")
.otherwise("Unknown Browser"))
.withColumn("Failure_Category",when(col("Failure_Reason").isNull(),"No Failure").otherwise("Authentication Failure")))
display(digital_logs_gold_df)

customer_login_summary_df = digital_logs_gold_df.groupBy("Customer_Id").agg(count("Session_Id").alias("Total_Sessions"),
                            sum("Login_Success_Flag").alias("Successful_Logins"),
                            sum("Failed_Login_Flag").alias("Failed_logins"),
                            avg("Session_Duration_Minutes").alias("Avg_Session_Duration"))
display(customer_login_summary_df)

device_usage_summary_df = digital_logs_gold_df.groupBy("Device_Type").agg(count("Customer_Id").alias("Total_Users")).orderBy(col("Total_Users").desc())
display(device_usage_summary_df)

location_wise_summary_df = digital_logs_gold_df.groupBy("City").agg(count("Session_Id").alias("Total_Logins"))

peak_login_hour_df = digital_logs_gold_df.groupBy("Login_Hour").agg(count("Session_Id").alias("Login_Count")).orderBy(col("Login_Count").desc())
display(peak_login_hour_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Read fraud file from silver layer**

# COMMAND ----------

fraud_gold_df = spark.read.format("delta").load("abfss://silver@bankstorageaccount01.dfs.core.windows.net/fraud/2026-07-02/fraud.delta")
display(fraud_gold_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Apply Business Transformations on Fraud file**

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import * 

fraud_gold_df =(fraud_gold_df.withColumn("Fraud_Risk_Category",when(col("Fraud_Score") >= 80, "High Risk")
                                        .when(col("fraud_score") >= 50, "Medium Risk").otherwise("Low Risk"))

.withColumn("High_Risk_Fraud_Flag",when(col("Fraud_Score") >= 80, "Yes").otherwise("No"))
.withColumn("High_Amount_Transaction_Flag",when(col("Amount") >= 50000, "Yes").otherwise("No"))
.withColumn("Amount_Category",when(col("Amount") >= 100000, "Very High").when(col("Amount") >= 50000, "High")
                                .when(col("Amount") >= 10000, "Medium").otherwise("Low"))
.withColumn("Fraud_Score_Band",when(col("Fraud_Score") >= 90, "Critical").when(col("Fraud_Score") >= 70, "High")
                                .when(col("Fraud_Score") >= 40, "Medium").otherwise("Low")))
display(fraud_gold_df)

customer_fraud_summary_df= fraud_gold_df.groupBy("Customer_Id").agg(count("Event_Id").alias("Total_Fraud_Events"),
                            sum("Amount").alias("Total_Fraud_Amount"),
                            avg("Fraud_Score").alias("Avg_Fraud_Score"),
                            sum(when(col("High_Risk_Fraud_Flag") == "Yes",1).otherwise(0)).alias("High_Risk_Events"))
display(customer_fraud_summary_df)

device_fraud_summary_df= fraud_gold_df.groupBy("Device_Id").agg(count("Event_Id").alias("Fraud_Count"),
            sum("Amount").alias("Total_Fraud_Amount")).orderBy(col("Fraud_Count").desc())
display(device_fraud_summary_df)

ip_fraud_summary_df = fraud_gold_df.groupBy("IP").agg(count("Event_Id").alias("Fraud_Attempts"),
                avg("Fraud_Score").alias("Average_Fraud_Score")).orderBy(col("Fraud_Attempts").desc())
display(ip_fraud_summary_df)

daily_fraud_summary_df = fraud_gold_df.groupBy("Date").agg(count("Event_Id").alias("Fraud_Events"),
                sum("Amount").alias("Fraud_Amount"),
                avg("Fraud_Score").alias("Avg_Fraud_Score"))
display(daily_fraud_summary_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **1) Customer 360 Analytics**

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

customer_360_df = customer_gold_df.join(account_gold_df, 'Customer_Id', 'left')\
                                .join(atm_pos_gold_df, 'Customer_Id', 'left')\
                                .join(credit_card_gold_df, 'Customer_Id', 'left')\
                                .join(loan_gold_df, 'Customer_Id', 'left')\
                                .join(kyc_gold_df, 'Customer_Id', 'left')\
                                .join(digital_logs_gold_df, 'Customer_Id', 'left')\
                                .join(fraud_gold_df, 'Customer_Id', 'left')
display(customer_360_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **2) Account Analytics Table**

# COMMAND ----------

account_analytics = account_gold_df.join(customer_gold_df, 'Customer_Id', 'left')
display(account_analytics)

# COMMAND ----------

# MAGIC %md
# MAGIC **3) Transaction Analytics**

# COMMAND ----------

transcation_analytics = customer_gold_df.join(atm_pos_gold_df, 'Customer_Id', 'left')\
                                .join(credit_card_gold_df, 'Customer_Id', 'left')
display(transcation_analytics)

# COMMAND ----------

# MAGIC %md
# MAGIC **4) Loan Analytics**

# COMMAND ----------

loan_analytics = loan_gold_df.join(customer_gold_df, 'Customer_Id', 'left')
display(loan_analytics)

# COMMAND ----------

# MAGIC %md
# MAGIC **5) KYC Analytics**

# COMMAND ----------

kyc_analytics = kyc_gold_df.join(customer_gold_df, 'Customer_Id', 'left')
display(kyc_analytics)

# COMMAND ----------

# MAGIC %md
# MAGIC **6) Fraud Analytics**

# COMMAND ----------

fraud_analytics = fraud_gold_df.join(digital_logs_gold_df, 'Customer_Id', 'left')\
                               .join(atm_pos_gold_df, 'Customer_Id', 'left')
display(fraud_analytics)

# COMMAND ----------

# MAGIC %md
# MAGIC **Write all files into Delta Format in gold layer**

# COMMAND ----------

customer_gold_df.write.format('delta').mode('overwrite').save('abfss://gold@bankstorageaccount01.dfs.core.windows.net/customer')

# COMMAND ----------

account_gold_df.write.format('delta').mode('overwrite').save('abfss://gold@bankstorageaccount01.dfs.core.windows.net/account')
atm_pos_gold_df.write.format('delta').mode('overwrite').save('abfss://gold@bankstorageaccount01.dfs.core.windows.net/atm_pos')
credit_card_gold_df.write.format('delta').mode('overwrite').save('abfss://gold@bankstorageaccount01.dfs.core.windows.net/credit_card')
loan_gold_df.write.format('delta').mode('overwrite').save('abfss://gold@bankstorageaccount01.dfs.core.windows.net/loan')
kyc_gold_df.write.format('delta').mode('overwrite').save('abfss://gold@bankstorageaccount01.dfs.core.windows.net/kyc')
digital_logs_gold_df.write.format('delta').mode('overwrite').save('abfss://gold@bankstorageaccount01.dfs.core.windows.net/digital_logs')
fraud_gold_df.write.format('delta').mode('overwrite').save('abfss://gold@bankstorageaccount01.dfs.core.windows.net/fraud')

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC create table gold.customer
# MAGIC using delta
# MAGIC location 'abfss://gold@bankstorageaccount01.dfs.core.windows.net/customer'

# COMMAND ----------

# MAGIC %sql
# MAGIC create table gold.account 
# MAGIC using delta 
# MAGIC location 'abfss://gold@bankstorageaccount01.dfs.core.windows.net/account'

# COMMAND ----------

# MAGIC %sql
# MAGIC create table gold.atm_pos 
# MAGIC using delta 
# MAGIC location 'abfss://gold@bankstorageaccount01.dfs.core.windows.net/atm_pos'

# COMMAND ----------

# MAGIC %sql
# MAGIC create table gold.credit_card 
# MAGIC using delta 
# MAGIC location 'abfss://gold@bankstorageaccount01.dfs.core.windows.net/credit_card'

# COMMAND ----------

# MAGIC %sql
# MAGIC create table gold.loan 
# MAGIC using delta 
# MAGIC location 'abfss://gold@bankstorageaccount01.dfs.core.windows.net/loan'

# COMMAND ----------

# MAGIC %sql
# MAGIC create table gold.kyc 
# MAGIC using delta 
# MAGIC location 'abfss://gold@bankstorageaccount01.dfs.core.windows.net/kyc'

# COMMAND ----------

# MAGIC %sql
# MAGIC create table gold.digital_logs 
# MAGIC using delta 
# MAGIC location 'abfss://gold@bankstorageaccount01.dfs.core.windows.net/digital_logs'

# COMMAND ----------

# MAGIC %sql
# MAGIC create table gold.fraud 
# MAGIC using delta 
# MAGIC location 'abfss://gold@bankstorageaccount01.dfs.core.windows.net/fraud'