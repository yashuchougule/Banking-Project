# Databricks notebook source
import spark
spark

# COMMAND ----------

spark.conf.get("spark.databricks.clusterUsageTags.sparkVersion")

# COMMAND ----------

# MAGIC %md
# MAGIC **Check if bronze, silver & gold layer access to databricks through unity catalog external location, external credential and access connector**

# COMMAND ----------

dbutils.fs.ls("abfss://bronze@bankstorageaccount01.dfs.core.windows.net/")

# COMMAND ----------

dbutils.fs.ls("abfss://silver@bankstorageaccount01.dfs.core.windows.net/")

# COMMAND ----------

dbutils.fs.ls("abfss://gold@bankstorageaccount01.dfs.core.windows.net/")

# COMMAND ----------

# MAGIC %md
# MAGIC **Reading All Files from Bronze Layer**

# COMMAND ----------

# MAGIC %md
# MAGIC **Read customer File from Azure SQL**

# COMMAND ----------

df1 = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("abfss://bronze@bankstorageaccount01.dfs.core.windows.net/customers/2026-06-19/customers_2026-06-19.csv")
display(df1)

# COMMAND ----------

# MAGIC %md
# MAGIC **Read account file from Azure SQL**

# COMMAND ----------

df2 = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("abfss://bronze@bankstorageaccount01.dfs.core.windows.net/accounts/2026-06-22/accounts_2026_06_22.csv")
display(df2)

# COMMAND ----------

# MAGIC %md
# MAGIC **Read ATM File Data from on-prem SQL**

# COMMAND ----------

df3 = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("abfss://bronze@bankstorageaccount01.dfs.core.windows.net/atm_pos/2026-06-19/atm_pos_2026-06-19.csv")
display(df3)

# COMMAND ----------

# MAGIC %md
# MAGIC **Read credit_card file from on-prem SQL**

# COMMAND ----------

df4 = spark.read.format("csv")\
    .option('header', 'true')\
    .option('inferschema', 'true')\
    .load('abfss://bronze@bankstorageaccount01.dfs.core.windows.net/credit_card/2026-06-23/credit_card_2026_06_23.csv')
display(df4)

# COMMAND ----------

# MAGIC %md
# MAGIC **Read Loan File from Azure SQL**

# COMMAND ----------

df5 = spark.read.format('csv')\
    .option('header', 'true')\
    .option('inferschema', 'true')\
    .load('abfss://bronze@bankstorageaccount01.dfs.core.windows.net/loans/2026-06-23/loans_2026_06_23.csv')
display(df5)

# COMMAND ----------

# MAGIC %md
# MAGIC **Read kyc file from ADLS raw to bronze container**

# COMMAND ----------

df6 = spark.read.format('csv')\
    .option('header', 'true')\
    .option('inferschema', 'true')\
    .load('abfss://bronze@bankstorageaccount01.dfs.core.windows.net/kyc/2026-06-23/kyc_2026_06_23.csv')
display(df6)

# COMMAND ----------

# MAGIC %md
# MAGIC **Read digital_logs files as flat files**

# COMMAND ----------

df7 = spark.read.format('csv')\
    .option('header', 'true')\
    .option('inferschema', 'true')\
    .load('abfss://bronze@bankstorageaccount01.dfs.core.windows.net/digital_logs/2026-06-17/digital_logs_2026-06-17.csv')
display(df7)

# COMMAND ----------

# MAGIC %md
# MAGIC **Read fraud.json file but read in csv file format**

# COMMAND ----------

df8 = spark.read.format('csv')\
    .option('header', 'true')\
    .option('inferschema', 'true')\
    .load('abfss://bronze@bankstorageaccount01.dfs.core.windows.net/fraud/2026-06-17/fraud_2026-06-17.json')
display(df8)

# COMMAND ----------

# MAGIC %md
# MAGIC **Remove Duplicate Records from all files**

# COMMAND ----------

df1 = df1.dropDuplicates(['customer_id'])
df2 = df2.dropDuplicates(['account_no'])
df3 = df3.dropDuplicates(['transaction_id'])
df4 = df4.dropDuplicates(['card_number'])
df5 = df5.dropDuplicates(['loan_id'])
df6 = df6.dropDuplicates(['kyc_id'])
df7 = df7.dropDuplicates(['log_id'])
df8 = df8.dropDuplicates(['event_id'])

# COMMAND ----------

# MAGIC %md
# MAGIC **Rename Columns**

# COMMAND ----------

rename_df1 = df1.withColumnRenamed('customer_id', 'Customer_Id')\
    .withColumnRenamed('full_name', 'Customer_Name')\
    .withColumnRenamed('gender', 'Customer_Gender')\
    .withColumnRenamed('dob', 'Customer_DOB')\
    .withColumnRenamed('age', 'Customer_Age')\
    .withColumnRenamed('email', 'Customer_Email')\
    .withColumnRenamed('phone', 'Customer_Phone')\
    .withColumnRenamed('address', 'Customer_Address')\
    .withColumnRenamed('city', 'City')\
    .withColumnRenamed('state', 'State')\
    .withColumnRenamed('pincode', 'Pincode')\
    .withColumnRenamed('kyc_status', 'KYC_Status')\
    .withColumnRenamed('kyc_date', 'KYC_Date')
display(rename_df1)

# COMMAND ----------

rename_df2 = df2.withColumnRenamed("account_no", "Account_Number")\
    .withColumnRenamed("customer_id", "Customer_Id")\
    .withColumnRenamed("account_type", "Account_Type")\
    .withColumnRenamed("account_open_date", "Account_Open_Date")\
    .withColumnRenamed("account_status", "Account_Status")\
    .withColumnRenamed("nominee_name", "Nominee_Name")\
    .withColumnRenamed("nominee_relation", "Nominee_Relation")\
    .withColumnRenamed("occupation", "Occupation")\
    .withColumnRenamed("annual_income", "Annual_Income")\
    .withColumnRenamed("credit_score", "Credit_Score")
display(rename_df2)

rename_df3 = df3.withColumnRenamed("transaction_id", "Transaction_Id")\
    .withColumnRenamed("customer_id", "Customer_Id")\
    .withColumnRenamed("transaction_type", "Transaction_Type")\
    .withColumnRenamed("amount", "Amount")\
    .withColumnRenamed("transaction_date", "Transaction_Date")\
    .withColumnRenamed("transaction_time", "Transaction_Time")\
    .withColumnRenamed("merchant_name", "Merchant_Name")\
    .withColumnRenamed("merchant_category", "Merchant_Category")\
    .withColumnRenamed("atm_id", "ATM_Id")\
    .withColumnRenamed("terminal_id", "Terminal_Id")\
    .withColumnRenamed("city", "City")\
    .withColumnRenamed("state", "State")\
    .withColumnRenamed("response_code", "Response_Code")\
    .withColumnRenamed("available_balance", "Available_Balance")\
    .withColumnRenamed("device_type", "Device_Type")
display(rename_df3)

rename_df4 = df4.withColumnRenamed("card_number", "Card_Number")\
    .withColumnRenamed("customer_id", "Customer_Id")\
    .withColumnRenamed("card_type", "Card_Type")\
    .withColumnRenamed("credit_limit", "Credit_Limit")\
    .withColumnRenamed("available_limit", "Available_Limit")\
    .withColumnRenamed("outstanding_balance", "Outstanding_Balance")\
    .withColumnRenamed("minimum_due", "Minimum_Due")\
    .withColumnRenamed("billing_start", "Billing_Start")\
    .withColumnRenamed("billing_end", "Billing_End")\
    .withColumnRenamed("due_date", "Due_Date")\
    .withColumnRenamed("interest_rate", "Interest_Rate")\
    .withColumnRenamed("card_status", "Card_Status")\
    .withColumnRenamed("reward_points", "Reward_Points")
display(rename_df4)

rename_df5 = df5.withColumnRenamed("loan_id", "Loan_Id")\
    .withColumnRenamed("customer_id", "Customer_Id")\
    .withColumnRenamed("loan_type", "Loan_Type")\
    .withColumnRenamed("loan_amount", "Loan_Amount")\
    .withColumnRenamed("interest_rate", "Loan_Interest_Rate")\
    .withColumnRenamed("loan_tenure_months", "Loan_Tenure_Months")\
    .withColumnRenamed("emi_amount", "EMI_Amount")\
    .withColumnRenamed("disbursement_date", "Disbursement_Date")\
    .withColumnRenamed("loan_status", "Loan_Status")\
    .withColumnRenamed("outstanding_amount", "Outstanding_Amount")\
    .withColumnRenamed("next_due_date", "Next_Due_Date")\
    .withColumnRenamed("payment_frequency", "Payment_Frequency")\
    .withColumnRenamed("collateral_flag", "Collateral_Flag")\
    .withColumnRenamed("credit_score_at_approval", "Credit_Score_At_Approval")\
    .withColumnRenamed("risk_category", "Risk_Category")
display(rename_df5)

rename_df6 = df6.withColumnRenamed("kyc_id", "KYC_Id")\
    .withColumnRenamed("customer_id", "Customer_Id")\
    .withColumnRenamed("document_type", "Document_Type")\
    .withColumnRenamed("document_number", "Document_Number")\
    .withColumnRenamed("Issue_Date", "Issue_Date")\
    .withColumnRenamed("Expiry_Date", "Expiry_Date")\
    .withColumnRenamed("verification_status", "Verification_Status")\
    .withColumnRenamed("verification_method", "Verification_Method")\
    .withColumnRenamed("uploaded_date", "Uploaded_Date")\
    .withColumnRenamed("address_match", "Address_Match")\
    .withColumnRenamed("name_match", "Name_Match")      
display(rename_df6)

rename_df7 = df7.withColumnRenamed("log_id", "Log_Id")\
    .withColumnRenamed("customer_id", "Customer_Id")\
    .withColumnRenamed("session_id", "Session_Id")\
    .withColumnRenamed("login_time", "Login_Time")\
    .withColumnRenamed("logout_time", "Logout_Time")\
    .withColumnRenamed("ip_address", "Ip_Address")\
    .withColumnRenamed("device_type", "Device_Type")\
    .withColumnRenamed("os", "OS")\
    .withColumnRenamed("browser", "Browser")\
    .withColumnRenamed("action_type", "Action_Type")\
    .withColumnRenamed("status", "Status")\
    .withColumnRenamed("city", "City")\
    .withColumnRenamed("failure_reason", "Failure_Reason")
display(rename_df7)

rename_df8 = df8.withColumnRenamed("event_id", "Event_Id")\
    .withColumnRenamed("customer_id", "Customer_Id")\
    .withColumnRenamed("transaction_id", "Transaction_Id")\
    .withColumnRenamed("event_type", "Event_Type")\
    .withColumnRenamed("amount", "Amount")\
    .withColumnRenamed("device_id", "Device_Id")\
    .withColumnRenamed("ip", "IP")\
    .withColumnRenamed("fraud_score", "Fraud_Score")\
    .withColumnRenamed("timestamp", "Date")
display(rename_df8)

# COMMAND ----------

# MAGIC %md
# MAGIC **Convert timestamp to date column**

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

customer_df = rename_df1.withColumn("Customer_DOB",to_date("Customer_DOB", "yyyy-MM-dd"))\
    .withColumn("KYC_Date", to_date("KYC_Date", "yyyy-MM-dd"))
display(customer_df)

account_df = rename_df2.withColumn("Account_Open_Date", to_date("Account_Open_Date", "yyyy-MM-dd"))
display(account_df)

atm_df = rename_df3.withColumn("Transaction_Date", to_date("Transaction_Date", "yyyy-MM-dd"))
display(atm_df)

credit_card_df = rename_df4.withColumn("Billing_Start", to_date("Billing_Start", "yyyy-MM-dd"))\
    .withColumn("Billing_End", to_date("Billing_End", "yyyy-MM-dd"))\
    .withColumn("Due_Date", to_date("Due_Date", "yyyy-MM-dd"))
display(credit_card_df)

loan_df = rename_df5.withColumn("Disbursement_Date", to_date("Disbursement_Date","yyyy-MM-dd"))\
    .withColumn("Next_Due_Date", to_date("Next_Due_Date", "yyyy-MM-dd"))
display(loan_df)

kyc_df = rename_df6.withColumn("Issue_Date", to_date("Issue_Date", "yyyy-MM-dd"))\
    .withColumn("Expiry_Date", to_date("Expiry_Date", "yyyy-MM-dd"))
display(kyc_df)

digital_logs_df = rename_df7.withColumn("Login_Time", to_timestamp("Login_Time", "yyyy-MM-dd HH:mm:ss"))\
    .withColumn("Logout_Time", to_timestamp("Logout_Time", "yyyy-MM-dd HH:mm:ss"))
display(digital_logs_df)

fraud_df = rename_df8.withColumn("Date", to_date("Date", "yyyy-MM-dd"))
display(fraud_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Standardize text values -**
# MAGIC **Gender wise set the customer_name,**
# MAGIC **if Pooja is male then set to Female same for Male**
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import *
customer_df = customer_df.withColumn(
    "Customer_Gender",
    when(split(col("Customer_Name"), " ")[0].isin(
        "Aarav","Vivaan","Aditya","Arjun","Rohan","Rahul","Yash","Ishaan","Kabir","Shaurya","Reyansh","Siddharth","Devansh"
    ), "Male")
    .when(split(col("Customer_Name"), " ")[0].isin(
        "Priya","Neha","Kavya","Isha","Ananya","Saanvi","Diya","Aanya","Myra","Anika","Aarohi","Kiara"
    ), "Female")
    .otherwise(col("Customer_Gender"))
)
display(customer_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Convert Uppercase -> Proper Case Title Case for each file**

# COMMAND ----------

# MAGIC %md
# MAGIC **For ATM File**

# COMMAND ----------

from pyspark.sql.functions import *
atm_df = atm_df.withColumn("Transaction_Type",regexp_replace(regexp_replace(initcap(regexp_replace(col("Transaction_Type"), "_", " ")),
        "Atm", "ATM"), "Pos", "POS"))\
    .withColumn("Response_Code", initcap(col("Response_Code")))
display(atm_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **For credit_card file**

# COMMAND ----------

credit_card_df = credit_card_df.withColumn("Card_Type", initcap(col("Card_Type")))\
    .withColumn("Card_Status", initcap(col("Card_Status")))
display(credit_card_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **For loan file**

# COMMAND ----------

loan_df = loan_df.withColumn("Loan_Type", initcap(col("Loan_Type")))\
    .withColumn("Loan_Status", initcap(col("Loan_Status")))\
    .withColumn("Payment_Frequency", initcap(col("Payment_Frequency")))\
    .withColumn("Collateral_Flag", initcap(col("Collateral_Flag")))\
    .withColumn("Risk_Category", initcap(col("Risk_Category")))
display(loan_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **For KYC file**

# COMMAND ----------

kyc_df = kyc_df.withColumn("Document_Type", initcap(col("Document_Type")))\
    .withColumn("Verification_Status", initcap(col("Verification_Status")))\
    .withColumn("Verification_Method", initcap(col("Verification_Method")))\
    .withColumn("Address_Match", initcap(col("Address_Match")))\
    .withColumn("Name_Match", initcap(col("Name_Match")))
display(kyc_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **For digital_logs file**

# COMMAND ----------

digital_logs_df = digital_logs_df.withColumn("Action_Type", initcap(col("Action_Type")))\
    .withColumn("Status", initcap(col("Status")))
display(digital_logs_df)

# COMMAND ----------

display(customer_df)
display(account_df)
display(atm_df)
display(credit_card_df)
display(loan_df)
display(kyc_df)
display(digital_logs_df)
display(fraud_df)

# COMMAND ----------

display(digital_logs_df)

# COMMAND ----------

digital_logs_df = digital_logs_df.withColumn("Action_Type", initcap(col("Action_Type")))\
    .withColumn("Status", initcap(col("Status")))
display(digital_logs_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Write all files data into delta format in silver layer**

# COMMAND ----------

from datetime import datetime

# Current date
load_date = datetime.now().strftime("%Y-%m-%d")

# Silver container path
silver_path = "abfss://silver@bankstorageaccount01.dfs.core.windows.net/"

# DataFrames
tables = {
    "customers": customer_df,
    "account": account_df,
    "atm_pos": atm_df,
    "credit_card": credit_card_df,
    "loan": loan_df,
    "kyc": kyc_df,
    "digital_logs": digital_logs_df,
    "fraud": fraud_df
}

# Write DataFrames to ADLS in Delta format
for table_name, silver_df in tables.items():

    output_path = f"{silver_path}/{table_name}/{load_date}/{table_name}.delta"

    silver_df.write.format("delta").mode("overwrite").save(output_path)

    print(f"{table_name} written successfully to {output_path}")
