# Databricks notebook source
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

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.customer;   

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.account;    

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.atm_pos;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.credit_card;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.loan;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.kyc;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.digital_logs;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.fraud

# COMMAND ----------

# MAGIC %md
# MAGIC **optimize command on each table**

# COMMAND ----------

# MAGIC %sql
# MAGIC optimize gold.customer;
# MAGIC     
# MAGIC optimize gold.account;
# MAGIC     
# MAGIC optimize gold.atm_pos;
# MAGIC     
# MAGIC optimize gold.credit_card;
# MAGIC     
# MAGIC optimize gold.loan;
# MAGIC     
# MAGIC optimize gold.kyc;
# MAGIC     
# MAGIC optimize gold.digital_logs;
# MAGIC
# MAGIC optimize gold.fraud;

# COMMAND ----------

# MAGIC %md
# MAGIC **Describe History of each Table**

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history gold.customer;

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history gold.account;

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history gold.atm_pos

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history gold.credit_card;

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history gold.loan

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history gold.kyc

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history gold.digital_logs

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history gold.fraud