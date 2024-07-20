Cloud SQL, BigTable, Cloud Storage

streaming data to Google Cloud
data in GCS bucket -> schema validation -> put into BigQuery

how to identify 
how do 



——————

hive tables: SRC, TAR

accountid
SRC_col1
SRC_col2

accountid
TAR_col1
TAR_col2

df1 = spark.sql(“
select count(1) as no_records, count(distinct accountid) as no_dt_acctid
from SRC“)
df1.createOrReplacetempview(”temp_table”)

select count(1) as no_records, count(distinct accountid) as no_dt_acctid
from TAR


select
SRC.accountid,
case when SRC_col1 <> TAR_col1 then 1 else 0 end as col1_mismatch_tag,
case when SRC_col2 <> TAR_col2 then 1 else 0 end as col2_mismatch_tag,
from SRC
inner join TAR
on SRC. accountid = TAR. accountid


— Avl in SRC, not in TAR
select SRC.*
from SRC
left join TAR
on SRC.accountid = TAR.accountid
where TAR.accountid is null

semi join and anti join 


— Spark Submit
executor = 5
cores = 3
executor_memory = 64
driver_memory = 32


gcloud dataproc jobs submit pyspark —cluster=cluster_name  —executor_cores=5 —driver_cores=3 —executor_memory=64 —driver_memory=32 pysparkfilename.py

pandas
numpy
pyspark
sys

generators


when _-init__ == “main”:



exception handling

try:
	x = int(“2”)
except:
	print(“Some error occurred”)
else:
	print(1)

a_list = [1,3,5]
[i for i in a_list]


my_dict = {‘key1’:”value1”, “key2”: “value2”}

for i in my_dict.values():
	print(i)


value1
value2


## Other Questions I could not answer:
@bind, what is that called? => Python decorator, what does it do?
How to implement security features within Google Cloud? What are the different security features?
semi join vs anti join 
best way of ingesting streaming data to Google Cloud
What are generators? What are generators used for?
What are the use-cases of BigQuery?
What are the use-cases of service accounts in Google Cloud?
.tfstate file in Terraform - What is the purpose?
Where do we execute Terraform commands?
What do you mean by Broadcast join? Write code demonstrating a Broadcast join? Give a few use-cases of broadcast joins
What is __init__?
Output from PubSub consists of duplicates. How duplicates are removed/handled?
Difference between SparkContext and SparkSession
Specify spark-submit command and gcloud dataproc command to include driver/executor cores & memory
Difference between pandas and numpy and why are they created differently?
How to install missing Python packages using Airflow/Cloud Composer (probably using Python operator)
What is polymorphism?