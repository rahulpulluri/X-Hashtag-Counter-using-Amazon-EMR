<<<<<<< HEAD
# X-Hashtag-Counter-using-Amazon-EMR
=======
# X-Hashtag-Counter-Using-Amazon-EMR
-**Introduction**
This project aims to analyze a collection of tweets stored in an S3 bucket on AWS and extract the top 20 hashtags present in the dataset. By leveraging the scalability and processing power of Amazon EMR (Elastic MapReduce), we can efficiently process large volumes of data to gain valuable insights into the prevalent topics and discussions on Twitter.

-**Data Processing Tool**
Apache Spark is a multi-language engine for executing data engineering, data science, and
machine learning on single-node machines or clusters. PySpark is the Python API for Apache Spark.
Apache Spark is an open source tool for processing big data workloads.

-**Advantages of Apache Spark:**
1. Fast: It uses in-memory caching and has an optimized query execution for data of any size.
2. Easy to use: Apache Spark is developer friendly and with support for multiple programming
languages, developers can make use of this powerful data processing tool.
3. Handles multiple workloads.

-**Data Processing Steps:** 
1.	Read JSON file as a dataframe in PySpark.
2.	Study the schema of the JSON file and find that there are 3 fields of text where hashtags can be found. Fields are: ["doc.text", "value.properties.text", and “doc.user.description"].
3.	To extract the hashtags we will filter the words in the text field with help of PySpark functions of explode, split, col etc. 
4.	The explode function will generate a new row for each element in the given array or map which is created using the split function.
5.	For data cleaning we split the words on spaces, punctuation marks such as "[\\s,:!*.]+". This ensures that hashtags like #melbourne, #melbourne:, #melbourne*, #melbourne, are all counted as #melbourne.
6.	The case of the hashtag words has been handled by converting all the hashtag words to the lower case to ensure #Melbourne is counted as #melbourne. 
7.	The dataset is read from the AWS S3 bucket (p2-inputdata) using the S3 URI and the processing steps are executed. 
8.	Finally we have the S3 URI for the cloudcomputingvt bucket with the folder marked with our name. The output of the top 20 hashtag words have been written to a CSV file in that folder.



   

>>>>>>> 1ce427a (dev commit)
