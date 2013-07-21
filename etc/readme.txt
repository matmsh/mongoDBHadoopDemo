The code in this maven project is a clone of 

https://github.com/mongodb/mongo-hadoop/tree/master/examples/treasury_yield

which uses Hadoop 1.0.4. 



The mongo-hadoop connector jar is built following the instruction in 
https://github.com/mongodb/mongo-hadoop

with 
hadoopRelease in ThisBuild := "1.0"
git checkout release-1.0 (The branch release 1.0 is used.)


Steps to load data into MongoDB  : 

1) Run mongod in authentication  mode. In mongo, creat a db mongo_hadoop, and a user foo/foo123 for it.

2)  To import  yield_historical_in.json into MongoDB : 
     mongoimport   --username foo --password foo123    --db mongo_hadoop  --collection  yield_historical.in  --file   yield_historical_in.json
 
 3) In mongo, in mango_hadoop, add user foo with password foo123.
 
 
 4) Run TreasuryYieldXMLV3.java.
 
 