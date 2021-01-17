# Learning

Minimal example in action for use client java of KSQLDB reading each sentence from files where you write you logical KSQL sentences.
This is an alternative to use KSQL API

# Content

 - You can compile your project and use it like a jar, changing the package in the pom.xml
 - environment/docker-compose.yml you have an enough environment for testing this project
 - You have the class OrdersProducer.java wichOne if you execute is producer with fake Orders<String, JSON> 
 - In resourcers you have 2 files .ksql into you have different ksql sentences
 - You have the class App.java wichOne if you execute is a main class who read the content of files of resources and send this statement to KSQL-SERVER using the java client ksqldb. 
# Execution
- Launch the environment
- Execute The producer
- Execute The App.java
- Enter in ksql-cli of environment with: `docker exec -it ksqldb-cli ksql http://ksqldb-server:8088`
- Show tables and show streams: `show streams; show tables;`
- Check the content of table: `select * from ORDERS_BY_USER emit changes;`

TODO - Testing all Source