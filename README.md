# Transaction Analyser/Parser  

This program parse and analyse a csv file containing transactions records & report 
* The relative account balance
* Number of transactions


## Assumptions
* The cvs file contains millions of transactions
* Input file and records are all in a valid format
* Transaction are recorded in order


## Required
* Java 8/JDK 8

## Technology stack

Framework / library | Description
--------------------|------------
Java 8              | Java SE
Spark               | DataFrames/DataSet API
Gradle              | dependency management and build tool
Junit               | unit tests
Git                 | version control



## How to run the project

##### Using IDE
import to IntelliJ IDEA


or use terminal

##### Install dependencies
    
```    
$ ./gradlew clean build
```


##### Run unit test

```
$ ./gradlew test
```

##### Run :

```
$ ./gradlew run --args='{List of parameters}'
$ ./gradlew run --args='src/main/resources/transactions.csv ACC334455 "20/10/2018 12:00:00" "20/10/2018 19:00:00"'
```



