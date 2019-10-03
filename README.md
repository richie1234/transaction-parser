# HTTP Requests Parser  

This program parses a log file containing HTTP requests & report on its contents.

* The number of unique IP addresses
* The top 3 most visited URLs
* The top 3 most active IP addresses


## Assumptions
* The log file contains millions of rows
* URLs & IP addresses are ordered as strings

## Required
* Java 8/JDK 8

## Technology stack

Framework / library | Description
--------------------|------------
Java 8              | Java SE
Spark               | DataFrames API/SQL
Gradle              | dependency management and build tool
Junit               | unit tests
Git                 | version control



## How to run the project

##### Using IDE
import to IntelliJ IDEA


OR use terminal

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
$ ./gradlew run
$ ./gradlew run --args='{PATH To Log File}'
```



