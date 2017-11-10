# Backend of the search engine

> Java

## What contains:

# All the source code of java is in ./code

# A sampe source data which contains pages content of plain text is in ./wet

# The indexed data structure of the sampe source data is in ./index  

## How to Use:

``` bash
# compile Query.java
javac Query.java

# run the engine service
java Query
```
## How build your own indexed data sctruture with more WET files:

``` bash
# compile InvertedIndexBuilderBinary.java
javac InvertedIndexBuilderBinary.java

# run the index-building program
# Path is the directory contains your WET files 
java InvertedIndexBuilderBinary binary <Path>
```
This search engine are using pages cralwed by http://commoncrawl.org/. The source data should be WET files provided by this website in Sep 2017. The program will only choose those WET files whose name begins with 'CC-MAIN-20170919112242' to be used as source data. So be sure do not change the name when you download WET files from this website.

