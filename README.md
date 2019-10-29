# Weather Data Exercise

An example project for reading some sample Weather Station data. The intention is to give a simple starting point for a 
coding exercise during a technical interview.  The candidate does not necessarily need to implement everything 
successfully; it's also useful to see how the troubleshoot and figure things out.

**Note** that the code intentionally does not run on a local machine in it's
current state. This should be an easy fix for a Scala/Spark developer.

1. Read the data into a Dataset. 
    1. Print the Dataset.
    2. Save the Dataset to a CSV file.
2. Count the number of records
3. Get a distinct list of weather station names
    1. Print the Dataset.
    2. Save the Dataset to a CSV file.
4. Get the weather station names in all lowercase (or capitalized if in Scala)
    1. Print the Dataset.
    2. Save the Dataset to a CSV file.
5. Sort the data by weather station name
    1. Print the Dataset.
    2. Save the Dataset to a CSV file.
6. Count of the weather stations per province
    1. Print the Dataset.
    2. Save the Dataset to a CSV file.
7. Find the furthest north / south / west / east weather station
    1. Print the Dataset.
    2. Save the Dataset to a CSV file.

If possible please provide unit tests. Explain how this code could be made more testable.