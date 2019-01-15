# Minibase Inequality Join Operator

The objective of this project is to extend the code and capabilities of [Minibase][l1] to support fast and scalable Inequality Joins as well as Query Parsing. The java code in this project is scaled to fit and support join operations on large relations. The default join operation implemented in Java Minibase is a nested loop join which is considered a naive approach for handling joins on large relations. The join we implemented in this project is based on the [paper][l2] “Fast and Scalable Inequality Joins”. The runtime and performance for each of the implemented join operators will be studied, compared, and tested.

### To run the code
Modify the JDK directory according to your machine in the MakeFile in the *"JoinAssignement/Code/javaminibase/src/"* directory and *"JoinAssignement/Code/javaminibase/src/tests/"* directory of the project


##### Note: all the task details are found in the *"JoinAssignement/Report/"* directory and all the relations and the querys are found in the *"JoinAssignement/Output/"*

Navigate to the *"JoinAssignement/Code/javaminibase/src/"* directory of the project

### Task 1a
```sh
$ make Task1a
```
### Task 1b
```sh
$ make Task1b
```
### Task 2a
```sh
$ make Task2a
```
### Task 2b
```sh
$ make Task2b
```
### Task 2c
```sh
$ make Task2c
```
### Task 2c_1
```sh
$ make Task2c_1
```
### Task 2c_2
```sh
$ make Task2c_2
```
### Task 2d_2b
```sh
$ make Task2c_2b
```
### Task 2d_2c
```sh
$ make Task2d_2c
```
### Task 2d_2c_1
```sh
$ make Task2d_2c_1
```
### Task 2d_2c_2
```sh
$ make Task2d_2c_2
```

   [l1]: <http://research.cs.wisc.edu/coral/mini_doc/minibase.html>
   [l2]: <http://da.qcri.org/ntang/pubs/vldbj2016.pdf>

