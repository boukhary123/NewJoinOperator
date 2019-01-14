JARFILES=bufmgr/*.class diskmgr/*.class global/*.class iterator/*.class\
         heap/*.class chainexception/*.class  btree/*.class index/*.class tests/*.class

JDKPATH = /usr/lib/jvm/java-11-openjdk-amd64
LIBPATH = ./..
CLASSPATH = $(LIBPATH)
BINPATH = $(JDKPATH)/bin
JAVAC = $(JDKPATH)/bin/javac -classpath $(CLASSPATH)
JAVA  = $(JDKPATH)/bin/java  -classpath $(CLASSPATH)
	
test: 
	cd tests; make jointest

parser:
	cd tests; make parsertest

task1a:
	cd tests; make Task1a

task1b:
	cd tests; make Task1b

task2a:
	cd tests; make Task2a

task2b:
	cd tests; make Task2b

task2c:
	cd tests; make Task2c

task2c_1:
	cd tests; make Task2c_1

task2c_2:
	cd tests; make Task2c_2

task2d_2b:
	cd tests; make Task2d_2b

task2d_2c:
	cd tests; make Task2d_2c

task2d_2c_1:
	cd tests; make Task2d_2c_1

task2d_2c_2:
	cd tests; make Task2d_2c_2

plot:
	cd tests; make Plot

plot:
	cd tests; make Plot

