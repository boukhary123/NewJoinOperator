# modify the paths appropriately
JDKPATH = /usr/lib/jvm/java-11-openjdk-amd64
LIBPATH = ./..
CLASSPATH = $(LIBPATH)
BINPATH = $(JDKPATH)/bin
JAVAC = $(JDKPATH)/bin/javac -classpath $(CLASSPATH)
JAVA  = $(JDKPATH)/bin/java  -classpath $(CLASSPATH)

# test buffer manager

BMTest:BMTest.java
	$(JAVAC) TestDriver.java BMTest.java

bmtest: BMTest
	$(JAVA) tests.BMTest

# test disk manager

DBTest:DBTest.java
	$(JAVAC) TestDriver.java DBTest.java

dbtest: DBTest
	$(JAVA) tests.DBTest

# test heapfile 

HFTest:HFTest.java
	$(JAVAC) TestDriver.java HFTest.java

hftest: HFTest
	$(JAVA) tests.HFTest

# test btree

BTTest:BTTest.java
	$(JAVAC) TestDriver.java BTTest.java

bttest: BTTest
	$(JAVA) tests.BTTest

# test index

IndexTest:IndexTest.java
	$(JAVAC) TestDriver.java IndexTest.java

indextest: IndexTest
	$(JAVA) tests.IndexTest

# test join

JoinTest: JoinTest.java
	$(JAVAC) TestDriver.java JoinOperatorTest.java

jointest: JoinTest
	$(JAVA) tests.JoinOperatorTest

# test sort

SortTest:SortTest.java
	$(JAVAC) TestDriver.java SortTest.java

sorttest: SortTest
	$(JAVA) tests.SortTest

# test sortMerge

SMJoin:SM_JoinTest.java
	$(JAVAC) SM_JoinTest.java TestDriver.java

sortmerge: SMJoin
	$(JAVA) tests.SM_JoinTest


# test parser

parser:ParserTest.java
	$(JAVAC) ParserTest.java

parsertest: parser
	$(JAVA) tests.ParserTest

# Nested Loop Join Task1a

Task1a_Test: Task1a.java 
	$(JAVAC) Task1a.java

Task1a: Task1a_Test
	$(JAVA) tests.Task1a

# Nested Loop Join Task1b

Task1b_Test: Task1b.java 
	$(JAVAC) Task1b.java

Task1b: Task1b_Test
	$(JAVA) tests.Task1b

# Self Join One Predicate Task2a

Task2a_Test: Task2a.java 
	$(JAVAC) Task2a.java

Task2a: Task2a_Test
	$(JAVA) tests.Task2a

# Self Inequality Join Two Predicates Task2b

Task2b_Test: Task2b.java 
	$(JAVAC) Task2b.java

Task2b: Task2b_Test
	$(JAVA) tests.Task2b

# Inequality Join Two Predicates Task2c

Task2c_Test: Task2c.java 
	$(JAVAC) Task2c.java

Task2c: Task2c_Test
	$(JAVA) tests.Task2c

# Inequality Join Two Predicates Task2c_1

Task2c_1_Test: Task2c_1.java 
	$(JAVAC) Task2c_1.java

Task2c_1: Task2c_1_Test
	$(JAVA) tests.Task2c_1

# Inequality Join Two Predicates Task2c_2

Task2c_2_Test: Task2c_2.java 
	$(JAVAC) Task2c_2.java

Task2c_2: Task2c_2_Test
	$(JAVA) tests.Task2c_2

# Self Inequality Join TwoPredicate Optimized Task2d_2b

Task2d_2b_Test: Task2d_2b.java 
	$(JAVAC) Task2d_2b.java

Task2d_2b: Task2d_2b_Test
	$(JAVA) tests.Task2d_2b

# Inequality Join Two Predicates Optimized Task2d_2c

Task2d_2c_Test: Task2d_2c.java 
	$(JAVAC) Task2d_2c.java

Task2d_2c: Task2d_2c_Test
	$(JAVA) tests.Task2d_2c

# Inequality Join Two Predicates Optimized Task2d_2c_1

Task2d_2c_1_Test: Task2d_2c_1.java 
	$(JAVAC) Task2d_2c_1.java

Task2d_2c_1: Task2d_2c_1_Test
	$(JAVA) tests.Task2d_2c_1

# Inequality Join Two Predicates Optimized Task2d_2c_2

Task2d_2c_2_Test: Task2d_2c_2.java 
	$(JAVAC) Task2d_2c_2.java

Task2d_2c_2: Task2d_2c_2_Test
	$(JAVA) tests.Task2d_2c_2



