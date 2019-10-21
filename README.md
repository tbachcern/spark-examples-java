This repository shows some basic spark functionality in Java.

Spark supports Python, Scala and Java. However, using Java is sometimes a bit cumbersome and the documentation does not make a best attempt to answer questions and provide examples. Due to personal curiosity of how things work with pure Java on Windows, this project contains now some working examples.

Requirements
============
- Java >= 11 (Lower versions >= 8 are not a fundamental issue, but pom.xml and some functionaity like List.of must be adapted then)
- Maven
- Optional: Eclipse (it is tested on Eclipse 2019-09)

Setup
=====
- Clone project (or combine with next step)
- Initiate project with maven
- For Windows, spark requires the winutil binaries. They are included witin /src/resources. Alternatively, they may be found here: https://github.com/cdarlint/winutils
- Adapt HADOOP_PATH in SparkConfig within SparkProviderLocal to point to your (theoretical) Hadoop Path. We only use it to load winutils, which spark expects in HADOOP_PATH/bin
- Test.java should run.
