#!/bin/sh
java -Xbootclasspath/p:$SCALA_HOME/lib/scala-library.jar:$SCALA_HOME/lib/scala-compiler.jar:$SCALA_HOME/lib/scala-reflect.jar:$SCALA_HOME/lib/jline.jar -jar cdg.jar
