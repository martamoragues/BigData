javac -cp "/home/jcugat/hadoop-0.20.203.0/*:/home/jcugat/hadoop-0.20.203.0/lib/*" SeqConverter.java
jar cvf SeqConverter.jar *.class
/home/jcugat/hadoop-0.20.203.0/bin/hadoop jar SeqConverter.jar SeqConverter
