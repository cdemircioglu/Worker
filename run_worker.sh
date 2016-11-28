javac -cp "rabbitmq-client.jar:mysql-connector-java-5.1.39-bin.jar:." ReadMSISDN.java -Xlint:unchecked
javac -cp "rabbitmq-client.jar:mysql-connector-java-5.1.39-bin.jar:." ReceiveWorkServer.java -Xlint:unchecked
javac -cp "rabbitmq-client.jar:mysql-connector-java-5.1.39-bin.jar:." JobCheck.java -Xlint:unchecked
java -cp "rabbitmq-client.jar:mysql-connector-java-5.1.39-bin.jar:." JobCheck

