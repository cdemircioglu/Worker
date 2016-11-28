git -C ~/worker/ stash save --keep-index git -C ~/worker/ stash drop
git -C ~/worker/ fetch origin  --quiet
git -C ~/worker/ merge origin/master  --quiet

cd ~/worker
javac -cp "rabbitmq-client.jar:mysql-connector-java-5.1.39-bin.jar:." ReadMSISDN.java -Xlint:unchecked
javac -cp "rabbitmq-client.jar:mysql-connector-java-5.1.39-bin.jar:." ReceiveWorkServer.java -Xlint:unchecked
javac -cp "rabbitmq-client.jar:mysql-connector-java-5.1.39-bin.jar:." JobCheck.java -Xlint:unchecked

