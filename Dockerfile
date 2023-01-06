FROM eclipse-temurin:11-jdk-focal
WORKDIR /app
COPY ./target/springboot_auto_properties-0.0.1-SNAPSHOT.jar /app

#import ca cert into java trust store
#USER root
#COPY src/main/resources/ca-pulsar-cert.pem $JAVA_HOME/lib/security
#COPY src/main/resources/broker.der $JAVA_HOME/lib/security
#COPY src/main/resources/ca-pulsar-cert.der $JAVA_HOME/lib/security
#COPY src/main/resources/ca-pulsar-cert.pem /usr/local/share/ca-certificates/
#COPY src/main/resources/ca-pulsar-cert.pem /etc/ssl/certs/

#RUN \
#    cd $JAVA_HOME/lib/security \
#    && keytool -import -alias pulsarcacert -keystore cacerts -file ca-pulsar-cert.der -storepass changeit -noprompt
#&& keytool -import -alias pulsarbrokercert -keystore cacerts -file broker.der -storepass changeit -noprompt
#&& keytool -importcert -file ca-pulsar-cert.pem -alias mycacert -keystore $JAVA_HOME/lib/security/cacerts -storepass changeit -noprompt -trustcacerts

ENTRYPOINT ["java", "-jar", "springboot_auto_properties-0.0.1-SNAPSHOT.jar"]