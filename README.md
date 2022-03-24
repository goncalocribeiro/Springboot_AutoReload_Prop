# Springboot_AutoReload_Prop

This project shows how to update a Spring application properties, without the need to restart the server. By doing this, we can
remove the downtime of a service anytime we need to update configurations.

This app, also produces and consume to/from Pulsar topics, using the properties defined in the .properties file that's present in the remote repository.

When updating the properties in this remote file, and in order to update de values of the @Value variables, the endpoint <appUrl>/actuator/refresh must be called.

**** Properties Server ****
  
  https://github.com/goncalocribeiro/springboot-prop-reader
  
**** Environments ****
  
  To set the desired environment (dev, pre, pro, etc), this definition is made in the src/main/resources/bootstrap.properties file.
  
  Setting the value spring.profiles.active=dev will then use the file demo-service-dev.properties.
  
  If the value is pro, then the properties file used will be demo-service-pro.properties.
  
**** Properties repository ****
  
  https://github.com/goncalocribeiro/SpringbootConfigRepo
  
**** Pulsar ****
- Encrypted namespace: Need to create encryption keys and configure producer and consumer to use it
  - https://pulsar.apache.org/docs/en/security-encryption/
  - peek messages in subscription backlog is not allowed
  - consumer needs the private key to decrypt messages
