package com.example.springboot_auto_properties.controllers;

import client.AuthenticationSibs;
import com.example.springboot_auto_properties.config.PropertyConfiguration;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.context.config.annotation.RefreshScope;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.stream.IntStream;

@RestController
@RefreshScope
@Slf4j
public class DemoController {
    @Value("${demo.service.profileProp}")
    private String profileProp;

    @Value("${pulsar.service.url}")
    private String SERVICE_URL;

    @Value("${pulsar.topic}")
    private String TOPIC_NAME;

    @Value("${pulsar.client.user}")
    private String PULSAR_CLIENT_USER;

    @Value("${pulsar.client.password}")
    private String PULSAR_CLIENT_PASSWORD;

    @Value("${pulsar.client.authMethod}")
    private String PULSAR_CLIENT_AUTHMETHOD;

    @Value("${pulsar.producer.defaultMsg}")
    private String PULSAR_PRODUCER_DEFAULT_MESSAGE;

    @Autowired
    private PropertyConfiguration propertyConfiguration;

    @GetMapping(value="/profile")
    public String getProfileProp(){
        return profileProp;
    }

    @PostMapping(value="/send")
    public boolean sendToPulsar() throws PulsarClientException {
        AuthenticationSibs authSibs = new AuthenticationSibs(PULSAR_CLIENT_USER, PULSAR_CLIENT_PASSWORD, PULSAR_CLIENT_AUTHMETHOD);

        PulsarClient client = null;
        try {
            client = PulsarClient.builder()
                    .serviceUrl(SERVICE_URL)
                    .authentication(authSibs)
                    .build();
        } catch (Exception e) {
            log.error("Error creating Pulsar Client");
            e.printStackTrace();
            return false;
        }

        Producer<String> producer = null;
        try {
            producer = client.newProducer(Schema.STRING)
                    .topic(TOPIC_NAME)
                    
                    .create();
        } catch (PulsarClientException e) {
            log.error("Error creating Pulsar Producer");
            e.printStackTrace();
            return false;
        }

        /*Producer<String> finalProducer = producer;
        IntStream.range(1, 5).forEach(i -> {
            String content = PULSAR_PRODUCER_DEFAULT_MESSAGE + "-" + i;

            try {
                log.info("******* Sending message: " + content);
                MessageId msgId = finalProducer.send(content);
            } catch (PulsarClientException e) {
                e.printStackTrace();
                log.error("*************************************** ENCRYPTED TOPIC ***************************************");
                if (e.getMessage().contains("Encryption is required")){
                    log.error("Encryption required!!!");
                    try {
                        throw new PulsarClientException(e.getMessage());
                    } catch (PulsarClientException ex) {
                        ex.printStackTrace();
                    }
                }
            }
        });

        finalProducer.close();*/
        client.close();

        return true;
    }
}
