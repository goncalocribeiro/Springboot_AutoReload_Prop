package com.example.springboot_auto_properties.services;

import client.AuthenticationSibs;
import com.example.springboot_auto_properties.utils.RawFileKeyReader;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.context.config.annotation.RefreshScope;
import org.springframework.stereotype.Service;

@Slf4j
@RefreshScope
@Service
public class PulsarServiceImpl implements PulsarService {
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

    @Value("${pulsar.consumer.subscription}")
    private String PULSAR_CONSUMER_SUBSCRIPTION;

    //Local
    private String LOCAL_PUB_KEY = "src/main/resources/test_ecdsa_pubkey.pem";
    private String LOCAL_PRV_KEY = "src/main/resources/test_ecdsa_prvkey.pem";

    AuthenticationSibs authSibs;
    PulsarClient client;
    Consumer consumer;

    @Override
    public Boolean produce() throws PulsarClientException {
        authSibs = new AuthenticationSibs(PULSAR_CLIENT_USER, PULSAR_CLIENT_PASSWORD, PULSAR_CLIENT_AUTHMETHOD);
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
                    .addEncryptionKey("myAppTestKey")
                    .cryptoKeyReader(new RawFileKeyReader(LOCAL_PUB_KEY, LOCAL_PRV_KEY))
                    .create();
        } catch (PulsarClientException e) {
            log.error("Error creating Pulsar Producer");
            e.printStackTrace();
            return false;
        }

        Producer<String> finalProducer = producer;
        for (int i=0; i<10; i++) {
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
        }

        finalProducer.close();
        client.close();

        return true;
    }

    @Override
    public void consume() throws PulsarClientException {
        authSibs = new AuthenticationSibs(PULSAR_CLIENT_USER, PULSAR_CLIENT_PASSWORD, PULSAR_CLIENT_AUTHMETHOD);
        client = PulsarClient.builder()
                .serviceUrl(SERVICE_URL)
                .authentication(authSibs)
                .build();

        consumer = client.newConsumer()
                .topic(TOPIC_NAME)
                .subscriptionName(PULSAR_CONSUMER_SUBSCRIPTION)
                .cryptoKeyReader(new RawFileKeyReader(LOCAL_PUB_KEY, LOCAL_PRV_KEY))
                .subscribe();
        Message msg = null;

        while(true) {
            msg = consumer.receive();
            // do something
            System.out.println("Received: " + new String(msg.getData()));
            consumer.acknowledge(msg);
        }

        // Acknowledge the consumption of all messages at once
        //consumer.acknowledgeCumulative(msg);
    }

    @Override
    public void stopConsume() throws PulsarClientException {
        consumer.close();
        client.close();
    }
}
