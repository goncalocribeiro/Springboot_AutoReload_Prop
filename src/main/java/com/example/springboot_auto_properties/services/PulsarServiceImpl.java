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
    //YAML/Properties
    @Value("${pulsar.service.url}")
    private String SERVICE_URL;

    @Value("${pulsar.encryptedTopic}")
    private String ENCRYPTED_TOPIC_NAME;

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

    @Value("${pulsar.producer.encryptedMsg}")
    private String PULSAR_PRODUCER_ENCRYPTED_MESSAGE;

    @Value("${pulsar.consumer.subscription}")
    private String PULSAR_CONSUMER_SUBSCRIPTION;

    //Local
    private String LOCAL_PUB_KEY = "src/main/resources/test_ecdsa_pubkey.pem";
    private String LOCAL_PRV_KEY = "src/main/resources/test_ecdsa_prvkey.pem";

    AuthenticationSibs authSibs;
    PulsarClient pulsarClient;
    Consumer pulsarConsumer;
    Producer<String> pulsarProducer;

    String errorMsg="";
    String topic="";

    private Producer<String> buildProducer() throws PulsarClientException {
        return pulsarClient.newProducer(Schema.STRING)
                .topic(topic)
                .create();
    }

    private Producer<String> buildEncryptedProducer() throws PulsarClientException {
        return pulsarClient.newProducer(Schema.STRING)
                .topic(topic)
                .addEncryptionKey("myAppTestKey")
                .cryptoKeyReader(new RawFileKeyReader(LOCAL_PUB_KEY, LOCAL_PRV_KEY))
                .create();
    }

    @Override
    public String produce(Boolean encrypted) {
        authSibs = new AuthenticationSibs(PULSAR_CLIENT_USER, PULSAR_CLIENT_PASSWORD, PULSAR_CLIENT_AUTHMETHOD);

        topic = encrypted ? ENCRYPTED_TOPIC_NAME : TOPIC_NAME;
        try {
            pulsarClient = PulsarClient.builder()
                    .serviceUrl(SERVICE_URL)
                    .authentication(authSibs)
                    .build();

            pulsarProducer = encrypted ? buildEncryptedProducer() : buildProducer();

            for (int i=0; i<10; i++) {
                String content = encrypted ? PULSAR_PRODUCER_ENCRYPTED_MESSAGE + "-" + i : PULSAR_PRODUCER_DEFAULT_MESSAGE + "-" + i;

                log.info("******* Sending message: " + content);
                MessageId msgId = pulsarProducer.send(content);
            }

            pulsarProducer.close();
            pulsarClient.close();
        } catch (PulsarClientException e) {
            errorMsg = "Error creating Pulsar Producer";
            log.error(errorMsg);
            e.printStackTrace();
            return errorMsg;
        } catch (Exception e) {
            errorMsg = "Error creating Pulsar Client";
            log.error(errorMsg);
            e.printStackTrace();
            return errorMsg;
        }

        return "Success sending messages";
    }

    @Override
    public void consumeEncrypt() throws PulsarClientException {
        authSibs = new AuthenticationSibs(PULSAR_CLIENT_USER, PULSAR_CLIENT_PASSWORD, PULSAR_CLIENT_AUTHMETHOD);

        pulsarClient = PulsarClient.builder()
                .serviceUrl(SERVICE_URL)
                .authentication(authSibs)
                .build();

        pulsarConsumer = pulsarClient.newConsumer()
                .topic(TOPIC_NAME)
                .subscriptionName(PULSAR_CONSUMER_SUBSCRIPTION)
                .cryptoKeyReader(new RawFileKeyReader(LOCAL_PUB_KEY, LOCAL_PRV_KEY))
                .subscribe();
        Message msg = null;

        while(true) {
            msg = pulsarConsumer.receive();
            // do something
            System.out.println("Received: " + new String(msg.getData()));
            pulsarConsumer.acknowledge(msg);
        }

        // Acknowledge the consumption of all messages at once
        //consumer.acknowledgeCumulative(msg);
    }

    @Override
    public void stopConsume() throws PulsarClientException {
        pulsarConsumer.close();
        pulsarClient.close();
    }
}
