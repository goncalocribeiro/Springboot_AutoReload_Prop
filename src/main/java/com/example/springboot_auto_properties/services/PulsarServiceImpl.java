package com.example.springboot_auto_properties.services;

import client.AuthenticationSibs;
import com.example.springboot_auto_properties.utils.RawFileKeyReader;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.impl.BatchMessageIdImpl;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.api.raw.RawMessage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.context.config.annotation.RefreshScope;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Locale;

@Slf4j
@RefreshScope
@Service
public class PulsarServiceImpl implements PulsarService {
    //YAML/Properties
    @Value("${pulsar.encryptedTopic}")
    private String ENCRYPTED_TOPIC_NAME;

    @Value("${pulsar.defaultTopic}")
    private String TOPIC_NAME;

    @Value("${pulsar.producer.defaultMsg}")
    private String PULSAR_PRODUCER_DEFAULT_MESSAGE;

    @Value("${pulsar.producer.encryptedMsg}")
    private String PULSAR_PRODUCER_ENCRYPTED_MESSAGE;

    @Value("${pulsar.consumer.encryptedSubscription}")
    private String PULSAR_CONSUMER_ENCRYPTED_SUBSCRIPTION;

    @Value("${pulsar.consumer.defaultSubscription}")
    private String PULSAR_CONSUMER_DEFAULT_SUBSCRIPTION;

    @Value("${pulsar.reader.defaultSubscription}")
    private String PULSAR_READER_SUBSCRIPTION;

    //Local
    private String LOCAL_PUB_KEY = "src/main/resources/test_ecdsa_pubkey.pem";
    private String LOCAL_PRV_KEY = "src/main/resources/test_ecdsa_prvkey.pem";

    AuthenticationSibs authSibs;
    PulsarClient pulsarClient;
    Consumer pulsarConsumer;
    Producer<String> pulsarProducer;
    Reader<byte[]> pulsarReader;
    MessageId messageId;

    String errorMsg="";
    String topic="";

    @Autowired
    public PulsarServiceImpl(@Value("${pulsar.client.user}") String pulsarClientUser,
                             @Value("${pulsar.client.password}") String pulsarClientPassword,
                             @Value("${pulsar.client.authMethod}") String pulsarClientMethod,
                             @Value("${pulsar.service.url}") String pulsarServiceUrl){
        authSibs = new AuthenticationSibs(pulsarClientUser, pulsarClientPassword, pulsarClientMethod);

        try {
            pulsarClient = PulsarClient.builder()
                    .serviceUrl(pulsarServiceUrl)
                    .authentication(authSibs)
                    .build();
        } catch (PulsarClientException e) {
            e.printStackTrace();
        }
    }

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
    public String produce(Boolean encrypted, String message) {
        topic = encrypted ? ENCRYPTED_TOPIC_NAME : TOPIC_NAME;
        try {
            pulsarProducer = encrypted ? buildEncryptedProducer() : buildProducer();

            for (int i=0; i<10; i++) {
                String content = ((message == null || message.isEmpty()) && encrypted) ? PULSAR_PRODUCER_ENCRYPTED_MESSAGE + "-" + i : (message == null || message.isEmpty()) ? PULSAR_PRODUCER_DEFAULT_MESSAGE + "-" + i : message + "-" + i;

                log.info("******* Sending message: " + content);
                MessageId msgId = pulsarProducer.send(content);
                log.info("Message ID of sent message: " + msgId.toString());
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
        pulsarConsumer = pulsarClient.newConsumer()
                .topic(TOPIC_NAME)
                .subscriptionName(PULSAR_CONSUMER_ENCRYPTED_SUBSCRIPTION)
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

    /**
     *
     * @param encrypted
     * @param messageId can be MessageId.earliest, MessageId.latest, <messageId> in format <ledgerId>:<entryId>:<partitionIndex>
     * @param readOnlyOnce - boolean to indicate if should read only one message
     */
    @Override
    public String read(Boolean encrypted, String messageId, Boolean readOnlyOnce) throws IOException {
        topic = encrypted ? ENCRYPTED_TOPIC_NAME : TOPIC_NAME;
        log.info("Reading from topic: " + topic);

        switch (messageId.toUpperCase()){
            case "E":
                this.messageId = MessageId.earliest;
                break;
            case "L":
                this.messageId = MessageId.latest;
                break;
            default:
                if (messageId != null && !messageId.isEmpty()){
                    log.info("Trying to read messageId: " + messageId);
                    String[] messageIdComponents = messageId.split(":");
                    if(messageIdComponents.length != 3) {
                        this.messageId = null;
                        break;
                    }

                    long ledgerId = Long.parseLong(messageIdComponents[0]);
                    long entryId = Long.parseLong(messageIdComponents[1]);
                    int partitionIndex = Integer.parseInt(messageIdComponents[2]);

                    log.info("ledgerId: " + ledgerId + ", entryId: " + entryId + ", partitionIndex: " + partitionIndex);

                    this.messageId = new MessageIdImpl(ledgerId, entryId, partitionIndex);
                }
        }

        if (this.messageId == null) {
            return "MessageId is not valid";
        }

        try {
            pulsarReader = pulsarClient.newReader()
                    .topic(topic)
                    .subscriptionName(PULSAR_READER_SUBSCRIPTION)
                    .startMessageId(this.messageId)
                    .create();
        } catch (PulsarClientException e) {
            log.error("Error reading from Pulsar Topic: " + topic);
            e.printStackTrace();
        }

        if (pulsarReader != null) {
            while(pulsarReader.hasMessageAvailable()){
                try {
                    Message message = pulsarReader.readNext();
                    log.info("Pulsar Reader | Message Id: " + message.getMessageId().toString());
                    log.info("Pulsar Reader | Message producer: " + message.getProducerName());
                    log.info("Pulsar Reader | Message string: " + new String(message.getData()));

                    if (readOnlyOnce) {
                        //Reads only one message
                        break;
                    }
                } catch (PulsarClientException e) {
                    log.error("Error reading message");
                    e.printStackTrace();
                }
            }
        }

        pulsarReader.close(); //to delete reader subscription
        return "Finished reading";
    }
}
