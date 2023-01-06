package com.example.springboot_auto_properties.services;

import com.example.springboot_auto_properties.utils.RawFileKeyReader;
import com.example.springboot_auto_properties.utils.RawFileKeyReaderBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.Clusters;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.impl.BatchMessageIdImpl;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.auth.AuthenticationTls;
import org.apache.pulsar.common.api.raw.RawMessage;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.FailureDomain;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.context.config.annotation.RefreshScope;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

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

    private String authPlugin;
    private String authParams;

    //Local
    private String LOCAL_PUB_KEY = "src/main/resources/test_ecdsa_pubkey.pem";
    private String LOCAL_PRV_KEY = "src/main/resources/test_ecdsa_prvkey.pem";

    PulsarClient pulsarClient;
    Consumer pulsarConsumer;
    Producer<String> pulsarProducer;
    Reader<byte[]> pulsarReader;
    MessageId messageId;
    RawFileKeyReader producerCryptoReader;
    RawFileKeyReader consumerCryptoReader;
    PulsarAdmin pulsarAdmin;

    String errorMsg="";
    String topic="";
    String subscriptionName="";

    @Autowired
    public PulsarServiceImpl(@Value("${pulsar.service.url}") String pulsarServiceUrl,
                             @Value("${pulsar.admin.url}") String pulsarAdminUrl,
                             @Value("${pulsar.client.cert}") String clientPubKey,
                             @Value("${pulsar.client.key}") String clientPrvKey,
                             @Value("${pulsar.trusted.ca}") String trustCaCert,
                             @Value("${pulsar.authPlugin}") String authPlugin,
                             @Value("${pulsar.authParams}") String authParams){
        this.authPlugin = authPlugin;
        this.authParams = authParams;
        buildAuthClient(pulsarServiceUrl, trustCaCert);
        buildPulsarAdmin(pulsarAdminUrl, trustCaCert);
    }

    private void buildAuthClient(String pulsarServiceUrl,
                                 String trustCaCert){
        try {
            pulsarClient = PulsarClient.builder()
                    .serviceUrl(pulsarServiceUrl)
                    .tlsTrustCertsFilePath(trustCaCert)
                    .authentication(this.authPlugin, this.authParams)
                    .build();
        } catch (PulsarClientException e) {
            e.printStackTrace();
        }
    }

    public void buildPulsarAdmin(String pulsarAdminUrl,
                                 String trustCaCert){
        try{
            this.pulsarAdmin = PulsarAdmin.builder()
                    .authentication(this.authPlugin, this.authParams)
                    .serviceHttpUrl(pulsarAdminUrl)
                    .tlsTrustCertsFilePath(trustCaCert)
                    .allowTlsInsecureConnection(false)
                    .build();
        } catch (PulsarClientException.UnsupportedAuthenticationException e) {
            e.printStackTrace();
        } catch (PulsarClientException e) {
            e.printStackTrace();
        }
    }

    private void buildProducerCryptoReader(){
        producerCryptoReader = new RawFileKeyReaderBuilder()
                .setPublicKey(LOCAL_PUB_KEY)
                .build();
    }

    private void buildConsumerCryptoReader(){
        consumerCryptoReader = new RawFileKeyReaderBuilder()
                .setPrivateKey(LOCAL_PRV_KEY)
                .build();
    }

    private Producer<String> buildProducer() throws PulsarClientException {
        return pulsarClient.newProducer(Schema.STRING)
                .topic(topic)
                .create();
    }

    private Producer<String> buildEncryptedProducer() throws PulsarClientException {
        buildProducerCryptoReader();
        return pulsarClient.newProducer(Schema.STRING)
                .topic(topic)
                .addEncryptionKey("myAppTestKey")
                .cryptoKeyReader(this.producerCryptoReader)
                .create();
    }

    private Consumer buildConsumer(MessageListener pulsarMessageListener) throws PulsarClientException {
        return pulsarConsumer = pulsarClient.newConsumer()
                .topic(topic)
                .subscriptionName(subscriptionName)
                .subscriptionType(SubscriptionType.Shared)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Latest)
                .messageListener(pulsarMessageListener)
                .subscribe();
    }

    private Consumer buildEncryptedConsumer(MessageListener pulsarMessageListener) throws PulsarClientException {
        buildConsumerCryptoReader();
        return pulsarClient.newConsumer()
                .topic(topic)
                .subscriptionName(subscriptionName)
                .messageListener(pulsarMessageListener)
                .cryptoKeyReader(this.consumerCryptoReader)
                .subscribe();
    }

    /**
     *
     * @param encrypted: should produce to encrypted topic
     * @param message: message to be sent. If no message,then will send static message in .properties
     * @return String value
     */
    @Override
    public String produce(Boolean encrypted, String message, Integer n_msg) throws PulsarClientException {
        topic = encrypted ? ENCRYPTED_TOPIC_NAME : TOPIC_NAME;
        try {
            pulsarProducer = encrypted ? buildEncryptedProducer() : buildProducer();

            for (int i=0; i<n_msg; i++) {
                String content = ((message == null || message.isEmpty()) && encrypted) ? PULSAR_PRODUCER_ENCRYPTED_MESSAGE + "-" + i : (message == null || message.isEmpty()) ? PULSAR_PRODUCER_DEFAULT_MESSAGE + "-" + i : message + "-" + i;

                log.info("******* Sending message: " + content);
                MessageId msgId = pulsarProducer.send(content);

                //Send a message after 10 seconds
                MessageId delayedMsgId = pulsarProducer
                        .newMessage()
                        .deliverAfter(60, TimeUnit.SECONDS)
                        .value(content)
                        .send();
                log.info("Message ID of sent message: " + msgId.toString());
                log.info("Message ID of delay sent message: " + delayedMsgId.toString());
            }

            //pulsarProducer.close();
            //pulsarClient.close();
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

        pulsarProducer.close();
        return "Success sending messages";
    }

    @Override
    public void consume(Boolean encrypted) throws PulsarClientException {
        topic = encrypted ? ENCRYPTED_TOPIC_NAME : TOPIC_NAME;
        subscriptionName = encrypted ? PULSAR_CONSUMER_ENCRYPTED_SUBSCRIPTION : PULSAR_CONSUMER_DEFAULT_SUBSCRIPTION;


        //Avoid mainthread and request to be locked
        MessageListener pulsarMessageListener = (consumer, receivedMsg) -> {
            try{
                System.out.println("Received: " + new String(receivedMsg.getData()));
                consumer.acknowledge(receivedMsg);
            } catch (Exception e) {
                consumer.negativeAcknowledge(receivedMsg);
            }
        };

        pulsarConsumer = encrypted ? buildEncryptedConsumer(pulsarMessageListener) : buildConsumer(pulsarMessageListener);
    }

    @Override
    public void stopConsume() throws PulsarClientException {
        pulsarConsumer.close();
        //pulsarClient.close();
    }

    /**
     *
     * @param encrypted
     * @param messageId can be MessageId.earliest, MessageId.latest, <messageId> in format <ledgerId>:<entryId>:<partitionIndex>
     * @param readOnlyOnce - boolean to indicate if should read only one message
     */
    @Override
    public String read(Boolean encrypted, String messageId, Boolean readOnlyOnce) throws IOException {
        int counter = 0;
        ArrayList<String> readMessages = new ArrayList<String>();
        topic = encrypted ? ENCRYPTED_TOPIC_NAME : TOPIC_NAME;
        log.info("Start reading from topic: " + this.topic);

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
        /*
        * subsA
        * ------------------------ consumer.subscription()
        * */

        try {
            pulsarReader = pulsarClient.newReader()
                    .topic(topic)
                    //.subscriptionName(PULSAR_READER_SUBSCRIPTION)
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

                    readMessages.add(new String(message.getData()));
                    counter++;

                    if (readOnlyOnce) {
                        //Reads only one message
                        break;
                    }
                } catch (PulsarClientException e) {
                    log.error("Error reading message");
                    e.printStackTrace();
                }
            }

            log.info("Finished reading from topic: " + this.topic);
            pulsarReader.close(); //to delete reader subscription
        }
        return "Finished reading " + counter + " messages. Messages list: " + readMessages.toString();
    }

    public String admin() throws PulsarAdminException {
        List<String> tenantsList = pulsarAdmin.tenants().getTenants();

        Clusters clusters = pulsarAdmin.clusters();
        List<String> clustersList = clusters.getClusters();
        ClusterData clusterData = clusters.getCluster("standalone");

        log.info("serviceUrl: ", clusterData.getServiceUrl());
        log.info("serviceUrlTls: ", clusterData.getServiceUrlTls());
        log.info("brokerServiceUrl: ", clusterData.getBrokerServiceUrl());
        log.info("brokerServiceUrlTls: ", clusterData.getBrokerServiceUrlTls());

        /*List<String> clustersList = pulsarAdmin.clusters().getClusters();
        Map<String, FailureDomain> failureDomains = pulsarAdmin.clusters().getFailureDomains("standalone");*/

        /*for (String tenant: tenantsList) {
            log.info("Tenant: " + tenant);
        }

        return tenantsList.toString();*/

        /*for (String cluster : clustersList) {
            log.info("Cluster: " + cluster);
        }

        for (String failureDomain : failureDomains.keySet()) {
            log.info("Failure domain: " + failureDomain.toString());
        }*/

        return clustersList.toString();
    }
}
