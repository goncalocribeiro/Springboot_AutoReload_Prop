package com.example.springboot_auto_properties.services;

import org.apache.pulsar.client.api.PulsarClientException;
import org.springframework.stereotype.Service;

@Service
public interface PulsarService {
    public String produceEncrypt();
    public void consumeEncrypt() throws PulsarClientException;
    public void stopConsume() throws PulsarClientException;
}
