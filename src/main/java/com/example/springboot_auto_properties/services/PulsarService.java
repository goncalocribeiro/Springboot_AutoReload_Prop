package com.example.springboot_auto_properties.services;

import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.springframework.stereotype.Service;

import java.io.IOException;

@Service
public interface PulsarService {
    public String produce(Boolean encrypted, String message, Integer n_msg) throws PulsarClientException;
    public void consume(Boolean encrypted) throws PulsarClientException;
    public void stopConsume() throws PulsarClientException;
    public String read(Boolean encrypted, String messageId, Boolean readOnlyOnce) throws IOException;
    public String admin() throws PulsarAdminException;
}
