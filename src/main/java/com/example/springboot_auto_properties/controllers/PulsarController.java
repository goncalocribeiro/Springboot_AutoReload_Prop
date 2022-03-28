package com.example.springboot_auto_properties.controllers;

import com.example.springboot_auto_properties.services.PulsarService;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Slf4j
public class PulsarController {
    @Value("${demo.service.profileProp}")
    private String profileProp;

    @Autowired
    PulsarService pulsarService;

    /**
     *
     * @return
     */
    @GetMapping(value="/profile")
    public String getProfileProp(){
        return profileProp;
    }

    /**
     * Function to produce messages to a Pulsar topic
     * All the needed properties are settled in the .properties file in github repository: https://github.com/goncalocribeiro/SpringbootConfigRepo
     * @return String message
     */
    @PostMapping(value="/produce")
    public ResponseEntity<String> produce(@RequestParam Boolean encrypted,
                                          @RequestParam String msg) {
        return ResponseEntity.ok(pulsarService.produce(encrypted, msg));
    }

    /**
     * Function to consume messages from a Pulsar topic in an encrypted namespace
     * All the needed properties are settled in the .properties file in github repository: https://github.com/goncalocribeiro/SpringbootConfigRepo
     * @throws PulsarClientException
     */
    @PostMapping(value="/consumeEncrypt")
    public void consumeEncrypt() throws PulsarClientException {
        pulsarService.consumeEncrypt();
    }

    /**
     * Function to stop Pulsar consumers
     * @throws PulsarClientException
     */
    @PostMapping(value="/stopConsume")
    public void stopConsumeFromPulsar() throws PulsarClientException {
        pulsarService.stopConsume();
    }

    @PostMapping(value="/read")
    public ResponseEntity<String> readFromPulsar(@RequestParam Boolean encrypted,
                               @RequestParam String messageId) {
        return ResponseEntity.ok(pulsarService.read(encrypted, messageId));
    }
}
