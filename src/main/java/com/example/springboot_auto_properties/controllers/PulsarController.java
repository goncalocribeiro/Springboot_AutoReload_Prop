package com.example.springboot_auto_properties.controllers;

import com.example.springboot_auto_properties.services.PulsarService;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Slf4j
public class PulsarController {
    @Value("${demo.service.profileProp}")
    private String profileProp;

    @Autowired
    PulsarService pulsarService;

    @GetMapping(value="/profile")
    public String getProfileProp(){
        return profileProp;
    }

    @PostMapping(value="/produce")
    public ResponseEntity<Boolean> sendToPulsar() throws PulsarClientException {
        return ResponseEntity.ok(pulsarService.produce());
    }

    @PostMapping(value="/consume")
    public void consumeFromPulsar() throws PulsarClientException {
        pulsarService.consume();
    }

    @PostMapping(value="/stopConsume")
    public void stopConsumeFromPulsar() throws PulsarClientException {
        pulsarService.stopConsume();
    }
}
