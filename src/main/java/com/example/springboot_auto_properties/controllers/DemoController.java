package com.example.springboot_auto_properties.controllers;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.context.config.annotation.RefreshScope;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RefreshScope
public class DemoController {
    @Value("${demo.service.profileProp}")
    private String profileProp;

    @GetMapping(value="/profile")
    public String getProfileProp(){
        return profileProp;
    }
}
