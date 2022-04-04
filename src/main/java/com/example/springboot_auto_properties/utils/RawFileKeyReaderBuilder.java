package com.example.springboot_auto_properties.utils;

import lombok.Getter;
import lombok.Setter;

public class RawFileKeyReaderBuilder {
    String publicKeyFile = "";
    String privateKeyFile = "";

    public  RawFileKeyReaderBuilder(){}

    public RawFileKeyReaderBuilder(String publicKeyFile, String privateKeyFile){
        this.publicKeyFile = publicKeyFile;
        this.privateKeyFile = privateKeyFile;
    }

    public RawFileKeyReaderBuilder setPublicKey(String publicKeyFile){
        this.publicKeyFile = publicKeyFile;
        return this;
    }

    public RawFileKeyReaderBuilder setPrivateKey(String privateKeyFile){
        this.privateKeyFile = privateKeyFile;
        return this;
    }

    public RawFileKeyReader build(){
        return new RawFileKeyReader(publicKeyFile, privateKeyFile);
    }
}
