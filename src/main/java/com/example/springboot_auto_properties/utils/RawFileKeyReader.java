package com.example.springboot_auto_properties.utils;

import org.apache.pulsar.client.api.CryptoKeyReader;
import org.apache.pulsar.client.api.EncryptionKeyInfo;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;

public class RawFileKeyReader implements CryptoKeyReader {

    String publicKeyFile = "";
    String privateKeyFile = "";

    public RawFileKeyReader(String pubKeyFile, String prvKeyFile) {
        publicKeyFile = pubKeyFile;
        privateKeyFile = prvKeyFile;
    }

    @Override
    //used by producer
    public EncryptionKeyInfo getPublicKey(String keyName, Map<String, String> keyMeta) {
        EncryptionKeyInfo keyInfo = new EncryptionKeyInfo();
        try {
            keyInfo.setKey(Files.readAllBytes(Paths.get(publicKeyFile)));
        } catch (IOException e) {
            System.out.println("ERROR: Failed to read public key from file " + publicKeyFile);
            e.printStackTrace();
        }
        return keyInfo;
    }

    @Override
    //used by consumer
    public EncryptionKeyInfo getPrivateKey(String keyName, Map<String, String> keyMeta) {
        EncryptionKeyInfo keyInfo = new EncryptionKeyInfo();
        try {
            keyInfo.setKey(Files.readAllBytes(Paths.get(privateKeyFile)));
        } catch (IOException e) {
            System.out.println("ERROR: Failed to read private key from file " + privateKeyFile);
            e.printStackTrace();
        }
        return keyInfo;
    }
}