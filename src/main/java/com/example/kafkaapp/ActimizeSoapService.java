package com.example.kafkaapp;

import jakarta.jws.WebMethod;
import jakarta.jws.WebParam;
import jakarta.jws.WebService;

@WebService(targetNamespace = "http://example.com/actimize")
public interface ActimizeSoapService {

    @WebMethod
    String checkFraud(@WebParam(name = "payload") String payload);
}
