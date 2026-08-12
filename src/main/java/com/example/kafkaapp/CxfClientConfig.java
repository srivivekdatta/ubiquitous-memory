package com.example.kafkaapp;

import org.apache.cxf.endpoint.Client;
import org.apache.cxf.frontend.ClientProxy;
import org.apache.cxf.jaxws.JaxWsProxyFactoryBean;
import org.apache.cxf.transport.http.HTTPConduit;
import org.apache.cxf.transports.http.configuration.HTTPClientPolicy;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class CxfClientConfig {

    @Value("${cxf.client.connection-timeout}")
    private long connectionTimeout;

    @Value("${cxf.client.receive-timeout}")
    private long receiveTimeout;

    @Bean
    public ActimizeSoapService actimizeSoapService() {
        JaxWsProxyFactoryBean jaxWsProxyFactoryBean = new JaxWsProxyFactoryBean();
        jaxWsProxyFactoryBean.setServiceClass(ActimizeSoapService.class);
        // Replace with the actual endpoint URL of your SOAP service
        jaxWsProxyFactoryBean.setAddress("http://localhost:8080/services/actimize");

        ActimizeSoapService soapClient = (ActimizeSoapService) jaxWsProxyFactoryBean.create();

        // Configure the HTTP Conduit for timeouts to strictly enforce the 2.5-second SLA
        Client client = ClientProxy.getClient(soapClient);
        HTTPConduit httpConduit = (HTTPConduit) client.getConduit();
        HTTPClientPolicy httpClientPolicy = new HTTPClientPolicy();
        httpClientPolicy.setConnectionTimeout(connectionTimeout);
        httpClientPolicy.setReceiveTimeout(receiveTimeout);
        httpConduit.setClient(httpClientPolicy);

        return soapClient;
    }
}
