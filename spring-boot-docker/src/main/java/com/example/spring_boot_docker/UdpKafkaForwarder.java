package com.example.spring_boot_docker;

import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

@Component
public class UdpKafkaForwarder {
    private static final Logger log = LoggerFactory.getLogger(UdpKafkaForwarder.class);

    @Value("${udp.port:8000}")
    private int udpPort;

    @Value("${udp.topic:sensors}")
    private String topic;

    private final KafkaTemplate<String, String> kafka;
    private DatagramSocket socket;
    private ExecutorService executor;

    public UdpKafkaForwarder(KafkaTemplate<String, String> kafka) {
        this.kafka = kafka;
    }

    @PostConstruct
    public void start() {
        executor = Executors.newSingleThreadExecutor(r -> new Thread(r, "udp-listener"));
        executor.submit(() -> {
            try (DatagramSocket ds = new DatagramSocket(udpPort)) {
                this.socket = ds;
                byte[] buf = new byte[65535];
                log.info("UDP listener started on port {}", udpPort);
                while (!Thread.currentThread().isInterrupted()) {
                    DatagramPacket pkt = new DatagramPacket(buf, buf.length);
                    ds.receive(pkt);
                    String payload = new String(pkt.getData(), pkt.getOffset(), pkt.getLength(), StandardCharsets.UTF_8);
                    String key = pkt.getAddress().getHostAddress(); // or parse device id from payload
                    kafka.send(topic, key, payload)
                         .whenComplete((result, ex) -> {
                             if (ex != null) {
                                 log.error("Kafka send failed", ex);
                             } else {
                                 log.debug("Sent to Kafka: topic={} key={}", topic, key);
                             }
                         });
                }
            } catch (IOException e) {
                log.error("UDP listener error", e);
            }
        });
    }

    @PreDestroy
    public void stop() {
        if (executor != null) executor.shutdownNow();
        if (socket != null) socket.close();
        log.info("UDP listener stopped");
    }
}
