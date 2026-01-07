package com.aut25.vertx;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.core.shareddata.LocalMap;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.pcap4j.packet.EthernetPacket;
import org.pcap4j.packet.IpPacket;
import org.pcap4j.packet.IpV4Packet;
import org.pcap4j.packet.IpV6Packet;
import org.pcap4j.packet.Packet;
import org.pcap4j.packet.TcpPacket;
import org.pcap4j.packet.UdpPacket;
import org.pcap4j.packet.factory.PacketFactories;
import org.pcap4j.packet.namednumber.DataLinkType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.CopyOnWriteArrayList;

import com.aut25.vertx.services.DnsService;
import com.aut25.vertx.services.GeoIPService;
import com.aut25.vertx.services.WhoisService;
import com.aut25.vertx.utils.Colors;
import com.aut25.vertx.utils.Flow;
import com.aut25.vertx.utils.NDPIWrapper;
import com.aut25.vertx.utils.NdpiFlowWrapper;

import static java.lang.Thread.sleep;

import io.vertx.core.Future;
import io.vertx.core.Promise;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.consumer.KafkaConsumerRecords;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

public class PcapCoordinatorVerticle extends AbstractVerticle {

        private static final Logger logger = LoggerFactory.getLogger(PcapCoordinatorVerticle.class);
        private final Set<Integer> donePartitions = new HashSet<>();
        private int totalPartitions;

        @Override
        public void start() {

                fetchPartitionCount(vertx, "localhost:9092", "network-data")
                                .onSuccess(count -> {
                                        totalPartitions = count;

                                        logger.info("[ PCAP COORD VERTICLE ]           Topic network-data has {} partitions",
                                                        totalPartitions);

                                        startListening();
                                })
                                .onFailure(err -> {
                                        logger.error("[ PCAP COORD VERTICLE ]           Failed to fetch partition count", err);
                                        vertx.close(); // ou fail fast
                                });

        }

        private void startListening() {

                vertx.eventBus().consumer("pcap.partition.done", msg -> {

                        JsonObject body = (JsonObject) msg.body();
                        int partition = body.getInteger("partition");

                        if (donePartitions.add(partition)) {
                                logger.info("[ PCAP COORD VERTICLE ]           Partition {} done ({}/{})",
                                                partition, donePartitions.size(), totalPartitions);
                        }

                        if (donePartitions.size() == totalPartitions) {
                                logger.warn("[ PCAP COORD VERTICLE ]           ALL partitions done ({}/{}), publishing global done",
                                                donePartitions.size(), totalPartitions);
                                vertx.eventBus().publish("pcap.global.done", "");
                        }
                });
        }

        private Future<Integer> fetchPartitionCount(Vertx vertx, String bootstrapServers, String topic) {

                Promise<Integer> promise = Promise.promise();

                vertx.executeBlocking(blockingPromise -> {
                        Properties props = new Properties();
                        props.put("bootstrap.servers", bootstrapServers);

                        try (AdminClient admin = AdminClient.create(props)) {

                                DescribeTopicsResult result = admin.describeTopics(Collections.singletonList(topic));

                                TopicDescription description = result.values().get(topic).get();

                                int partitions = description.partitions().size();
                                blockingPromise.complete(partitions);

                        } catch (Exception e) {
                                blockingPromise.fail(e);
                        }

                }, false, res -> {
                        if (res.succeeded()) {
                                promise.complete((Integer) res.result());
                        } else {
                                promise.fail(res.cause());
                        }
                });

                return promise.future();
        }

}
