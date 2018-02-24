package io.monkeypatch.twitter2kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import twitter4j.*;
import twitter4j.auth.AccessToken;

import java.util.Properties;


public class TwitterKafkaProducer {

    private static final String TWITTER_CONSUMER_KEY = System.getenv("TWITTER_CONSUMER_KEY");
    private static final String TWITTER_CONSUMER_SECRET = System.getenv("TWITTER_CONSUMER_SECRET");
    private static final String TWITTER_ACCESS_TOKEN = System.getenv("TWITTER_ACCESS_TOKEN");
    private static final String TWITTER_ACCESS_TOKEN_SECRET = System.getenv("TWITTER_ACCESS_TOKEN_SECRET");
    private static final String topic = "twitter-feed";

    public static void run() throws InterruptedException {

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "172.16.238.3:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getCanonicalName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getCanonicalName());
        properties.put(ProducerConfig.CLIENT_ID_CONFIG,
                "camus");

        final KafkaProducer<String, String> producer =
                new KafkaProducer<String, String>(properties);

        AccessToken oathAccessToken = new AccessToken(TWITTER_ACCESS_TOKEN, TWITTER_ACCESS_TOKEN_SECRET);
        FilterQuery filterQuery = new FilterQuery();
        filterQuery.track(new String[]{"banana"});
        TwitterStream stream = TwitterStreamFactory.getSingleton();
        stream.setOAuthConsumer(TWITTER_CONSUMER_KEY, TWITTER_CONSUMER_SECRET);
        stream.setOAuthAccessToken(oathAccessToken);

        final ObjectMapper mapper = new ObjectMapper();

        stream.addListener(new StatusAdapter(){
            @Override
            public void onStatus(Status status) {
            try {
                String json = mapper.writeValueAsString(status);
                System.out.println(json);
                producer.send(new ProducerRecord<String, String>(topic, status.getSource()));
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
            }
        });
        stream.filter(filterQuery);
    }

    public static void main(String[] args) {
        try {
            TwitterKafkaProducer.run();
        } catch (InterruptedException e) {
            System.out.println(e);
        }
    }
}