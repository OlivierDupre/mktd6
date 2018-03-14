package io.monkeypatch.mktd6.server.gibber;

import io.monkeypatch.mktd6.kstreams.KafkaStreamsBoilerplate;
import io.monkeypatch.mktd6.model.gibber.Gibb;
import io.monkeypatch.mktd6.server.MonkonomyServer;
import io.monkeypatch.mktd6.topic.TopicDef;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.*;
import twitter4j.auth.AccessToken;

import java.util.Arrays;
import java.util.List;

import static io.monkeypatch.mktd6.kstreams.LaunchHelper.getLocalIp;

/**
 * This class reads data from Twitter and feeds the gibber topic
 * with it.
 */
public class GibberServer {

    private static final Logger LOG =
            LoggerFactory.getLogger(GibberServer.class);

    private static final String TWITTER_CONSUMER_KEY =
        System.getenv("TWITTER_CONSUMER_KEY");
    private static final String TWITTER_CONSUMER_SECRET =
        System.getenv("TWITTER_CONSUMER_SECRET");
    private static final String TWITTER_ACCESS_TOKEN =
        System.getenv("TWITTER_ACCESS_TOKEN");
    private static final String TWITTER_ACCESS_TOKEN_SECRET =
        System.getenv("TWITTER_ACCESS_TOKEN_SECRET");

    private final KafkaStreamsBoilerplate boilerplate;

    public GibberServer(KafkaStreamsBoilerplate boilerplate) {
        this.boilerplate = boilerplate;
    }

    public void run(List<String> twitterFilter) {
        KafkaProducer<String, Gibb> producer =
                new KafkaProducer<>(boilerplate.producerConfig(TopicDef.GIBBS, false));

        TwitterStream stream = getTwitterStream();
        stream.addListener(new StatusAdapter(){
            @Override
            public void onStatus(Status status) {
                //LOG.info("Adding gibb: {}", status.getText());
                String id = Long.toHexString(status.getId());
                DateTime time = DateTime.now(DateTimeZone.UTC);
                Gibb gibb = new Gibb(id, time, status.getText());
                producer.send(new ProducerRecord<>(
                    TopicDef.GIBBS.getTopicName(),
                    MonkonomyServer.ONE_KEY,
                    gibb));
            }
        });

        FilterQuery filterQuery = new FilterQuery()
                .track(twitterFilter.toArray(new String[]{}));
        stream.filter(filterQuery);
    }

    private TwitterStream getTwitterStream() {
        AccessToken oathAccessToken = new AccessToken(
                TWITTER_ACCESS_TOKEN,
                TWITTER_ACCESS_TOKEN_SECRET);
        TwitterStream stream = TwitterStreamFactory.getSingleton();
        stream.setOAuthConsumer(
                TWITTER_CONSUMER_KEY,
                TWITTER_CONSUMER_SECRET);
        stream.setOAuthAccessToken(oathAccessToken);
        return stream;
    }

    public static void main(String[] args) {
        new GibberServer(
            new KafkaStreamsBoilerplate(
                getLocalIp() + ":9092",
                "gibber-server")
        )
        .run(Arrays.asList(
            "banana",
            "mktd"));
    }
}
