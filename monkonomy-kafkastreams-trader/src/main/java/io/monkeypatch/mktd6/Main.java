package io.monkeypatch.mktd6;

import io.monkeypatch.mktd6.kstreams.KafkaStreamsBoilerplate;
import io.monkeypatch.mktd6.model.Team;
import io.monkeypatch.mktd6.model.trader.Trader;
import io.monkeypatch.mktd6.trader.TraderTopology;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;

import static io.monkeypatch.mktd6.kstreams.LaunchHelper.getLocalIp;

public class Main {

    public static final Trader TRADER = new Trader(
        Team.SAGOUIN, //##################### CHANGE YOUR TEAM WHEN YOU'RE READY
        "my-player-name"
    );

    //*/
    public static final String KAFKA_HOST = getLocalIp(); // Local server
    /*/
    public static final String KAFKA_HOST = "192.168.?.?"; // Change this
    //*/

    public static final String KAFKA_PORT = "9092";

    public static final String BOOTSTRAP_SERVER = KAFKA_HOST + ":" + KAFKA_PORT;

    public static final String TRADER_APPLICATION_ID = String.format(
        "monkonomy-trader-%s-%s", TRADER.getTeam(), TRADER.getName());



    public static void main(String[] args) {
        KafkaStreamsBoilerplate helper = new KafkaStreamsBoilerplate(
            BOOTSTRAP_SERVER, TRADER_APPLICATION_ID);

        KafkaStreams kafkaStreams = new KafkaStreams(
            new TraderTopology(TRADER)
                .apply(helper, new StreamsBuilder())
                .build(),
            helper.streamsConfig(false));

        kafkaStreams.start();
    }


}
