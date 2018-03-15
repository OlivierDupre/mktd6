package mktd6;

import mktd6.kstreams.KafkaStreamsBoilerplate;
import mktd6.model.Team;
import mktd6.model.trader.Trader;
import mktd6.trader.TraderTopology;
import mktd6.trader.helper.TraderStores;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;

import java.util.Random;

import static mktd6.kstreams.LaunchHelper.getLocalIp;

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

    public static final String RANDOM = Integer.toString(new Random().nextInt(100000), 36);
    public static final String TRADER_APPLICATION_ID = String.format(
        "monkonomy-trader-%s-%s-%s", TRADER.getTeam(), TRADER.getName(), RANDOM);



    public static void main(String[] args) {
        KafkaStreamsBoilerplate helper = new KafkaStreamsBoilerplate(
            BOOTSTRAP_SERVER, TRADER_APPLICATION_ID);

        StreamsBuilder builder = new StreamsBuilder();

        TraderStores.TRADER_INVESTMENT_STORE.addTo(builder);

        KafkaStreams kafkaStreams = new KafkaStreams(
            new TraderTopology(TRADER)
                .apply(helper, builder)
                .build(),
            helper.streamsConfig(false, "latest"));

        kafkaStreams.start();
    }


}
