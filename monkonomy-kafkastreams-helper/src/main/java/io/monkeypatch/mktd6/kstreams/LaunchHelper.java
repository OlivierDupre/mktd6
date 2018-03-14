package io.monkeypatch.mktd6.kstreams;

import io.monkeypatch.mktd6.topic.TopicDef;
import kafka.admin.AdminUtils;
import kafka.utils.ZkUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Enumeration;
import java.util.List;

public class LaunchHelper {

    private static final Logger LOG = LoggerFactory.getLogger(LaunchHelper.class);

    public static String getLocalIp() {
        try {
            Enumeration<NetworkInterface> n = NetworkInterface.getNetworkInterfaces();
            for (; n.hasMoreElements(); ) {
                NetworkInterface e = n.nextElement();
                Enumeration<InetAddress> a = e.getInetAddresses();
                for (; a.hasMoreElements(); ) {
                    InetAddress addr = a.nextElement();
                    String hostAddress = addr.getHostAddress();
                    if (hostAddress.startsWith("192.")) {
                        return hostAddress;
                    }
                }
            }
        }
        catch(Exception e) {
            LOG.error(e.getMessage(), e);
        }
        return "localhost";
    }

    public static boolean allTopicsExist(ZkUtils zkUtils, List<TopicDef<?, ?>> topicDefs) {
        return topicDefs.stream()
                .map(TopicDef::getTopicName)
                .allMatch(t -> AdminUtils.topicExists(zkUtils, t));
    }

}
