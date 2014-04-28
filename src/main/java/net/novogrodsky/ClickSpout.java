package net.novogrodsky;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.apache.log4j.Logger;
import org.json.simple.JSONObject;
import org.mortbay.util.ajax.JSON;
import redis.clients.jedis.Jedis;

import java.util.Map;

/**
 * Created by david.j.novogrodsky on 4/27/2014.
 */
public class ClickSpout extends BaseRichSpout {
    public static Logger LOG = Logger.getLogger(ClickSpout.class);

    private Jedis jedis;
    private String host;
    private int port;
    private SpoutOutputCollector collector;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(net.novogrodsky.Fields.IP,
                net.novogrodsky.Fields.URL, net.novogrodsky.Fields.CLIENT_KEY));
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        host = map.get(Conf.REDIS_HOST_KEY).toString();
        port = Integer.valueOf( map.get(Conf.REDIS_PORT_KEY).toString());
        this.collector = spoutOutputCollector;
        connectToRedis();
    }

    @Override
    public void nextTuple() {
        String content = jedis.rpop("count");
        if (content == null || "nil".equals(content)) {
            try {
                Thread.sleep(300);
            }catch(InterruptedException e) {
                LOG.info("problem with time out for queue", e);
            }


        } else {
            JSONObject incomingObject = (JSONObject) JSON.parse(content);
            String ip = incomingObject.get(net.novogrodsky.Fields.IP).toString();
            String url = incomingObject.get(net.novogrodsky.Fields.URL).toString();
            String clientKey = incomingObject.get(net.novogrodsky.Fields.CLIENT_KEY).toString();

            // here is where we populate the values
            // must match order defined in the declareOutputfields method
            collector.emit(new Values(ip, url, clientKey));
        }


    }

    private void connectToRedis() {
        jedis = new Jedis(host,port);
    }
}
