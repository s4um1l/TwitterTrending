

package storm.starter.spout;

import backtype.storm.Config;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesSampleEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.BasicClient;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.LinkedBlockingQueue;


public class TwitterSampleSpout2 extends BaseRichSpout {
    SpoutOutputCollector _collector;
    LinkedBlockingQueue<String> queue = null;
   BasicClient client;
    String _username;
    String _pwd;
    
    
    public TwitterSampleSpout2(String username, String pwd) {
        _username = username;
        _pwd = pwd;
    }
    
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        queue = new LinkedBlockingQueue<String>(1000);
        _collector = collector;
       	 StatusesSampleEndpoint endpoint = new StatusesSampleEndpoint();
    endpoint.stallWarnings(false);

    Authentication auth = new OAuth1("cmmYapikNZUGaTTFYc10eA", "XQ2Dafvn0KGJKD1uazKq2p6hPGbUoBnxOIK3dMfG8", "2249812004-kD5j1o2SpVmLsOZ1Enh7HwYFvEFDGPBiAml6K5l", "waTHMDtza9EdzgMToDGwWdgxTUaEmT3PCbSeYMJtheAnv");
   
    // Create a new BasicClient. By default gzip is enabled.
     client = new ClientBuilder()
      .name("twitterClient")
      .hosts(Constants.STREAM_HOST)
      .endpoint(endpoint)
      .authentication(auth)
      .processor(new StringDelimitedProcessor(queue))
      .build();

    // Establish a connection
 	client.connect();
   
	
    }

    @Override
    public void nextTuple() {
      if (client.isDone()) {
 //       System.out.println("Client connection closed unexpectedly: " + client.getExitEvent().getMessage());
        Utils.sleep(100);
      }
	
      try{
      String msg = queue.poll(5, TimeUnit.SECONDS);
    
      if (msg == null) {
                Utils.sleep(100);
      } else {
         _collector.emit(new Values(msg));
      }}
      catch (Exception e){
	}
                  
        
    }

    @Override
    public void close() {
         client.stop();
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config ret = new Config();
        ret.setMaxTaskParallelism(1);
        return ret;
    }    

    @Override
    public void ack(Object id) {
    }

    @Override
    public void fail(Object id) {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tweet"));
    }
    
}
