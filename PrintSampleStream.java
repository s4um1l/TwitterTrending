
// to use this example, uncomment the twitter4j dependency information in the project.clj,
// uncomment storm.starter.spout.TwitterSampleSpout, and uncomment this class

package storm.starter;

import storm.starter.spout.TwitterSampleSpout2;
import storm.starter.bolt.IntermediateRankingsBolt;
import storm.starter.bolt.RollingCountBolt;
import storm.starter.bolt.TotalRankingsBolt;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import storm.starter.bolt.PrinterBolt;
import backtype.storm.tuple.Fields;
import storm.starter.bolt.TweetHashtagBolt;
import storm.starter.util.StormRunner;


public class PrintSampleStream {    
  private static final int DEFAULT_RUNTIME_IN_SECONDS = 600;
  private static final int TOP_N = 5;
  private final TopologyBuilder builder;
  private final String topologyName;
  private final Config topologyConfig;
  private final int runtimeInSeconds;
 public PrintSampleStream() throws InterruptedException {
    builder = new TopologyBuilder();
    topologyName = "twitter-hashtags";
    topologyConfig = createTopologyConfiguration();
    runtimeInSeconds = DEFAULT_RUNTIME_IN_SECONDS;

    wireTopology();
  }


 private static Config createTopologyConfiguration() {
    Config conf = new Config();
    conf.setDebug(true);
    return conf;
  }
private void wireTopology() throws InterruptedException {
       builder.setSpout("spout", new TwitterSampleSpout2("bigdatastorm1","Dallas@2015"));
      
	 builder.setBolt("hashtags", new TweetHashtagBolt())
                .shuffleGrouping("spout");
	builder.setBolt("print", new PrinterBolt())
                .shuffleGrouping("hashtags");
String spoutId = "hashtags";
     String counterId = "counter";
    String intermediateRankerId = "intermediateRanker";
    String totalRankerId = "finalRanker";
      builder.setBolt(counterId, new RollingCountBolt(9, 3), 4).fieldsGrouping(spoutId, new Fields("hashtags"));
    builder.setBolt(intermediateRankerId, new IntermediateRankingsBolt(TOP_N), 4).fieldsGrouping(counterId, new Fields(
        "obj"));
    builder.setBolt(totalRankerId, new TotalRankingsBolt(TOP_N)).globalGrouping(intermediateRankerId);


  }

  public void run() throws InterruptedException {
    StormRunner.runTopologyLocally(builder.createTopology(), topologyName, topologyConfig, runtimeInSeconds);
  }    
    public static void main(String[] args) throws Exception {
      new PrintSampleStream().run();

/*       // String username = args[0];
       // String pwd = args[1];
        TopologyBuilder builder = new TopologyBuilder();
        
        
        Config conf = new Config();
        
        
        LocalCluster cluster = new LocalCluster();
        
        cluster.submitTopology("test", conf, builder.createTopology());
        
        Utils.sleep(10000000);
        cluster.shutdown();*/
    }
}
