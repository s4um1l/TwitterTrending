package storm.starter.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.*;
import org.json.*;


public class TweetHashtagBolt extends BaseBasicBolt {

  @Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {
 if (tuple.getString(0).contains("hashtags") && !tuple.getString(0).contains("delete")){
    System.out.println("Checking here"+tuple.getString(0));
   JSONObject jsonObj =new JSONObject(tuple.getString(0));
   for(int i=0 ;i<jsonObj.getJSONObject("entities").getJSONArray("hashtags").length();i++){
				String[] a=jsonObj.getJSONObject("entities").getJSONArray("hashtags").getJSONObject(i).toString().split(",");
				
				String[] a1=a[0].split(":");
				
				
				collector.emit(new Values(a1[1].replaceAll("\"","")));
			}
  }
   }
  @Override
  public void declareOutputFields(OutputFieldsDeclarer ofd) {
  ofd.declare(new Fields("hashtags"));
  }

}
