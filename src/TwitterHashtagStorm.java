/**
 * Created by derek_000 on 16/02/16.
 */
import java.util.*;

import backtype.storm.tuple.Fields;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import joptsimple.OptionParser;
import joptsimple.OptionSet;


public class TwitterHashtagStorm {
    public static void main(String[] args) throws Exception{
        String consumerKey = "XZ7tdGXZwryywGN2uLgkFT08T";
        String consumerSecret = "EqJr2IQC9zAXlsKGGsIN47LeVagHfupiAeA98rcD7vodZHZbr5";

        String accessToken = "55706879-AWgf4C3sUBsxKJxWZj1de8fAwtNO5ZU64IN44gQcw";
        String accessTokenSecret = "9uejCNcIdVhb5NzhTV4WKkd2KU4zhM57X50gTT0m4sACf";

        OptionParser parser = new OptionParser();
        parser.accepts("tag1").withRequiredArg().ofType(String.class);
        parser.accepts("tag2").withOptionalArg().ofType(String.class);

        OptionSet options = parser.parse(args);

        ArrayList<String> tags = new ArrayList<>();

        tags.add((String)options.valueOf("tag1"));
        tags.add((String)options.valueOf("tag2"));


        Config config = new Config();
        config.setDebug(false);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("twitter-spout", new TwitterSamplerSpout(consumerKey,
                consumerSecret, accessToken, accessTokenSecret, tags));

        builder.setBolt("twitter-hashtag-reader-bolt", new HashtagReaderBolt())
                .shuffleGrouping("twitter-spout");

        builder.setBolt("twitter-hashtag-counter-bolt", new HashtagCounterBolt())
                .fieldsGrouping("twitter-hashtag-reader-bolt", new Fields("hashtag"));

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("TwitterHashtagStorm", config,
                builder.createTopology());
       Thread.sleep(100000);
       cluster.shutdown();
    }
}
