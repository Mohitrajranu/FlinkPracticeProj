package main.java.com.flink.dataingestion.twitter;

/* java imports */

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.util.Collector;

import java.util.Properties;

/* flink imports */
/* parser imports */
/* flink streaming twittter imports */

public class TwitterData {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties twitterCredentials = new Properties();
        twitterCredentials.setProperty(TwitterSource.CONSUMER_KEY, "AJcUteyVsUOk");
        twitterCredentials.setProperty(TwitterSource.CONSUMER_SECRET, "8XzIDXlVLQA2OYSfhEqFsSEKI12");
        twitterCredentials.setProperty(TwitterSource.TOKEN, "183234343-U6hOdLxBRGa9sGAMGZ5ZfSL");
        twitterCredentials.setProperty(TwitterSource.TOKEN_SECRET, "S5m6OEZ7q5QuOqt47Qt7Ze");

        DataStream < String > twitterData = env.addSource(new TwitterSource(twitterCredentials));

        twitterData.flatMap(new TweetParser())
                .addSink(StreamingFileSink
                        .forRowFormat(new Path("/home/jivesh/tweet"),
                                new SimpleStringEncoder < Tuple2 < String, Integer >> ("UTF-8"))
                        .withRollingPolicy(DefaultRollingPolicy.builder().build())
                        .build());

        env.execute("Twitter Example");
    }

    public static class TweetParser implements FlatMapFunction < String, Tuple2 < String, Integer >> {

        public void flatMap(String value, Collector < Tuple2 < String, Integer >> out) throws Exception {
            ObjectMapper jsonParser = new ObjectMapper();
            JsonNode node = jsonParser.readValue(value, JsonNode.class);

            boolean isEnglish =
                    node.has("user") &&
                            node.get("user").has("lang") &&
                            node.get("user").get("lang").asText().equals("en");

            boolean hasText = node.has("text");

            if (isEnglish && hasText) {
                String tweet = node.get("text").asText();

                out.collect(new Tuple2 < String, Integer > (tweet, 1));
            }
        }
    }
}