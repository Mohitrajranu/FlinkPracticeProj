package main.java.com.flink.dataingestion.twitter;

/* java imports */
import java.util.List;
import java.util.Arrays;
import java.util.Properties;
/* flink imports */
import org.apache.flink.util.Collector;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
/* parser imports */
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
/* flink streaming twittter imports */
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

//import org.apache.flink.api.java.tuple.Tuple2;

import org.apache.flink.core.fs.Path;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

public class Twitter_UseCase {

    public static void main(String[] args) throws Exception {
        final List < String > keywords = Arrays.asList("global warming", "pollution", "save earth", "temperature increase", "weather change",
                "climate", "co2", "air quality", "dust", "carbondioxide", "greenhouse", "ozone", "methane", "sealevel", "sea level");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties twitterCredentials = new Properties();
        twitterCredentials.setProperty(TwitterSource.CONSUMER_KEY, "AJcUxpyVsUOk");
        twitterCredentials.setProperty(TwitterSource.CONSUMER_SECRET, "8XzIDXlVLQANLu2OYSfhEqFsSEKI12");
        twitterCredentials.setProperty(TwitterSource.TOKEN, "183234343-U6hOdLRXvWbGAMGZ5ZfSL");
        twitterCredentials.setProperty(TwitterSource.TOKEN_SECRET, "S5m6OEZ7qtgJV52t47Qt7Ze");

        DataStream < String > twitterData = env.addSource(new TwitterSource(twitterCredentials));

        DataStream < JsonNode > parsedData = twitterData.map(new TweetParser());

        DataStream < JsonNode > englishTweets = parsedData.filter(new EnglishFilter());

        DataStream < JsonNode > RelevantTweets = englishTweets.filter(new FilterByKeyWords(keywords));

        // Format: <source, tweetObject>
        DataStream < Tuple2 < String, JsonNode >> tweetsBySource = RelevantTweets.map(new ExtractTweetSource());

        // Format: <source, hourOfDay, 1>
        tweetsBySource.map(new ExtractHourOfDay())
                .keyBy(t -> Tuple2.of(t.f0, t.f1)) // groupBy source and hour
                .sum(2) // sum for each category i.e. Number of tweets from 'source' in given 'hour'
                .addSink(StreamingFileSink
                        .forRowFormat(new Path("/home/jivesh/tweets.txt"),
                                new SimpleStringEncoder < Tuple3 < String, String, Integer >> ("UTF-8"))
                        .withRollingPolicy(DefaultRollingPolicy.builder().build())
                        .build());
        //.writeAsText("/home/jivesh/tweets.txt");
        // e.g. 100 tweets from Android about Pollution in 16th hour of day
        //      150 tweets from Apple devices about Pollution in 20th hour of day etc.

        env.execute("Twitter Analysis");
    }

    public static class TweetParser implements MapFunction < String, JsonNode > {

        public JsonNode map(String value) throws Exception {
            ObjectMapper jsonParser = new ObjectMapper();

            JsonNode node = jsonParser.readValue(value, JsonNode.class);
            return node;
        }
    }

    public static class EnglishFilter implements FilterFunction < JsonNode > {
        public boolean filter(JsonNode node) {
            boolean isEnglish =
                    node.has("user") &&
                            node.get("user").has("lang") &&
                            node.get("user").get("lang").asText().equals("en");
            return isEnglish;
        }
    }

    public static class FilterByKeyWords implements FilterFunction < JsonNode > {
        private final List < String > filterKeyWords;

        public FilterByKeyWords(List < String > filterKeyWords) {
            this.filterKeyWords = filterKeyWords;
        }

        public boolean filter(JsonNode node) {
            if (!node.has("text"))
                return false;
            // keep tweets metioning keywords
            String tweet = node.get("text").asText().toLowerCase();

            return filterKeyWords.parallelStream().anyMatch(tweet::contains);
        }
    }

    public static class ExtractTweetSource implements MapFunction < JsonNode, Tuple2 < String, JsonNode >> {
        public Tuple2 < String,
                JsonNode > map(JsonNode node) {
            String source = "";
            if (node.has("source")) {
                String sourceHtml = node.get("source").asText().toLowerCase();
                if (sourceHtml.contains("ipad") || sourceHtml.contains("iphone"))
                    source = "AppleMobile";
                else if (sourceHtml.contains("mac"))
                    source = "AppleMac";
                else if (sourceHtml.contains("android"))
                    source = "Android";
                else if (sourceHtml.contains("BlackBerry"))
                    source = "BlackBerry";
                else if (sourceHtml.contains("web"))
                    source = "Web";
                else
                    source = "Other";
            }
            return new Tuple2 < String, JsonNode > (source, node); // returns  (Android,tweet)
        }
    }

    public static class ExtractHourOfDay implements MapFunction < Tuple2 < String, JsonNode > , Tuple3 < String, String, Integer >> {
        public Tuple3 < String,
                String,
                Integer > map(Tuple2 < String, JsonNode > value) {
            JsonNode node = value.f1;
            String timestamp = node.get("created_at").asText(); //Thu May 10 15:24:15 +0000 2018
            String hour = timestamp.split(" ")[3].split(":")[0] + "th hour";
            return new Tuple3 < String, String, Integer > (value.f0, hour, 1);
        }
    }
}