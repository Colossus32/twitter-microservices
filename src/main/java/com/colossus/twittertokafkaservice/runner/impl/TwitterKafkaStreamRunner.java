package com.colossus.twittertokafkaservice.runner.impl;

import com.colossus.config.TwitterToKafkaServiceConfigData;
import com.colossus.twittertokafkaservice.listener.TwitterKafkaStatusListener;
import com.colossus.twittertokafkaservice.runner.StreamRunner;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import twitter4j.FilterQuery;
import twitter4j.TwitterException;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;

import javax.annotation.PreDestroy;
import java.util.Arrays;

@Component
@RequiredArgsConstructor
@Slf4j
@ConditionalOnProperty(name = "twitter-to-kafka-service.enable-mock-tweets", havingValue = "false", matchIfMissing = true)
public class TwitterKafkaStreamRunner implements StreamRunner {

    private final TwitterToKafkaServiceConfigData configData;
    private final TwitterKafkaStatusListener listener;
    private TwitterStream twitterStream;
    @Override
    public void start() throws TwitterException {

        twitterStream = new TwitterStreamFactory().getInstance();
        twitterStream.addListener(listener);
        addFilter();
    }

    @PreDestroy
    public void shutdown() {
        if (twitterStream != null) {
            log.info("Closing twitter stream");
            twitterStream.shutdown();
        }
    }

    private void addFilter() {
        String[] keywords = configData.getTwitterKeywords().toArray(new String[0]);
        FilterQuery filterQuery = new FilterQuery(keywords);
        twitterStream.filter(filterQuery);
        log.info("Started filtering twitter stream for keywords {}", Arrays.toString(keywords));
    }
}
