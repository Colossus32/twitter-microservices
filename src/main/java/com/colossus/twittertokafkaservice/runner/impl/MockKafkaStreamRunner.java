package com.colossus.twittertokafkaservice.runner.impl;

import com.colossus.twittertokafkaservice.config.TwitterToKafkaServiceConfigData;
import com.colossus.twittertokafkaservice.exception.TwitterToKafkaServiceException;
import com.colossus.twittertokafkaservice.listener.TwitterKafkaStatusListener;
import com.colossus.twittertokafkaservice.runner.StreamRunner;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;

@Component
@Slf4j
@RequiredArgsConstructor
@ConditionalOnProperty(name = "twitter-to-kafka-service.enable-mock-tweets", havingValue = "true")
public class MockKafkaStreamRunner implements StreamRunner {
    private final TwitterToKafkaServiceConfigData configData;
    private final TwitterKafkaStatusListener listener;
    private static final Random RANDOM = new Random();
    private static final String[] WORDS =
            "Lorem ipsum dolor sit amet, consectetur adipiscing elit sed do eiusmod tempor incididunt ut labore et dolore magna aliqua"
                    .split(" ");
    private static final String tweetAsRawJson = "{" +
            "\"created_at\":\"{0}\"," +
            "\"id\":\"{1}\"," +
            "\"text\":\"{2}\"," +
            "\"user\":{\"id\":\"{3}\"}" +
            "}";
    private static final String TWITTER_STATUS_DATE_FORMAT = "EEE MMM dd HH:mm:ss zzz yyyy";

    @Override
    public void start() throws TwitterException {
        String[] keywords = configData.getTwitterKeywords().toArray(new String[0]);
        int minTweetLength = configData.getMockMinTweetLength();
        int maxTweetLength = configData.getMockMaxTweetLength();
        long sleepTimeMs = configData.getMockSleepMs();

        log.info("Starting mock filtering stream for keywords {}", Arrays.toString(keywords));
        infinityLoop(keywords, minTweetLength, maxTweetLength, sleepTimeMs);
    }

    private void infinityLoop(String[] keywords, int minTweetLength, int maxTweetLength, long sleepTimeMs){

        Executors.newSingleThreadExecutor().submit(() -> {
            try {
                while (true) {
                    String formattedTweetAsRawJson = getFormattedTweet(keywords, minTweetLength, maxTweetLength);
                    Status status = TwitterObjectFactory.createStatus(formattedTweetAsRawJson);
                    listener.onStatus(status);
                    sleep(sleepTimeMs);
                }
            } catch (TwitterException e) {
                log.error("Error creating tweet status.");
                e.printStackTrace();
            }
        });

    }

    private void sleep(long sleepTimeMs) {
        try {
            Thread.sleep(sleepTimeMs);
        } catch (InterruptedException e) {
            throw new TwitterToKafkaServiceException("Error while sleeping for waiting new status to create");
        }
    }

    private String getFormattedTweet(String[] keywords, int minTweetLength, int maxTweetLength) {
        String[] param = new String[] {
                ZonedDateTime.now().format(DateTimeFormatter.ofPattern(TWITTER_STATUS_DATE_FORMAT, Locale.ENGLISH)),
                        String.valueOf(ThreadLocalRandom.current().nextLong(Long.MAX_VALUE)),
                        getRandomTweetContent(keywords,minTweetLength, maxTweetLength),
                        String.valueOf(ThreadLocalRandom.current().nextLong(Long.MAX_VALUE))
        };
        return formatTweetAsJsonWithParams(param);
    }

    private static String formatTweetAsJsonWithParams(String[] param) {
        String tweet = tweetAsRawJson;
        for (int i = 0; i < param.length; i++) {
            tweet = tweet.replace("{" + i + "}", param[i]);
        }
        return tweet;
    }

    private String getRandomTweetContent(String[] keywords, int minTweetLength, int maxTweetLength) {
        StringBuilder tweet = new StringBuilder();
        int tweetLength = RANDOM.nextInt(maxTweetLength - minTweetLength + 1) + minTweetLength;
        return constructRandomTweet(keywords, tweet, tweetLength);
    }

    private static String constructRandomTweet(String[] keywords, StringBuilder tweet, int tweetLength) {

        for (int i = 0; i < tweetLength; i++) {
            tweet.append(WORDS[RANDOM.nextInt(WORDS.length)]).append(' ');
            if (i == tweetLength / 2) tweet.append(keywords[RANDOM.nextInt(keywords.length)]).append(' ');
        }

        return tweet.toString().trim();
    }
}
