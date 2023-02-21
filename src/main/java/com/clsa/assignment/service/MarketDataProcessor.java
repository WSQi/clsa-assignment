package com.clsa.assignment.service;

import com.clsa.assignment.dto.MarketData;

import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MarketDataProcessor {

    private static final long SLIDING_WINDOW = 1000;
    private Map<String, MarketData> latestMarketDataMap = new ConcurrentHashMap<>();
    private long callsWithinSlidingWindow = 0;
    private long slidingWindowStart = System.currentTimeMillis();

    /**
     * Receive incoming market data
     * @param data
     */
    public void onMessage(MarketData data) {
        updateMarketData(data);
        throttlePublishAggregatedMarketData(data);
    }

    private void updateMarketData(MarketData data) {
        MarketData latestMarketData = this.latestMarketDataMap.get(data.getSymbol());
        // each symbol does not update more than once per sliding window
        if (latestMarketData != null &&
                ChronoUnit.MILLIS.between(data.getUpdateTime(), latestMarketData.getUpdateTime()) < SLIDING_WINDOW) {
            return;
        }

        // store latest market data for the incoming symbol
        this.latestMarketDataMap.put(data.getSymbol(), data);
    }

    private void throttlePublishAggregatedMarketData(MarketData data) {
        // Check if the number of calls of publishAggregatedMarketData method
        // for publishing messages does not exceed 100 times per second
        if (callsWithinSlidingWindow >= 100) {
            long currentTime = System.currentTimeMillis();
            if (currentTime - slidingWindowStart >= 1000) {
                callsWithinSlidingWindow = 0;
                slidingWindowStart = currentTime;
            } else {
                return;
            }
        }

        publishAggregatedMarketData(latestMarketDataMap.get(data));

        // Increase the number of calls in the sliding window
        callsWithinSlidingWindow++;
    }

    /**
     * Publish aggregated and throttled market data
     * @param data
     */
    public void publishAggregatedMarketData(MarketData data) {
        // Do Nothing, assume implemented.
    }

    /**
     * Get calls within sliding window
     * @return
     */
    public long getCallsWithinSlidingWindow() {
        return this.callsWithinSlidingWindow;
    }

    /**
     * Get latest market data by symbol
     * @param symbol
     * @return
     */
    public MarketData getLatestMarketDataBySymbol(String symbol) {
        return this.latestMarketDataMap.get(symbol);
    }
}
