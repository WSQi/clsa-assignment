package com.clsa.assignment;

import com.clsa.assignment.dto.MarketData;
import com.clsa.assignment.service.MarketDataProcessor;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(MockitoExtension.class)
public class MarketDataProcessorTest {

    private static final String SYMBOL = "AAPL";

    @Test
    public void testPublishAggregatedMarketDataThrottling() {
        MarketDataProcessor processor = new MarketDataProcessor();
        for (int i = 0; i < 110; i++) {
            processor.onMessage(
                    MarketData.builder()
                            .symbol(SYMBOL)
                            .price(BigDecimal.ONE)
                            .updateTime(LocalDateTime.now())
                            .build());
        }
        assertEquals(100, processor.getCallsWithinSlidingWindow());
    }

    @Test
    public void testOnMessageUpdateOncePerSlidingWindow() {
        MarketDataProcessor processor = new MarketDataProcessor();
        processor.onMessage(
                MarketData.builder()
                        .symbol(SYMBOL)
                        .price(BigDecimal.ONE)
                        .updateTime(LocalDateTime.now())
                        .build());
        processor.onMessage(
                MarketData.builder()
                        .symbol(SYMBOL)
                        .price(BigDecimal.TEN)
                        .updateTime(LocalDateTime.now())
                        .build());
        assertEquals(BigDecimal.ONE, processor.getLatestMarketDataBySymbol(SYMBOL).getPrice());
    }
}
