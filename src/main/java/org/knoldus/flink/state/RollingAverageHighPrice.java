package org.knoldus.flink.state;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * [[RollingAverageHighPrice]] class contains the implementation of use case(Stock price Analysis)-
 * Compute Rolling Average High Price using ListState Object.
 * <p>
 * Here computing average high price for 50 days records
 * <p>
 * Output- Stock Symbol(Ticker name) and computed average high price for 50 days.
 * (HDFC,964.207)
 * <p>
 * Apart from HDFC there can be multiple stock ticker in dataset i.e, multiple keys in this example.
 */
public final class RollingAverageHighPrice {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment executionEnvironment =
                StreamExecutionEnvironment.getExecutionEnvironment();

        // DataStream of Stock Information
        DataStream<String> hdfcStockDataStream =
                executionEnvironment.readTextFile("src/main/resources/HDFC.csv")
                        .filter(stockStream -> !stockStream.contains("Date,Symbol,Series,Prev Close,Open,High,Low," +
                                "Last,Close,VWAP,Volume,Turnover,Trades,Deliverable Volume,%Deliverble"));

        //Info interested in:  Symbol(Ticker Name), High Price
        //keyBy- Symbol(Ticker Name):HDFC
        //output Tuple2 of stock ticker name and average high price for 50 days.
        hdfcStockDataStream.map(hdfcStock -> {
            String[] stockTokens = hdfcStock.split(",");
            return new Tuple2<>(stockTokens[1], Double.parseDouble(stockTokens[5]));
        }, TypeInformation.of(new TypeHint<Tuple2<String, Double>>() {
        })).keyBy(((KeySelector<Tuple2<String, Double>, String>) tickerNameKeyBy ->
                        tickerNameKeyBy.f0),
                TypeInformation.of(new TypeHint<String>() {
                })).flatMap(new RollingAverageHighPriceFn()).print();

        executionEnvironment.execute("Flink Reducing state Example");
    }

    static class RollingAverageHighPriceFn extends
            RichFlatMapFunction<Tuple2<String, Double>, Tuple2<String, Double>> {

        //ValueState Object to persist count of stock record seen so far.
        private transient ValueState<Integer> priceCountState;
        //ReducingState Object to persist average high price of passed 50 records.
        private transient ReducingState<Double> averageHighPriceState;

        @Override
        public void flatMap(Tuple2<String, Double> hdfcStockInfo,
                            Collector<Tuple2<String, Double>> collector) throws Exception {

            Integer priceCount = priceCountState.value();

            if (priceCount == null) {

                priceCountState.update(1);
                averageHighPriceState.add(hdfcStockInfo.f1);
            } else {

                if (priceCount < 50) {

                    priceCountState.update(priceCount + 1);
                    averageHighPriceState.add(hdfcStockInfo.f1);
                } else {

                    Double averageHighPrice = averageHighPriceState.get() / priceCount;
                    priceCountState.clear();
                    averageHighPriceState.clear();

                    collector.collect(Tuple2.of(hdfcStockInfo.f0, averageHighPrice));
                }
            }
        }

        @Override
        public void open(Configuration parameters) throws Exception {

            //Initializing valueState object via ValueStateDescriptor
            // Parameters- name of value state: Price-count
            // Type information(what structure ValueState object accommodate):
            // TypeInformation.of(new TypeHint<Integer>())- count of stock Record
            ValueStateDescriptor<Integer> valueStateDescriptor = new ValueStateDescriptor<>(
                    "Price-count",
                    TypeInformation.of(new TypeHint<Integer>() {
                    }));
            priceCountState = getRuntimeContext().getState(valueStateDescriptor);

            //Initializing ReducingState object via ReducingStateDescriptor
            // Parameters- name of value state: average-price
            // Reducing Function- what aggregation apply on Reducing state object
            // here adding the cumulative and current price
            ReducingStateDescriptor<Double> reducingState = new ReducingStateDescriptor<Double>(
                    "average-price",
                    Double::sum, Double.class);
            averageHighPriceState = getRuntimeContext().getReducingState(reducingState);
        }
    }
}
