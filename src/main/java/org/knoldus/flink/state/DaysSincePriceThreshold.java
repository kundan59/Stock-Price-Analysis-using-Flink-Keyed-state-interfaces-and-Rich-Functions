package org.knoldus.flink.state;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * [[DaysSincePriceThreshold]] class contains the implementation of use case(Stock price Analysis)-
 * Number of Days passes between the threshold breaches using ListState Object.
 * <p>
 * Example: In the HDFC.csv file figure out the first two record
 * 2000-01-03,HDFC,EQ,271.75,293.5,293.5,293.5,293.5,293.5,293.5,22744,667536400000.0,,,
 * 2000-01-04,HDFC,EQ,293.5,317.0,317.0,297.0,304.0,304.05,303.62,255251,7749972375000.0,,,
 * <p>
 * if set a threshold 300 for closing price then on date '2000-01-04' breaches the threshold and in between
 * only on record passed so, number days will be 1. Output we will get the date on which threshold breaches
 * and the count of days passes in between.
 * (2000-01-04,1).
 */
public final class DaysSincePriceThreshold {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment executionEnvironment =
                StreamExecutionEnvironment.getExecutionEnvironment();

        // DataStream of Stock Information
        DataStream<String> hdfcStockDataStream =
                executionEnvironment.readTextFile("src/main/resources/HDFC.csv")
                        .filter(stockStream -> !stockStream.contains("Date,Symbol,Series,Prev Close,Open,High,Low," +
                                "Last,Close,VWAP,Volume,Turnover,Trades,Deliverable Volume,%Deliverble"));

        //Info interested in:  Date, Symbol(Ticker Name), Close Price
        //keyBy- Symbol(Ticker Name):HDFC
        //output Tuple2 of Date on which threshold breaches and the count of days passes in between.
        hdfcStockDataStream.map(hdfcStock -> {
            String[] stockTokens = hdfcStock.split(",");
            return new Tuple3<>(stockTokens[0], stockTokens[1], Double.parseDouble(stockTokens[8]));
        }, TypeInformation.of(new TypeHint<Tuple3<String, String, Double>>() {
        })).keyBy(((KeySelector<Tuple3<String, String, Double>, String>) tickerNameKeyBy ->
                        tickerNameKeyBy.f1),
                TypeInformation.of(new TypeHint<String>() {
                })).flatMap(new DaysCountSinceLastThresholdBreachFn()).print();

        executionEnvironment.execute("Flink List state Example");
    }

    static class DaysCountSinceLastThresholdBreachFn extends
            RichFlatMapFunction<Tuple3<String, String, Double>, Tuple2<String, Integer>> {

        //ListState Object to persist list of prices
        private transient ListState<Double> priceListState;

        @Override
        public void flatMap(Tuple3<String, String, Double> hdfcStockInfo,
                            Collector<Tuple2<String, Integer>> collector) throws Exception {

            if (hdfcStockInfo.f2 >= 300) {

                List<Double> priceList = StreamSupport
                        .stream(priceListState.get().spliterator(), false)
                        .collect(Collectors.toList());

                priceListState.clear();

                collector.collect(Tuple2.of(hdfcStockInfo.f0, priceList.size()));
            } else {
                priceListState.add(hdfcStockInfo.f2);
            }
        }

        @Override
        public void open(Configuration parameters) throws Exception {

            //Initializing ListState object via ListStateDescriptor
            // Parameters- name of value state: day-since-threshold-price
            // Type information(what structure ListState object accommodate):
            // TypeInformation.of(new TypeHint<Double>())- List of prices
            ListStateDescriptor<Double> listStateDescriptor =
                    new ListStateDescriptor<>(
                            "day-since-threshold-price",
                            TypeInformation.of(new TypeHint<Double>() {
                            }));

            // get handle on ListState Object
            priceListState = getRuntimeContext().getListState(listStateDescriptor);
        }

    }
}
