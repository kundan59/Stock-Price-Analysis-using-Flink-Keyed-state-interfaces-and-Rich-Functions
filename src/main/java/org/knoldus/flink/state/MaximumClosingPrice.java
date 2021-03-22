package org.knoldus.flink.state;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
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
 * [[MaximumClosingPrice]] class contains the implementation of use case(Stock price Analysis)-
 * Maximum closing price per year seen so far Using ValueState Object.
 */
public final class MaximumClosingPrice {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment executionEnvironment =
                StreamExecutionEnvironment.getExecutionEnvironment();

        // DataStream of Stock Information
        DataStream<String> hdfcStockDataStream =
                executionEnvironment.readTextFile("src/main/resources/HDFC.csv")
                        .filter(stockStream -> !stockStream.contains("Date,Symbol,Series,Prev Close,Open,High,Low," +
                                "Last,Close,VWAP,Volume,Turnover,Trades,Deliverable Volume,%Deliverble"));

        //Info interested in: Year(extracted from the date), Close Price
        //keyBy- Year
        //output Tuple2 of Year and maximum closing price of the Year seen so far.
        hdfcStockDataStream.map(hdfcStock -> {
            String[] stockTokens = hdfcStock.split(",");
            return new Tuple2<>(Integer.parseInt(stockTokens[0].substring(0, 4)),
                    Double.parseDouble(stockTokens[8]));
        }, TypeInformation.of(new TypeHint<Tuple2<Integer, Double>>() {
        })).keyBy(((KeySelector<Tuple2<Integer, Double>, Integer>) hdfcKeyBy ->
                hdfcKeyBy.f0), TypeInformation.of(new TypeHint<Integer>() {
        })).flatMap(new MaxClosingPriceFunction()).print();

        executionEnvironment.execute("Flink Value state Example: Maximum-Closing-Price");
    }

    static class MaxClosingPriceFunction extends
            RichFlatMapFunction<Tuple2<Integer, Double>, Tuple2<Integer, Double>> {

        //ValueState object
        private transient ValueState<Tuple2<Integer, Double>> maxClose;

        @Override
        public void flatMap(Tuple2<Integer, Double> hdfcStockInfo,
                            Collector<Tuple2<Integer, Double>> collector) throws Exception {

            Tuple2<Integer, Double> maxClosingPrice = maxClose.value();

            if (maxClosingPrice == null)
                maxClose.update(Tuple2.of(hdfcStockInfo.f0, hdfcStockInfo.f1));
            else if (hdfcStockInfo.f1 > maxClosingPrice.f1)
                maxClose.update(Tuple2.of(hdfcStockInfo.f0, hdfcStockInfo.f1));

            collector.collect(maxClose.value());
        }

        @Override
        public void open(Configuration parameters) throws Exception {

            //Initializing ValueState object via ValueStateDescriptor
            // Parameters- name of value state: max-closing-price
            // Type information(what structure ValueState object accommodate):
            // TypeInformation.of(new TypeHint<Tuple2<Integer, Double>>()) - year and closing price
            ValueStateDescriptor<Tuple2<Integer, Double>> maxCloseValueStateDescriptor =
                    new ValueStateDescriptor<>("max-closing-price",
                            TypeInformation.of(new TypeHint<Tuple2<Integer, Double>>() {
                            }));

            // get handle on ValueState Object
            maxClose = getRuntimeContext().getState(maxCloseValueStateDescriptor);
        }
    }
}
