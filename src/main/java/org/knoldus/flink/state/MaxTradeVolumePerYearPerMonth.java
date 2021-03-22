package org.knoldus.flink.state;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * [[MaxTradeVolumePerYearPerMonth]] class contains the implementation of use case(Stock price Analysis)-
 * Compute Maximum Trade Volume Per Year Per month using MapState Object.
 * <p>
 * output- Year, month, and current maximum of Trade volume in this month of the year.
 * (2000,1,22744)
 * (2000,1,255251)
 */
public final class MaxTradeVolumePerYearPerMonth {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment executionEnvironment =
                StreamExecutionEnvironment.getExecutionEnvironment();

        // DataStream of Stock Information
        DataStream<String> hdfcStockDataStream =
                executionEnvironment.readTextFile("src/main/resources/HDFC.csv")
                        .filter(stockStream -> !stockStream.contains("Date,Symbol,Series,Prev Close,Open,High,Low," +
                                "Last,Close,VWAP,Volume,Turnover,Trades,Deliverable Volume,%Deliverble"));

        //Info interested in:  Date, Volume
        //keyBy- Tuple2(Year, month)
        //output Tuple3 of Year, month, and current maximum of Trade volume in this month of the year.
        hdfcStockDataStream.map(hdfcStock -> {
            String[] stockTokens = hdfcStock.split(",");
            return new Tuple3<>(Integer.parseInt(stockTokens[0].substring(0, 4)),
                    Integer.parseInt(stockTokens[0].substring(5, 7)), Long.parseLong(stockTokens[10]));
        }, TypeInformation.of(new TypeHint<Tuple3<Integer, Integer, Long>>() {
        })).keyBy(((KeySelector<Tuple3<Integer, Integer, Long>, Tuple2<Integer, Integer>>) hdfcTickerKeyBy ->
                        Tuple2.of(hdfcTickerKeyBy.f0, hdfcTickerKeyBy.f1)),
                TypeInformation.of(new TypeHint<Tuple2<Integer, Integer>>() {
                })).flatMap(new MaxTradeVolumePerYearPerMonthFn()).print();

        executionEnvironment.execute("Flink Map state Example");
    }

    static class MaxTradeVolumePerYearPerMonthFn extends
            RichFlatMapFunction<Tuple3<Integer, Integer, Long>, Tuple3<Integer, Integer, Long>> {

        private transient MapState<Integer, Long> maxTradeVolumePerMonthState;

        @Override
        public void flatMap(Tuple3<Integer, Integer, Long> hdfcStockInfo,
                            Collector<Tuple3<Integer, Integer, Long>> collector) throws Exception {


            if (maxTradeVolumePerMonthState.isEmpty())
                maxTradeVolumePerMonthState.put(hdfcStockInfo.f1, hdfcStockInfo.f2);
            else if (hdfcStockInfo.f2 > maxTradeVolumePerMonthState.get(hdfcStockInfo.f1))
                maxTradeVolumePerMonthState.put(hdfcStockInfo.f1, hdfcStockInfo.f2);
            collector.collect(Tuple3.of(hdfcStockInfo.f0, hdfcStockInfo.f1,
                    maxTradeVolumePerMonthState.get(hdfcStockInfo.f1)));
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            MapStateDescriptor<Integer, Long> maxTradeVolumePerMonthStateDescriptor = new MapStateDescriptor<>(

                    //Initializing MapState object via MapStateDescriptor
                    // Parameters- name of Map state: max-Trade-Volume-Per-Year
                    // Type information of key of the map:
                    // TypeInformation.of(new TypeHint<Integer>())- month,
                    // Type information of Value of the map:
                    // TypeInformation.of(new TypeHint<Long>())- volume
                    "max-Trade-Volume-Per-Year",
                    TypeInformation.of(new TypeHint<Integer>() {
                    }),
                    TypeInformation.of(new TypeHint<Long>() {
                    })
            );

            // get handle on ValueState Object
            maxTradeVolumePerMonthState = getRuntimeContext().getMapState(maxTradeVolumePerMonthStateDescriptor);
        }
    }
}
