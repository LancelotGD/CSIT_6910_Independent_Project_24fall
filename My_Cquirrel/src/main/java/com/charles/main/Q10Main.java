package com.charles.main;

import com.charles.RelationType.Payload;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;

public class Q10Main {
    public static final OutputTag<Payload> lineitemTag = new OutputTag<Payload>("lineitem"){};
    public static final OutputTag<Payload> orderTag = new OutputTag<Payload>("order"){};
    public static final OutputTag<Payload> customerTag = new OutputTag<Payload>("customer"){};
    public static final OutputTag<Payload> nationTag = new OutputTag<Payload>("nation"){};

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String inputPath = parameterTool.get("input");
        String outputPath = parameterTool.get("output");

        DataStreamSource<String> data = env.readTextFile(inputPath).setParallelism(1);

        SingleOutputStreamOperator<Payload> originalStream = getOriginalData(data);

        DataStream<Payload> lineitem = originalStream.getSideOutput(lineitemTag);
        DataStream<Payload> order = originalStream.getSideOutput(orderTag);
        DataStream<Payload> customer = originalStream.getSideOutput(customerTag);
        DataStream<Payload> nation = originalStream.getSideOutput(nationTag);

        DataStream<Payload> nationResult = nation.keyBy(p -> p.key).process(new Q10NationProcessFunction());

        // 处理customer（与nation join）
        DataStream<Payload> customerResult = nationResult.connect(customer)
                .keyBy(p -> p.key, p -> p.key)
                .process(new Q10CustomerProcessFunction());

        // 处理order（与customer join）
        DataStream<Payload> orderResult = customerResult.connect(order)
                .keyBy(p -> p.key, p -> p.key)
                .process(new Q10OrderProcessFunction());

        // 处理lineitem（与order join）
        DataStream<Payload> lineitemResult = orderResult.connect(lineitem)
                .keyBy(p -> p.key, p -> p.key)
                .process(new Q10LineitemProcessFunction());

        // 最终聚合结果
        DataStream<Payload> result = lineitemResult.keyBy(p -> p.key)
                .process(new Q10AggregateProcessFunction());

        DataStreamSink<Payload> output = result.writeAsText(outputPath, FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        env.execute("Flink Streaming Q10");
    }

    private static SingleOutputStreamOperator<Payload> getOriginalData(DataStreamSource<String> data){
        final SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        return data.process(new ProcessFunction<String, Payload>() {
            @Override
            public void processElement(String value, Context ctx, Collector<Payload> out) throws Exception {
                String header = value.substring(0,3);
                String[] cells = value.substring(3).split("\\|");

                switch (header) {
                    case "+NA":
                        ctx.output(nationTag, new Payload("Insert", Long.valueOf(cells[0]),
                                Arrays.asList("NATIONKEY","N_NAME","N_COMMENT"),
                                Arrays.asList(Long.valueOf(cells[0]), cells[1], cells[3])));
                        break;
                    case "-NA":
                        ctx.output(nationTag, new Payload("Delete", Long.valueOf(cells[0]),
                                Arrays.asList("NATIONKEY","N_NAME","N_COMMENT"),
                                Arrays.asList(Long.valueOf(cells[0]), cells[1], cells[3])));
                        break;
                    case "+CU":
                        ctx.output(customerTag, new Payload("Insert", Long.valueOf(cells[3]),
                                Arrays.asList("CUSTKEY","NATIONKEY","C_NAME","C_ACCTBAL","C_PHONE","C_ADDRESS","C_COMMENT"),
                                Arrays.asList(Long.valueOf(cells[0]), Long.valueOf(cells[3]),
                                        cells[1],Double.valueOf(cells[5]), cells[4],cells[2],cells[7])));
                        break;
                    case "-CU":
                        ctx.output(customerTag, new Payload("Delete", Long.valueOf(cells[3]),
                                Arrays.asList("CUSTKEY","NATIONKEY","C_NAME","C_ACCTBAL","C_PHONE","C_ADDRESS","C_COMMENT"),
                                Arrays.asList(Long.valueOf(cells[0]), Long.valueOf(cells[3]),
                                        cells[1],Double.valueOf(cells[5]), cells[4],cells[2],cells[7])));
                        break;
                    case "+OR":
                        ctx.output(orderTag, new Payload("Insert", Long.valueOf(cells[1]),
                                Arrays.asList("CUSTKEY","ORDERKEY","O_ORDERDATE","O_COMMENT"),
                                Arrays.asList(Long.valueOf(cells[1]), Long.valueOf(cells[0]),
                                        format.parse(cells[4]), cells[8])));
                        break;
                    case "-OR":
                        ctx.output(orderTag, new Payload("Delete", Long.valueOf(cells[1]),
                                Arrays.asList("CUSTKEY","ORDERKEY","O_ORDERDATE","O_COMMENT"),
                                Arrays.asList(Long.valueOf(cells[1]), Long.valueOf(cells[0]),
                                        format.parse(cells[4]), cells[8])));
                        break;
                    case "+LI":
                        ctx.output(lineitemTag, new Payload("Insert", Long.valueOf(cells[0]),
                                Arrays.asList("LINENUMBER","ORDERKEY","L_EXTENDEDPRICE","L_RETURNFLAG","L_COMMENT","L_DISCOUNT"),
                                Arrays.asList(Integer.valueOf(cells[3]), Long.valueOf(cells[0]),
                                        Double.valueOf(cells[5]), cells[8], cells[15], Double.valueOf(cells[6]))));
                        break;
                    case "-LT":
                        ctx.output(lineitemTag, new Payload("Delete", Long.valueOf(cells[0]),
                                Arrays.asList("LINENUMBER","ORDERKEY","L_EXTENDEDPRICE","L_RETURNFLAG","L_COMMENT","L_DISCOUNT"),
                                Arrays.asList(Integer.valueOf(cells[3]), Long.valueOf(cells[0]),
                                        Double.valueOf(cells[5]), cells[8], cells[15], Double.valueOf(cells[6]))));
                        break;
                }
            }
        }).setParallelism(1);
    }
}
