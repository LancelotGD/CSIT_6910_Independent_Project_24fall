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
import java.util.Arrays;

public class Q5Main {
    public static final OutputTag<Payload> regionTag = new OutputTag<Payload>("region"){};
    public static final OutputTag<Payload> nationTag = new OutputTag<Payload>("nation"){};
    public static final OutputTag<Payload> supplierTag = new OutputTag<Payload>("supplier"){};
    public static final OutputTag<Payload> customerTag = new OutputTag<Payload>("customer"){};
    public static final OutputTag<Payload> ordersTag = new OutputTag<Payload>("orders"){};
    public static final OutputTag<Payload> lineitemTag = new OutputTag<Payload>("lineitem"){};

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String inputPath = parameterTool.get("input");
        String outputPath = parameterTool.get("output");

        DataStreamSource<String> data = env.readTextFile(inputPath).setParallelism(1);
        SingleOutputStreamOperator<Payload> originalStream = parseInput(data);

        DataStream<Payload> regionStream = originalStream.getSideOutput(regionTag);
        DataStream<Payload> nationStream = originalStream.getSideOutput(nationTag);
        DataStream<Payload> supplierStream = originalStream.getSideOutput(supplierTag);
        DataStream<Payload> customerStream = originalStream.getSideOutput(customerTag);
        DataStream<Payload> ordersStream = originalStream.getSideOutput(ordersTag);
        DataStream<Payload> lineitemStream = originalStream.getSideOutput(lineitemTag);

        DataStream<Payload> regionResult = regionStream.keyBy(p->p.key)
                .process(new Q5RegionProcessFunction());

        DataStream<Payload> nationResult = regionResult.connect(nationStream)
                .keyBy(p->p.key, p->p.key)
                .process(new Q5NationProcessFunction());

        DataStream<Payload> supplierResult = nationResult.connect(supplierStream)
                .keyBy(p->p.key, p->p.key)
                .process(new Q5SupplierProcessFunction());

        // 与nationResult连接获得customer维度(确保customer与符合region条件的nation匹配)
        DataStream<Payload> customerResult = nationResult.connect(customerStream)
                .keyBy(p->p.key, p->p.key)
                .process(new Q5CustomerProcessFunction());

        DataStream<Payload> ordersResult = customerResult.connect(ordersStream)
                .keyBy(p->p.key, p->p.key)
                .process(new Q5OrdersProcessFunction());

        DataStream<Payload> intermedResult = supplierResult.connect(ordersResult)
                .keyBy(p->p.key, p->p.key)
                .process(new Q5IntermediateProcessFunction());

        DataStream<Payload> lineitemResult = intermedResult.connect(lineitemStream)
                .keyBy(p->p.key, p->p.key)
                .process(new Q5LineitemProcessFunction());

        // 最终聚合，以 n_name 为Key聚合求revenue
        DataStream<Payload> result = lineitemResult.keyBy(p->p.getValueByColumnName("N_NAME"))
                .process(new Q5AggregateProcessFunction());

        result.writeAsText(outputPath, FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute("Flink Streaming Q5");
    }

    private static SingleOutputStreamOperator<Payload> parseInput(DataStreamSource<String> data){
        final SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        return data.process(new ProcessFunction<String, Payload>() {
            @Override
            public void processElement(String value, Context ctx, Collector<Payload> out) throws Exception {
                String header = value.substring(0,3);
                String[] cells = value.substring(3).split("\\|");

                switch(header) {
                    case "+RG":
                        ctx.output(regionTag, new Payload("Insert", Long.valueOf(cells[0]),
                                Arrays.asList("R_REGIONKEY","R_NAME","R_COMMENT"),
                                Arrays.asList(Long.valueOf(cells[0]), cells[1], cells[2])));
                        break;
                    case "-RG":
                        ctx.output(regionTag, new Payload("Delete", Long.valueOf(cells[0]),
                                Arrays.asList("R_REGIONKEY","R_NAME","R_COMMENT"),
                                Arrays.asList(Long.valueOf(cells[0]), cells[1], cells[2])));
                        break;
                    case "+NA":
                        ctx.output(nationTag, new Payload("Insert", Long.valueOf(cells[0]),
                                Arrays.asList("N_NATIONKEY","N_NAME","N_REGIONKEY","N_COMMENT"),
                                Arrays.asList(Long.valueOf(cells[0]), cells[1], Long.valueOf(cells[2]), cells[3])));
                        break;
                    case "-NA":
                        ctx.output(nationTag, new Payload("Delete", Long.valueOf(cells[0]),
                                Arrays.asList("N_NATIONKEY","N_NAME","N_REGIONKEY","N_COMMENT"),
                                Arrays.asList(Long.valueOf(cells[0]), cells[1], Long.valueOf(cells[2]), cells[3])));
                        break;
                    case "+SU":
                        ctx.output(supplierTag, new Payload("Insert", Long.valueOf(cells[0]),
                                Arrays.asList("S_SUPPKEY","S_NATIONKEY","S_NAME"),
                                Arrays.asList(Long.valueOf(cells[0]), Long.valueOf(cells[3]), cells[1])));
                        break;
                    case "-SU":
                        ctx.output(supplierTag, new Payload("Delete", Long.valueOf(cells[0]),
                                Arrays.asList("S_SUPPKEY","S_NATIONKEY","S_NAME"),
                                Arrays.asList(Long.valueOf(cells[0]), Long.valueOf(cells[3]), cells[1])));
                        break;
                    case "+CU":
                        ctx.output(customerTag, new Payload("Insert", Long.valueOf(cells[0]),
                                Arrays.asList("C_CUSTKEY","C_NATIONKEY","C_NAME","C_ACCTBAL"),
                                Arrays.asList(Long.valueOf(cells[0]), Long.valueOf(cells[3]), cells[1],Double.valueOf(cells[5]))));
                        break;
                    case "-CU":
                        ctx.output(customerTag, new Payload("Delete", Long.valueOf(cells[0]),
                                Arrays.asList("C_CUSTKEY","C_NATIONKEY","C_NAME","C_ACCTBAL"),
                                Arrays.asList(Long.valueOf(cells[0]), Long.valueOf(cells[3]), cells[1],Double.valueOf(cells[5]))));
                        break;
                    case "+OR":
                        ctx.output(ordersTag, new Payload("Insert", Long.valueOf(cells[1]),
                                Arrays.asList("O_ORDERKEY","C_CUSTKEY","O_ORDERDATE"),
                                Arrays.asList(Long.valueOf(cells[0]), Long.valueOf(cells[1]), format.parse(cells[4]))));
                        break;
                    case "-OR":
                        ctx.output(ordersTag, new Payload("Delete", Long.valueOf(cells[1]),
                                Arrays.asList("O_ORDERKEY","C_CUSTKEY","O_ORDERDATE"),
                                Arrays.asList(Long.valueOf(cells[0]), Long.valueOf(cells[1]), format.parse(cells[4]))));
                        break;
                    case "+LI":
                        ctx.output(lineitemTag, new Payload("Insert", Long.valueOf(cells[0]),
                                Arrays.asList("L_ORDERKEY","L_SUPPKEY","L_EXTENDEDPRICE","L_DISCOUNT"),
                                Arrays.asList(Long.valueOf(cells[0]), Long.valueOf(cells[2]), Double.valueOf(cells[5]), Double.valueOf(cells[6]))));
                        break;
                    case "-LI":
                        ctx.output(lineitemTag, new Payload("Delete", Long.valueOf(cells[0]),
                                Arrays.asList("L_ORDERKEY","L_SUPPKEY","L_EXTENDEDPRICE","L_DISCOUNT"),
                                Arrays.asList(Long.valueOf(cells[0]), Long.valueOf(cells[2]), Double.valueOf(cells[5]), Double.valueOf(cells[6]))));
                        break;
                }
            }
        }).setParallelism(1);
    }
}
