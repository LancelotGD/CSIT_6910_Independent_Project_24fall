package com.charles.main;

import com.charles.RelationType.Payload;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.*;

public class Q10AggregateProcessFunction extends KeyedProcessFunction<Object, Payload, Payload> {
    ValueState<Double> preValue;

    List<String> outputNames = Arrays.asList("CUSTKEY","C_NAME","C_ACCTBAL","C_PHONE","N_NAME","C_ADDRESS","C_COMMENT");
    String aggregateName = "revenue";
    String nextKey = "CUSTKEY";

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<Double> preValueDescriptor = new ValueStateDescriptor<>(
                "Q3AggregateProcessFunctionPreValue", TypeInformation.of(Double.class));
        preValue = getRuntimeContext().getState(preValueDescriptor);
    }

    @Override
    public void processElement(Payload payload, Context context, Collector<Payload> out) throws Exception {
        if(preValue.value() == null) {
            preValue.update(0.0);
        }

        Double delta = getAggregate(payload);
        double newValue = preValue.value();

        if("Add".equals(payload.type)){
            newValue += delta;
        } else if("Sub".equals(payload.type)){
            newValue -= delta;
        }

        preValue.update(newValue);
        List<Object> attribute_value = new ArrayList<>();
        List<String> attribute_name = new ArrayList<>();

        attribute_value.add(newValue);
        attribute_name.add(aggregateName);

        for (String col : outputNames) {
            attribute_value.add(payload.getValueByColumnName(col));
            attribute_name.add(col);
        }

        payload.attribute_name = attribute_name;
        payload.attribute_value = attribute_value;
        payload.setKey(nextKey);
        payload.type = "Output";
        out.collect(payload);
    }

    public Double getAggregate(Payload p){
        return (Double)p.getValueByColumnName("L_EXTENDEDPRICE")*(1.0-(Double)p.getValueByColumnName("L_DISCOUNT"));
    }
}
