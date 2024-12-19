package com.charles.main;

import com.charles.RelationType.Payload;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.*;

public class Q5AggregateProcessFunction extends KeyedProcessFunction<Object, Payload, Payload> {
    ValueState<Double> preValue;
    String aggregateName = "revenue";

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<Double> desc = new ValueStateDescriptor<>("Q5AggregateValue", TypeInformation.of(Double.class));
        preValue = getRuntimeContext().getState(desc);
    }

    @Override
    public void processElement(Payload value, Context ctx, Collector<Payload> out) throws Exception {
        if(preValue.value()==null) {
            preValue.update(0.0);
        }
        double delta = ((Double)value.getValueByColumnName("L_EXTENDEDPRICE"))*(1.0-(Double)value.getValueByColumnName("L_DISCOUNT"));
        double newValue = preValue.value();
        if("Add".equals(value.type)){
            newValue += delta;
        } else if("Sub".equals(value.type)){
            newValue -= delta;
        }
        preValue.update(newValue);

        // 最终输出：N_NAME, revenue
        Payload output = new Payload(value);

        String n_name = (String) value.getValueByColumnName("N_NAME");
        output.attribute_name = Arrays.asList("N_NAME", "revenue");
        output.attribute_value = Arrays.asList(n_name, newValue);
        output.type="Output";
        output.key=n_name;
        out.collect(output);
    }
}
