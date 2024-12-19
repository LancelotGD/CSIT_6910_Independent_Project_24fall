package com.charles.main;

import com.charles.RelationType.Payload;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class Q10OrderProcessFunction extends KeyedCoProcessFunction<Object, Payload, Payload, Payload> {
    String nextKey = "ORDERKEY";

    ValueState<Integer> aliveCount;
    ValueState<Set<Payload>> aliveSet;
    ValueState<Payload> alivePayload;

    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<Integer> countDescriptor =
                new ValueStateDescriptor<>("Q10OrderProcessFunctionAliveCount", Integer.class);
        aliveCount = getRuntimeContext().getState(countDescriptor);

        TypeInformation<Set<Payload>> ti = TypeInformation.of(new TypeHint<Set<Payload>>() {});
        ValueStateDescriptor<Set<Payload>> setDesc = new ValueStateDescriptor<>("Q10OrderProcessFunctionAliveSet", ti);
        aliveSet = getRuntimeContext().getState(setDesc);

        ValueStateDescriptor<Payload> aliveNodeDesc = new ValueStateDescriptor<>("Q10OrderProcessFunctionAlivePayload", Payload.class);
        alivePayload = getRuntimeContext().getState(aliveNodeDesc);
    }

    // o_orderdate >= '1993-03-15' and o_orderdate < '1996-03-15'
    public boolean isValid(Payload value) throws ParseException {
        Date date = (Date) value.getValueByColumnName("O_ORDERDATE");
        Date lowerBound = format.parse("1993-03-15");
        Date upperBound = format.parse("1996-03-15");
        return date.compareTo(lowerBound) >= 0 && date.compareTo(upperBound) < 0;
    }

    @Override
    public void processElement1(Payload payload, Context ctx, Collector<Payload> out) throws Exception {
        if(aliveSet.value() == null){
            aliveSet.update(Collections.newSetFromMap(new ConcurrentHashMap<>()));
            aliveCount.update(0);
            alivePayload.update(null);
        }

        if("SetLive".equals(payload.type)){
            alivePayload.update(new Payload(payload));
            aliveCount.update(aliveCount.value() + 1);
            for(Payload p: aliveSet.value()){
                outputPayload(payload, p, out);
            }
        }else{
            aliveCount.update(aliveCount.value() - 1);
            for(Payload p: aliveSet.value()){
                outputPayload(payload, p, out);
            }
            alivePayload.update(null);
        }
    }

    @Override
    public void processElement2(Payload payload, Context ctx, Collector<Payload> out) throws Exception {
        if(aliveSet.value() == null){
            aliveSet.update(Collections.newSetFromMap(new ConcurrentHashMap<>()));
            aliveCount.update(0);
            alivePayload.update(null);
        }

        Payload tmp = new Payload(payload);
        tmp.type = "Tmp";
        tmp.key = 0;

        if(isValid(payload)){
            if("Insert".equals(payload.type)){
                if(aliveCount.value() == 1){
                    if(aliveSet.value().add(tmp)){
                        payload.type = "SetLive";
                        outputPayload(payload, alivePayload.value(), out);
                    }
                } else {
                    aliveSet.value().add(tmp);
                }
            } else if("Delete".equals(payload.type)){
                if(aliveCount.value() == 1){
                    if(aliveSet.value().remove(tmp)){
                        payload.type = "SetDead";
                        outputPayload(payload, alivePayload.value(), out);
                    }
                } else {
                    aliveSet.value().remove(tmp);
                }
            }
        }
    }

    public void outputPayload(Payload left, Payload right, Collector<Payload> out) {
        Payload tmp = new Payload(left);
        if(right != null) {
            for(int i=0; i<right.attribute_name.size(); i++){
                String attr = right.attribute_name.get(i);
                if(!tmp.attribute_name.contains(attr)){
                    tmp.attribute_name.add(attr);
                    tmp.attribute_value.add(right.attribute_value.get(i));
                }
            }
        }
        tmp.setKey(nextKey);
        out.collect(tmp);
    }
}
