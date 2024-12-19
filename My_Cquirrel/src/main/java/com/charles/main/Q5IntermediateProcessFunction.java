package com.charles.main;

import com.charles.RelationType.Payload;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class Q5IntermediateProcessFunction extends KeyedCoProcessFunction<Object, Payload, Payload, Payload> {
    ValueState<Integer> aliveCount;
    ValueState<Payload> alivePayload;
    ValueState<Set<Payload>> aliveSet;

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<Integer> countDesc = new ValueStateDescriptor<>("Q5InterCount", Integer.class);
        aliveCount = getRuntimeContext().getState(countDesc);

        ValueStateDescriptor<Payload> plDesc = new ValueStateDescriptor<>("Q5InterAlivePayload", Payload.class);
        alivePayload = getRuntimeContext().getState(plDesc);

        ValueStateDescriptor<Set<Payload>> setDesc = new ValueStateDescriptor<>("Q5InterSet",
                TypeInformation.of(new TypeHint<Set<Payload>>(){}));
        aliveSet = getRuntimeContext().getState(setDesc);
    }

    @Override
    public void processElement1(Payload supplier, Context ctx, Collector<Payload> out) throws Exception {
        if(aliveSet.value()==null){
            aliveSet.update(Collections.newSetFromMap(new ConcurrentHashMap<>()));
            aliveCount.update(0);
            alivePayload.update(null);
        }

        if("SetLive".equals(supplier.type)){
            alivePayload.update(new Payload(supplier));
            aliveCount.update(aliveCount.value()+1);
            for(Payload p: aliveSet.value()){
                supplier.type="SetLive";
                outputPayload(supplier,p,out);
            }
        } else {
            aliveCount.update(aliveCount.value()-1);
            for(Payload p: aliveSet.value()){
                supplier.type="SetDead";
                outputPayload(supplier,p,out);
            }
            alivePayload.update(null);
        }
    }

    @Override
    public void processElement2(Payload orders, Context ctx, Collector<Payload> out) throws Exception {
        if(aliveSet.value()==null){
            aliveSet.update(Collections.newSetFromMap(new ConcurrentHashMap<>()));
            aliveCount.update(0);
            alivePayload.update(null);
        }

        Payload tmp = new Payload(orders); tmp.type="Tmp"; tmp.key=0;

        // 无额外过滤
        if("SetLive".equals(orders.type)){
            if(aliveCount.value()==1){
                if(aliveSet.value().add(tmp)){
                    orders.type="SetLive";
                    outputPayload(alivePayload.value(),orders,out);
                }
            } else {
                aliveSet.value().add(tmp);
            }
        } else if("SetDead".equals(orders.type)){
            if(aliveCount.value()==1){
                if(aliveSet.value().remove(tmp)){
                    orders.type="SetDead";
                    outputPayload(alivePayload.value(),orders,out);
                }
            } else {
                aliveSet.value().remove(tmp);
            }
        }
    }

    public void outputPayload(Payload supplier, Payload orders, Collector<Payload> out){
        Payload tmp = new Payload(orders);
        if(supplier!=null){
            for(int i=0;i<supplier.attribute_name.size();i++){
                String attr = supplier.attribute_name.get(i);
                if(!tmp.attribute_name.contains(attr)){
                    tmp.attribute_name.add(attr);
                    tmp.attribute_value.add(supplier.attribute_value.get(i));
                }
            }
        }

        tmp.setKey("O_ORDERKEY");
        out.collect(tmp);
    }
}
