package com.charles.main;

import com.charles.RelationType.Payload;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class Q5OrdersProcessFunction extends KeyedCoProcessFunction<Object, Payload, Payload, Payload> {
    String nextKey = "O_ORDERKEY";

    ValueState<Integer> aliveCount;
    ValueState<Payload> alivePayload;
    ValueState<Set<Payload>> aliveSet;

    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
    Date startDate;
    Date endDate;

    @Override
    public void open(Configuration parameters) throws Exception {
        startDate = format.parse("1994-01-01");
        endDate = format.parse("1995-01-01");

        ValueStateDescriptor<Integer> countDesc = new ValueStateDescriptor<>("Q5OrdersCount", Integer.class);
        aliveCount = getRuntimeContext().getState(countDesc);

        ValueStateDescriptor<Payload> plDesc = new ValueStateDescriptor<>("Q5OrdersAlivePayload", Payload.class);
        alivePayload = getRuntimeContext().getState(plDesc);

        TypeInformation<Set<Payload>> ti = TypeInformation.of(new TypeHint<Set<Payload>>() {});
        ValueStateDescriptor<Set<Payload>> setDesc = new ValueStateDescriptor<>("Q5OrdersSet", ti);
        aliveSet = getRuntimeContext().getState(setDesc);
    }

    public boolean isValid(Payload orders) throws Exception {
        Date odate = (Date) orders.getValueByColumnName("O_ORDERDATE");
        return !odate.before(startDate) && odate.before(endDate);
    }

    @Override
    public void processElement1(Payload customer, Context ctx, Collector<Payload> out) throws Exception {
        if(aliveSet.value()==null){
            aliveSet.update(Collections.newSetFromMap(new ConcurrentHashMap<>()));
            aliveCount.update(0);
            alivePayload.update(null);
        }

        if("SetLive".equals(customer.type)){
            alivePayload.update(new Payload(customer));
            aliveCount.update(aliveCount.value()+1);
            for(Payload p: aliveSet.value()){
                customer.type="SetLive";
                outputPayload(customer,p,out);
            }
        } else {
            aliveCount.update(aliveCount.value()-1);
            for(Payload p: aliveSet.value()){
                customer.type="SetDead";
                outputPayload(customer,p,out);
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

        if(isValid(orders)){
            if("Insert".equals(orders.type)){
                if(aliveCount.value()==1){
                    if(aliveSet.value().add(tmp)){
                        orders.type="SetLive";
                        outputPayload(alivePayload.value(),orders,out);
                    }
                } else {
                    aliveSet.value().add(tmp);
                }
            } else if("Delete".equals(orders.type)){
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
    }

    public void outputPayload(Payload customer, Payload orders, Collector<Payload> out) {
        Payload tmp = new Payload(orders);
        if(customer!=null){
            for(int i=0;i<customer.attribute_name.size();i++){
                String attr = customer.attribute_name.get(i);
                if(!tmp.attribute_name.contains(attr)){
                    tmp.attribute_name.add(attr);
                    tmp.attribute_value.add(customer.attribute_value.get(i));
                }
            }
        }
        tmp.setKey(nextKey);
        out.collect(tmp);
    }
}
