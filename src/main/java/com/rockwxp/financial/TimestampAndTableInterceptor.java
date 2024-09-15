package com.rockwxp.financial;

import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

/**
 * @author rock
 * @date 2024/9/15 00:11
 */
public class TimestampAndTableInterceptor implements Interceptor {


    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        Map<String, String> headers = event.getHeaders();
        try{
            String log = new String(event.getBody(), StandardCharsets.UTF_8);
            JSONObject jsonObject = JSONObject.parseObject(log);
            String table = jsonObject.getString("table");
            String ts = jsonObject.getString("ts")+"000";
            headers.put("tableName",table);
            headers.put("timestamp",ts);
            event.setHeaders(headers);
        }catch (JSONException e){
            return null;
        }
        return event;
    }

    @Override
    public List<Event> intercept(List<Event> list) {


        list.removeIf(next -> intercept(next) == null);


        return list;
    }

    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder {
        @Override
        public Interceptor build() {
            return new TimestampAndTableInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
