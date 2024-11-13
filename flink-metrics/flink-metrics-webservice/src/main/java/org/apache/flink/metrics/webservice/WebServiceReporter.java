/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.metrics.webservice;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import okhttp3.Authenticator;
import okhttp3.Credentials;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;

import okhttp3.Request;
import okhttp3.RequestBody;

import okhttp3.Response;

import okhttp3.Route;

import org.apache.commons.lang3.StringUtils;

import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.reporter.AbstractReporter;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.reporter.Scheduled;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** {@link MetricReporter} that exports {@link Metric Metrics} via InfluxDB. */
public class WebServiceReporter extends AbstractReporter implements Scheduled {
    public static final MediaType MEDIATYPE
            = MediaType.get("application/json; charset=utf-8");
    private OkHttpClient client;
    private String url;
    private String jobName;
    @Override
    public void open(MetricConfig config) {
        url = checkNotNull(WebServiceReporterOptions.getString(config,WebServiceReporterOptions.URL),
                "Invalid configuration. URL: " + WebServiceReporterOptions.getString(config,WebServiceReporterOptions.URL));
        int connectTimeout = WebServiceReporterOptions.getInteger(config, WebServiceReporterOptions.CONNECT_TIMEOUT);
        int writeTimeout = WebServiceReporterOptions.getInteger(config, WebServiceReporterOptions.WRITE_TIMEOUT);
        String userName = WebServiceReporterOptions.getString(config, WebServiceReporterOptions.USERNAME);
        String password = WebServiceReporterOptions.getString(config, WebServiceReporterOptions.PASSWORD);
        jobName = checkNotNull(WebServiceReporterOptions.getString(config,WebServiceReporterOptions.JOBNAME),
                "Invalid configuration. URL: " + WebServiceReporterOptions.getString(config,WebServiceReporterOptions.JOBNAME));

        client = new OkHttpClient.Builder()
                .connectTimeout(connectTimeout, TimeUnit.MILLISECONDS)
                .writeTimeout(writeTimeout, TimeUnit.MILLISECONDS)
                .authenticator(new Authenticator() {
                    @Nullable
                    @Override
                    public Request authenticate(
                            @Nullable Route route,
                            Response response) {
                        if (StringUtils.isNotEmpty(userName) || StringUtils.isNotEmpty(password)) {
                            String credential = Credentials.basic(userName, password);
                            return response.request().newBuilder()
                                    .header("Authorization", credential)
                                    .build();
                        }
                        return null;
                    }
                }).build();
    }


    @Override
    public void close() {
        client.dispatcher().executorService().shutdown();
        client.connectionPool().evictAll();
    }

    @Override
    public void report() {
        Request request = buildReport();
        try (Response response = client.newCall(request).execute()) {
            if (response.isSuccessful()) {
                String responseString = response.body().string();
                System.out.println(responseString);
            } else {
                System.out.println("Request failed: " + response.code());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Nullable
    private Request buildReport() {
        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        String reportJson = "{}";
        Map<String, Object> metricMap = new HashMap<>();
        metricMap.put("jobName", this.jobName);
        metricMap.put("timestamp", timestamp);
        JSONArray jsonArray = new JSONArray();
        try {
            gauges.forEach((gauge, metricName) -> {
                JSONObject jsonObject = new JSONObject();
                jsonObject.put("metricName", metricName);
                jsonObject.put("metricValue", gauge.getValue());
                jsonObject.put("metricType", "Gauge");
                jsonArray.add(jsonObject);
            });
            counters.forEach((counter, metricName) -> {
                JSONObject jsonObject = new JSONObject();
                jsonObject.put("metricName", metricName);
                jsonObject.put("metricValue", counter.getCount());
                jsonObject.put("metricType", "Counter");
                jsonArray.add(jsonObject);
            });
            histograms.forEach((histogram, metricName) -> {
                JSONObject jsonObject = new JSONObject();
                jsonObject.put("metricName", metricName);
                jsonObject.put("metricValue", histogram.getCount());
                jsonObject.put("metricType", "Histogram");
                jsonArray.add(jsonObject);
            });

            meters.forEach((meter, metricName) -> {
                JSONObject jsonObject = new JSONObject();
                jsonObject.put("metricName", metricName);
                jsonObject.put("metricValue", meter.getCount());
                jsonObject.put("metricType", "Meter");
                jsonArray.add(jsonObject);
            });

            metricMap.put("metrics", jsonArray);
            reportJson = JSONObject.toJSONString(metricMap);
        } catch (ConcurrentModificationException | NoSuchElementException e) {
            // ignore - may happen when metrics are concurrently added or removed
            // report next time
            return null;
        }

        RequestBody body = RequestBody.create(MEDIATYPE, reportJson);
        Request request = new Request.Builder()
                .url(url)
                .post(body)
                .build();
        return request;
    }

    @Override
    public String filterCharacters(String input) {
        return input;
    }
}
