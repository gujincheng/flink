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

import okhttp3.Authenticator;
import okhttp3.Credentials;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.Route;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.reporter.Scheduled;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** {@link MetricReporter} that exports {@link Metric Metrics} via InfluxDB. */
public class WebServiceReporter implements MetricReporter, Scheduled {
    private static final Logger LOG = LoggerFactory.getLogger(WebServiceReporter.class);
    public static final MediaType MEDIATYPE = MediaType.get("application/json; charset=utf-8");
    private final Map<Gauge<?>, String> gauges = new HashMap<>();
    private final Map<Counter, String> counters = new HashMap<>();
    private final Map<Histogram, String> histograms = new HashMap<>();
    private final Map<Meter, String> meters = new HashMap<>();
    HashSet<String> FILTERMETRICS = new HashSet<>();
    static final String JOB_ID_VARIABLE = "<job_id>";
    private OkHttpClient client;
    private String url;
    private String jobName;
    private String jobId;
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
        String filterMetricStr = WebServiceReporterOptions.getString(config,WebServiceReporterOptions.FILTERMETRICS);
        if(!isEmpty(filterMetricStr)){
            FILTERMETRICS = new HashSet<>(Arrays.asList(WebServiceReporterOptions.getString(config,WebServiceReporterOptions.FILTERMETRICS).split(",")));
        }

        client = new OkHttpClient.Builder()
                .connectTimeout(connectTimeout, TimeUnit.MILLISECONDS)
                .writeTimeout(writeTimeout, TimeUnit.MILLISECONDS)
                .authenticator(new Authenticator() {
                    @Nullable
                    @Override
                    public Request authenticate(
                            @Nullable Route route,
                            Response response) {
                        if (!isEmpty(userName) && !isEmpty(password)) {
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
    public void notifyOfAddedMetric(Metric metric, String metricName, MetricGroup group) {
        final String fullName = group.getMetricIdentifier(metricName);
        Map<String, String> allVariables = group.getAllVariables();
        this.jobId = allVariables.get(JOB_ID_VARIABLE);
        if (FILTERMETRICS.isEmpty() || FILTERMETRICS.contains(metricName)) {
            synchronized (this) {
                switch (metric.getMetricType()) {
                    case COUNTER:
                        counters.putIfAbsent((Counter) metric, fullName);
                        break;
                    case GAUGE:
                        gauges.putIfAbsent((Gauge<?>) metric, fullName);
                        break;
                    case HISTOGRAM:
                        histograms.putIfAbsent((Histogram) metric, fullName);
                        break;
                    case METER:
                        meters.putIfAbsent((Meter) metric, fullName);
                        break;
                    default:
                        LOG.warn(
                                "Cannot add metric of type {}. This indicates that the reporter "
                                        + "does not support this metric type.",
                                metric.getClass().getName());
                }
            }
        }
    }

    @Override
    public void notifyOfRemovedMetric(Metric metric, String metricName, MetricGroup group) {
        synchronized (this) {
            switch (metric.getMetricType()) {
                case COUNTER:
                    counters.remove(metric);
                    break;
                case GAUGE:
                    gauges.remove(metric);
                    break;
                case HISTOGRAM:
                    histograms.remove(metric);
                    break;
                case METER:
                    meters.remove(metric);
                    break;
                default:
                    LOG.warn(
                            "Cannot remove unknown metric type {}. This indicates that the reporter "
                                    + "does not support this metric type.",
                            metric.getClass().getName());
            }
        }
    }

    @Override
    public void report() {
        Request request = buildReport();
        if (request != null) {
            try (Response response = client.newCall(request).execute()) {
                if (response.isSuccessful()) {
                    String responseString = response.body().string();
                    LOG.debug("=======> {}" , responseString);
                } else {
                    LOG.error("#####> report failed: {}", response.message());
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Nullable
    private Request buildReport() {
        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        String reportJson = "{}";

        ObjectMapper mapper = new ObjectMapper();
        ObjectNode metricMap = mapper.createObjectNode();
        metricMap.put("jobName", this.jobName);
        metricMap.put("jobId", this.jobId);
        metricMap.put("timestamp", timestamp);
        ArrayNode jsonArray = mapper.createArrayNode();
        try {
            gauges.forEach((gauge, metricName) -> {
                ObjectNode jsonObject = mapper.createObjectNode();
                jsonObject.put("metricName", metricName);
                jsonObject.put("metricValue", mapper.valueToTree(gauge.getValue()));
                jsonObject.put("metricType", "Gauge");
                jsonArray.add(jsonObject);
            });
            counters.forEach((counter, metricName) -> {
                ObjectNode jsonObject = mapper.createObjectNode();
                jsonObject.put("metricName", metricName);
                jsonObject.put("metricValue", counter.getCount());
                jsonObject.put("metricType", "Counter");
                jsonArray.add(jsonObject);
            });
            histograms.forEach((histogram, metricName) -> {
                ObjectNode jsonObject = mapper.createObjectNode();
                jsonObject.put("metricName", metricName);
                jsonObject.put("metricValue", histogram.getCount());
                jsonObject.put("metricType", "Histogram");
                jsonArray.add(jsonObject);
            });

            meters.forEach((meter, metricName) -> {
                ObjectNode jsonObject = mapper.createObjectNode();
                jsonObject.put("metricName", metricName);
                jsonObject.put("metricValue", meter.getCount());
                jsonObject.put("metricType", "Meter");
                jsonArray.add(jsonObject);
            });

            metricMap.put("metrics", jsonArray);
            reportJson = mapper.writeValueAsString(metricMap);
        } catch (ConcurrentModificationException | NoSuchElementException | JsonProcessingException e) {
            // ignore - may happen when metrics are concurrently added or removed
            // report next time
            return null;
        }

        RequestBody body = RequestBody.create(MEDIATYPE, reportJson);
        if (!metricMap.get("metrics").isEmpty()){
            Request request = new Request.Builder()
                    .url(url)
                    .post(body)
                    .build();
            return request;
        }
        return null;
    }

    private static boolean isEmpty(String str){
        return str == null || str.isEmpty();
    }
}
