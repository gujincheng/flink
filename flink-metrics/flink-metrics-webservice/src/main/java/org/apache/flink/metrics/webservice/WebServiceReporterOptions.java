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

import org.apache.flink.annotation.docs.Documentation;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.metrics.MetricConfig;


/** Config options for {@link WebServiceReporter}. */
@Documentation.SuffixOption(ConfigConstants.METRICS_REPORTER_PREFIX + "webservice")
public class WebServiceReporterOptions {

    public static final ConfigOption<String> URL =
            ConfigOptions.key("url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("WebService URL");

    public static final ConfigOption<String> USERNAME =
            ConfigOptions.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("(optional) WebService username used for authentication");

    public static final ConfigOption<String> PASSWORD =
            ConfigOptions.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("(optional) WebService username's password used for authentication");

    public static final ConfigOption<String> JOBNAME =
            ConfigOptions.key("jobName")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "(optional) WebService Flink JobName");

    public static final ConfigOption<String> FILTERMETRICS =
            ConfigOptions.key("filterMetrics")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "(optional) WebService Flink JobName");

    public static final ConfigOption<Integer> CONNECT_TIMEOUT =
            ConfigOptions.key("connectTimeout")
                    .intType()
                    .defaultValue(3000)
                    .withDescription("(optional) the WebService connect timeout for metrics");

    public static final ConfigOption<Integer> WRITE_TIMEOUT =
            ConfigOptions.key("writeTimeout")
                    .intType()
                    .defaultValue(3000)
                    .withDescription("(optional) the WebService write timeout for metrics");

    static String getString(MetricConfig config, ConfigOption<String> key) {
        return config.getString(key.key(), key.defaultValue());
    }

    static int getInteger(MetricConfig config, ConfigOption<Integer> key) {
        return config.getInteger(key.key(), key.defaultValue());
    }
}
