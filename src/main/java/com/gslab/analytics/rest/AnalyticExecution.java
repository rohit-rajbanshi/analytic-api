package com.gslab.analytics.rest;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.PostConstruct;

import org.apache.http.Header;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.message.BasicHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.ge.predix.solsvc.ext.util.JsonMapper;
import com.ge.predix.solsvc.restclient.impl.RestClient;
import com.gslab.analytics.domain.Body;
import com.gslab.analytics.domain.QueryBody;
import com.gslab.analytics.domain.TimeseriesDataBean;

@Controller
public class AnalyticExecution {

	private static final Logger LOG = LoggerFactory.getLogger(AnalyticExecution.class);

	@Autowired
	private RestClient restClient;

	@Autowired
	private JsonMapper jsonMapper;

	@Value("${gslab.catalog.zoneId}")
	private String catalogZoneId;

	@Value("${gslab.timeseries.queryUrl}")
	private String queryUrl;

	@Value("${gslab.analytics.execUrlMinMax}")
	private String execUrlMinMax;

	@Value("${gslab.analytics.execUrlMedian}")
	private String execUrlMedian;

	private ObjectMapper mapper = new ObjectMapper();

	@PostConstruct
	public void init() {
		try {
			System.out.println(restClient.getSecureTokenForClientId());
			LOG.info("obtained uaa token");
		} catch (Exception e) {
			throw new RuntimeException("unable to obtain uaa token", e);
		}
	}

	// median api
	@RequestMapping(value = "/find-median", method = RequestMethod.POST)
	@ResponseBody
	public String findMedian(@RequestBody Body body) throws UnsupportedOperationException, IOException {

		List<TimeseriesDataBean> data = queryTimeseries(body);

		String requestAnalytics = jsonMapper.toJson(data);
		String urlAnalytics = execUrlMedian;
		List<Header> headersAnalytics = generateHeaders();

		// call analytic execution
		CloseableHttpResponse responseAnalytic = restClient.post(urlAnalytics, requestAnalytics, headersAnalytics);
		if (responseAnalytic.getStatusLine().getStatusCode() != 200) {
			LOG.info("analytic execution failed");
			return null;
		}
		// process response
		BufferedReader rd = new BufferedReader(new InputStreamReader(responseAnalytic.getEntity().getContent()));
		JsonNode resultNode = mapper.readTree(rd).get("result");
		mapper.configure(SerializationFeature.INDENT_OUTPUT, true);
		return mapper.writeValueAsString(resultNode);
	}

	// min-max api
	@RequestMapping(value = "/find-min-max", method = RequestMethod.POST)
	@ResponseBody
	public String findMinMax(@RequestBody Body body) throws UnsupportedOperationException, IOException {

		List<TimeseriesDataBean> data = queryTimeseries(body);

		String requestAnalytics = jsonMapper.toJson(data);
		String urlAnalytics = execUrlMinMax;
		List<Header> headersAnalytics = generateHeaders();

		// call analytic execution
		CloseableHttpResponse responseAnalytic = restClient.post(urlAnalytics, requestAnalytics, headersAnalytics);
		if (responseAnalytic.getStatusLine().getStatusCode() != 200) {
			LOG.info("analytic execution failed");
			return null;
		}
		// process response
		BufferedReader rd = new BufferedReader(new InputStreamReader(responseAnalytic.getEntity().getContent()));
		JsonNode resultNode = mapper.readTree(rd).get("result");
		mapper.configure(SerializationFeature.INDENT_OUTPUT, true);
		return mapper.writeValueAsString(resultNode);
	}

	private List<TimeseriesDataBean> queryTimeseries(Body body) throws UnsupportedOperationException, IOException {
		String panelId = body.getPanelId();
		String startTime = body.getStartTime();
		String endTime = body.getEndTime();
		if (Double.parseDouble(startTime) > Double.parseDouble(endTime)) {
			LOG.info("start time is greater than end time");
			return null;
		}
		QueryBody qbody = new QueryBody();
		qbody.setStartTime(startTime);
		qbody.setEndTime(endTime);

		String requestTS = jsonMapper.toJson(qbody);
		String urlTS = queryUrl + "?panelId=" + panelId;
		List<Header> headersTS = new ArrayList<Header>();
		headersTS.add(new BasicHeader("Content-Type", "application/json"));

		// Query time series data
		CloseableHttpResponse response = restClient.post(urlTS, requestTS, headersTS);
		if (response.getStatusLine().getStatusCode() != 200) {
			LOG.info("failed to query time series data");
			return null;
		}
		// process queried response
		BufferedReader reader = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
		String line = "";
		String dataLine = "";
		while ((line = reader.readLine()) != null) {
			dataLine += line;
		}
		dataLine = dataLine.replace("[", "");
		dataLine = dataLine.replace("]", "");
		dataLine = dataLine.replace(" ", "");
		String[] rawData = dataLine.split(",");
		List<TimeseriesDataBean> data = new ArrayList<TimeseriesDataBean>();
		for (int i = 0; i < rawData.length; i += 3) {
			TimeseriesDataBean d = new TimeseriesDataBean();
			d.setTimestamp(rawData[i]);
			d.setValue(Double.parseDouble(rawData[i + 1]));
			data.add(d);
		}
		LOG.info(jsonMapper.toJson(data));
		return data;
	}

	@SuppressWarnings({})
	private List<Header> generateHeaders() {
		List<Header> headers = this.restClient.getSecureTokenForClientId();
		headers.add(new BasicHeader("Content-Type", "application/json"));
		this.restClient.addZoneToHeaders(headers, catalogZoneId);
		return headers;
	}

}
