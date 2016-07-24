package in.co.hadooptutorials.flume.opentsdb.sink;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.apache.http.HttpResponse;
import org.apache.log4j.Logger;

public class HttpSink extends AbstractSink implements Configurable {

	private static final Logger LOG = Logger.getLogger(HttpSink.class);
	private RestClient client = new RestClient();

	private static final int DEFAULT_CONNECT_TIMEOUT = 5000;
	private static final int DEFAULT_REQUEST_TIMEOUT = 5000;
	private static final String DEFAULT_CONTENT_TYPE = "text/plain";
	private static final String DEFAULT_ACCEPT_HEADER = "text/plain";

	private URL endpointUrl;
	private HttpURLConnection httpClient;
	private SinkCounter sinkCounter;
	private String protocol;
	private String host;
	private String port;
	private String path;

	private int connectTimeout = DEFAULT_CONNECT_TIMEOUT;
	private int requestTimeout = DEFAULT_REQUEST_TIMEOUT;
	private String contentTypeHeader = DEFAULT_CONTENT_TYPE;
	private String acceptHeader = DEFAULT_ACCEPT_HEADER;
	private boolean defaultBackoff;
	private boolean defaultRollback;
	private boolean defaultIncrementMetrics;

	private HashMap<String, Boolean> backoffOverrides = new HashMap<String, Boolean>();
	private HashMap<String, Boolean> rollbackOverrides = new HashMap<String, Boolean>();
	private HashMap<String, Boolean> incrementMetricsOverrides = new HashMap<String, Boolean>();

	public void configure(Context context) {
		String configuredEndpoint = context.getString("endpoint", "");
		String configuredProtocol = context.getString("protocol", "");
		protocol = configuredProtocol;
		String configuredHost = context.getString("host", "");
		host = configuredHost;
		String configuredPort = context.getString("port", "");
		port = configuredPort;
		String configuredPath = context.getString("path", "");
		path = configuredPath;

		try {
			endpointUrl = new URL(configuredEndpoint);
		} catch (MalformedURLException e) {
			throw new IllegalArgumentException("Endpoint URL invalid", e);
		}

		connectTimeout = context.getInteger("connectTimeout", DEFAULT_CONNECT_TIMEOUT);
		if (connectTimeout <= 0) {
			throw new IllegalArgumentException("Connect timeout must be a non-zero and positive");
		}

		requestTimeout = context.getInteger("requestTimeout", DEFAULT_REQUEST_TIMEOUT);
		if (requestTimeout <= 0) {
			throw new IllegalArgumentException("Request timeout must be a non-zero and positive");
		}

		acceptHeader = context.getString("acceptHeader", DEFAULT_ACCEPT_HEADER);

		contentTypeHeader = context.getString("contentTypeHeader", DEFAULT_CONTENT_TYPE);

		defaultBackoff = context.getBoolean("defaultBackoff", true);

		defaultRollback = context.getBoolean("defaultRollback", true);

		defaultIncrementMetrics = context.getBoolean("defaultIncrementMetrics", false);

		if (this.sinkCounter == null) {
			this.sinkCounter = new SinkCounter(this.getName());
		}
	}

	@Override
	public void start() {
		sinkCounter.start();
	}

	@Override
	public void stop() {
		sinkCounter.stop();
	}

	public Status process() throws EventDeliveryException {
		Status status = null;
		OutputStream outputStream = null;

		Channel ch = getChannel();
		Transaction txn = ch.getTransaction();
		txn.begin();

		try {
			Event event = ch.take();

			byte[] eventBody = null;
			if (event != null) {
				eventBody = event.getBody();
			}
			String eveStr = new String(eventBody, StandardCharsets.UTF_8);
			LOG.info("Recieved Message:" + eveStr);
			String arr[] = eveStr.split(" ");
			String dateStr = arr[0] + " " + arr[1];
			String sensorid = arr[3];
			String temperature = arr[4];
			String msg = "{\"metric\": \"temperature\", \"timestamp\": %s, \"value\": %s, \"tags\": {\"sensor\" : \"%s\" }}";

			DateFormat dF = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS");
			Date date = dF.parse(dateStr);

			String jsonEvent = String.format(msg, date.getTime(), temperature, sensorid);
			eventBody = jsonEvent.getBytes();
			String convertedMsg = new String(eventBody, StandardCharsets.UTF_8);

			LOG.info("Final Message:" + convertedMsg);

			if (eventBody != null && eventBody.length > 0) {
				sinkCounter.incrementEventDrainAttemptCount();

				HttpResponse res = client.publishToOpenTSDB(protocol, host, port, path, convertedMsg);
				LOG.info("Response :" + res.getStatusLine());
				txn.commit();
				status = Status.READY;

			} else {
				txn.commit();
				status = Status.BACKOFF;

			}

		} catch (Throwable t) {
			txn.rollback();
			status = Status.BACKOFF;

			// re-throw all Errors
			if (t instanceof Error) {
				throw (Error) t;
			}

		} finally {
			txn.close();

			if (outputStream != null) {
				try {
					outputStream.close();
				} catch (IOException e) {
					// ignore errors
				}
			}
		}

		return status;
	}

	void setSinkCounter(SinkCounter sinkCounter) {
		this.sinkCounter = sinkCounter;
	}
}
