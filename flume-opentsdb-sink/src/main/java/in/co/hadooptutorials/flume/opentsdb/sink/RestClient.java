package in.co.hadooptutorials.flume.opentsdb.sink;

import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;

import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;

public class RestClient {

	DefaultHttpClient client = new DefaultHttpClient();

	public HttpResponse publishToOpenTSDB(String protocol, String host, String port, String path, String message) {
		HttpPost post = null;
		HttpResponse response = null;
		try {

			java.net.URI uri = new URIBuilder().setScheme(protocol).setHost(host+":"+port).setPath(path)
					.build();
			post = new HttpPost(uri);
			StringEntity myEntity = new StringEntity(message, ContentType.create("text/plain", "UTF-8"));

			post.setEntity(myEntity);

			response = client.execute(post);
		} catch (URISyntaxException e) {

			e.printStackTrace();
		} catch (ClientProtocolException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally // cleanup and reset
		{
			post.releaseConnection();
			post.reset();
			post = null;

		}
		return response;

	}
}
