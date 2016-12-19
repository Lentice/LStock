package lstock;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.URL;
import java.util.ArrayList;
import java.util.concurrent.Semaphore;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * @author LenticeTsai
 *
 */
public class Downloader {

	private static final Logger log = LogManager.getLogger(Downloader.class.getName());
	
	private final Semaphore available = new Semaphore(1, true);
	private long minDownloadGap = 0;
	private long downloadGapRandomShift = 0;
	private long nextDownloadBondary = 0;
	
	private ArrayList<String> proxyAddress = new ArrayList<>();
	private ArrayList<Integer> proxyPort = new ArrayList<>();
	private int proxyIndex = 0;
	private String currentProxyAddress = "";
	private int currentProxyPort = 0;
	private boolean useProxy = false;
	
	public Downloader() {
		
	}

	public Downloader(final long minDownloadGap) {
		this.minDownloadGap = minDownloadGap;
	}
	
	public Downloader(final long minDownloadGap, final long downloadGapRandomShift) {
		this.downloadGapRandomShift = downloadGapRandomShift;
	}
	
	public static void createFolder(String path) throws Exception {
		File dir = new File(path);
		if (!dir.exists())
			dir.mkdirs();
	}
	
	public void enableProxy() {
		getProxyList();
		if (proxyAddress.isEmpty()) {
			log.warn("Proxy List is empty!!");
			useProxy = false;
			return;
		}
		
		useProxy = true;
		currentProxyAddress = proxyAddress.get(proxyIndex);
		currentProxyPort = proxyPort.get(proxyIndex);
	}
	
	public void disableProxy() {
		useProxy = false;
	}
	
	public void nextProxy() {
		if (proxyAddress.isEmpty()) {
			log.warn("Proxy List is empty!!");
			return;
		}
		
		if (++proxyIndex == proxyAddress.size())
			proxyIndex = 0;
		
		currentProxyAddress = proxyAddress.get(proxyIndex);
		currentProxyPort = proxyPort.get(proxyIndex);
		log.info(String.format("Switch proxy server to [%d] %s:%d", proxyIndex, currentProxyAddress, currentProxyPort));
	}

	/**
	 * Download HTTP file via POST method
	 * 
	 * @param targetURL
	 * @param postData
	 * @param filename
	 * @return
	 * @throws IOException
	 */
	public boolean httpPost(String targetURL, String postData, String filename) {
		int length;
		HttpURLConnection urlConnection = null;
		InputStream in;
		DataOutputStream postStream;

		try {
			
			if (minDownloadGap > 0) { 
				available.acquire();
				final long timeDiff = nextDownloadBondary - System.currentTimeMillis();
				if (timeDiff > 0) {
					Thread.sleep(timeDiff);
				}
			}

			URL url = new URL(targetURL);

			// Set URL connection contain
			if (useProxy) {
				Proxy proxy = new Proxy(Proxy.Type.HTTP, new InetSocketAddress(currentProxyAddress, currentProxyPort));
				urlConnection = (HttpURLConnection) url.openConnection(proxy);
			} else {
				urlConnection = (HttpURLConnection) url.openConnection();
			}
			
			urlConnection.setRequestMethod("POST");
			urlConnection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
			urlConnection.setRequestProperty("Content-Length", "" + String.valueOf(postData.getBytes().length));
			urlConnection.setRequestProperty("Content-Language", "UTF-8");
			urlConnection.setRequestProperty("User-Agent", "Mozilla/5.0 (Windows NT 6.1; Win64; x64)");
			urlConnection.setUseCaches(false);
			urlConnection.setDoInput(true);
			urlConnection.setDoOutput(true);

			// Send Request
			postStream = new DataOutputStream(urlConnection.getOutputStream());
			postStream.writeBytes(postData);
			postStream.flush();
			postStream.close();

			in = urlConnection.getInputStream();

			// save to file
			byte[] buffer = new byte[1024];
			FileOutputStream file = new FileOutputStream(filename);
			while ((length = in.read(buffer)) > 0) {
				file.write(buffer, 0, length);
			}

			file.close();
			in.close();
			urlConnection.disconnect();

			log.trace("Download success. URL: " + targetURL);
		} catch (Exception e) {
			return false;
		} finally {
			if (minDownloadGap > 0) { 
				nextDownloadBondary = System.currentTimeMillis() + minDownloadGap 
						+ (int)(Math.random() * downloadGapRandomShift);
				available.release();
			}
		}

		return true;
	}

	/**
	 * Download HTTP file via GET method
	 * 
	 * @param targetURL
	 * @param filename
	 * @return
	 * @throws Exception
	 */
	public boolean httpGet(String targetURL, String filename) {
		int length;
		HttpURLConnection urlConnection = null;
		InputStream in = null;
		try {
			
			if (minDownloadGap > 0) { 
				available.acquire();
				final long timeDiff = nextDownloadBondary - System.currentTimeMillis();
				if (timeDiff > 0) {
					Thread.sleep(timeDiff);
				}
			}
			
			URL url = new URL(targetURL);
			if (useProxy) {
				Proxy proxy = new Proxy(Proxy.Type.HTTP, new InetSocketAddress(currentProxyAddress, currentProxyPort));
				urlConnection = (HttpURLConnection) url.openConnection(proxy);
			} else {
				urlConnection = (HttpURLConnection) url.openConnection();
			}
			urlConnection.setRequestProperty("User-Agent", "Mozilla/5.0 (Windows NT 6.1; Win64; x64)");

			in = urlConnection.getInputStream();
			FileOutputStream file = new FileOutputStream(filename);

			byte[] buffer = new byte[1024];
			while ((length = in.read(buffer)) > 0) {
				file.write(buffer, 0, length);
			}

			file.close();
			in.close();
			
			log.trace("Download success. URL: " + targetURL);
		} catch (Exception e) {
			return false;
		} finally {
			if (urlConnection != null) {
				urlConnection.disconnect();
			}
			if (minDownloadGap > 0) { 
				nextDownloadBondary = System.currentTimeMillis() + minDownloadGap 
						+ (int)(Math.random() * downloadGapRandomShift);
				available.release();
			}
		}

		return true;
	}
	
	public void setMinDownloadGap(final long minDownloadGap) {
		this.minDownloadGap = minDownloadGap; 
	}
	
	public void setDownloadGapRandomShift(final long RandomShift) {
		this.downloadGapRandomShift = RandomShift; 
	}
	
	private void getProxyList() {
		String line;
		final File file = new File("ProxyList.txt");
		if (!file.isFile())
			return;
		
		try {
			BufferedReader fileBR = new BufferedReader(new InputStreamReader(new FileInputStream(file), "MS950"), 40960);
			while ((line = fileBR.readLine()) != null) {
				if (line.isEmpty())
					continue;
				
				String[] data = line.trim().split("\\s+");
				if (data[0].isEmpty() || data[1].isEmpty())
					continue;
				
				proxyAddress.add(data[0]);
				proxyPort.add(new Integer(data[1]));
			}
			
			fileBR.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}


	public static void main(String[] args) throws InterruptedException {
		new Downloader().httpPost("http://mops.twse.com.tw/mops/web/ajax_t163sb04", "encodeURIComponent=1&step=1&firstin=1&off=1&TYPEK=sii&year=104&season=01", "Test.html");
	}
}
