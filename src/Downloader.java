import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.Calendar;

/**
 * @author LenticeTsai
 *
 */
public class Downloader {

	public static int download(String targetURL, String postData, String filename) throws Exception {
		int ret = -1;
		int length;
		HttpURLConnection urlConnection = null;

		try {
			URL url = new URL(targetURL);
			urlConnection = (HttpURLConnection) url.openConnection();
			urlConnection.setRequestMethod("POST");
			urlConnection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
			urlConnection.setRequestProperty("Content-Length", "" + String.valueOf(postData.getBytes().length));
			urlConnection.setRequestProperty("Content-Language", "UTF-8");
			urlConnection.setUseCaches(false);
			urlConnection.setDoInput(true);
			urlConnection.setDoOutput(true);

			// Send Request
			DataOutputStream postStream = new DataOutputStream(urlConnection.getOutputStream());
			postStream.writeBytes(postData);
			postStream.flush();
			postStream.close();

			InputStream in = urlConnection.getInputStream();
			FileOutputStream file = new FileOutputStream(filename);

			byte[] buffer = new byte[1024];
			while ((length = in.read(buffer)) > 0) {
				file.write(buffer, 0, length);
			}

			file.close();
			in.close();

			ret = 0;
		} finally {

			if (urlConnection != null) {
				urlConnection.disconnect();
			}
		}

		return ret;
	}

	public static int download(String targetURL, String filename) throws Exception {
		int ret = -1;
		int length;
		HttpURLConnection urlConnection = null;
		
		try {
			URL url = new URL(targetURL);
			urlConnection = (HttpURLConnection) url.openConnection();

			InputStream in = urlConnection.getInputStream();
			FileOutputStream file = new FileOutputStream(filename);
			
			byte[] buffer = new byte[1024];
			while ((length = in.read(buffer)) > 0) {
				file.write(buffer, 0, length);
			}

			file.close();
			in.close();
			ret = 0;

		} finally {

			if (urlConnection != null) {
				urlConnection.disconnect();
				return -1;
			}
		}

		return ret;
	}

	/**
	 * 下載每日收盤行情(全部(不含權證、牛熊證))
	 * 
	 * @param cal
	 * @return
	 * @throws Exception
	 */
	public static int downloadDailyTradeStocks(Calendar cal) throws Exception {
		final int year = cal.get(Calendar.YEAR);
		final int month = cal.get(Calendar.MONTH) + 1;
		final int day = cal.get(Calendar.DATE);
		final String urlTWSE = "http://www.twse.com.tw/ch/trading/exchange/MI_INDEX/MI_INDEX.php?";
		final String FILE_TYPE = "csv";
		final String DATA_TYPE = "ALLBUT0999"; // 全部(不含權證、牛熊證、可展延牛熊證)
		final String twDate = String.format("%03d/%02d/%02d", year - 1911, month, day);
		final String filename = String.format(Environment.DailyTradeStocksPath + "%04d%02d%02d.csv", year, month, day);
		
		Log.info("Download daily trade stocks on " + twDate);
		
		if (year < 2004 && month < 2 && day < 11)
			throw new Exception("Date is earlier than 2004/2/11");

		String postData = String.format("download=%s&selectType=%s&qdate=%s", URLEncoder.encode(FILE_TYPE, "UTF-8"),
		        URLEncoder.encode(DATA_TYPE, "UTF-8"), URLEncoder.encode(twDate, "UTF-8"));

		return download(urlTWSE, postData, filename);
	}

	public static int downloadDailyTradeStocks(Calendar startDate, int count) throws Exception {
		for (int i = 0; i < count; i++) {
			downloadDailyTradeStocks(startDate);
			startDate.add(Calendar.DATE, 1);
		}

		return 0;
	}

	public static int downloadDailyTradeStocks(Calendar startDate, Calendar endDate) throws Exception {
		if (startDate.compareTo(endDate) > 0)
			throw new Exception("Date order error");

		while (startDate.compareTo(endDate) <= 0) {
			downloadDailyTradeStocks(startDate);
			startDate.add(Calendar.DATE, 1);
		}

		return 0;
	}
	
	public static int supplementDailyTradeStocks(Calendar startDate, Calendar endDate) throws Exception {
		Calendar startCal = MyDB.getLastTradeDate();
		Calendar endCal = Calendar.getInstance();
		return downloadDailyTradeStocks(startCal, endCal);
	}

	public static void main(String[] args) {
		Calendar cal = Calendar.getInstance(); // 現在時間
		cal.set(2004, 1, 11); // 月份是 0-base的
		Calendar cal2 = (Calendar) cal.clone();
		cal2.set(2015, 9, 23); // 月份是 0-base的
		try {
			downloadDailyTradeStocks(cal,cal2);
		} catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			System.exit(-1);
		}
	}
}
