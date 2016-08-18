import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 * @author LenticeTsai
 *
 */
public class Downloader {

	public static void createFolder(String path) throws Exception {
		File dir = new File(path);
		if (!dir.exists())
			dir.mkdirs();
	}

	/**
	 * 檢查下載是否成功: 檔案太小代表失敗
	 * 
	 * @param filename
	 * @return true: 下載成功 ; false: 下載失敗或檔案不存在
	 */
	public static boolean isValidFile(String filename, int minSize) throws Exception {
		File file = new File(filename);
		if (!file.exists())
			return false;

		if (file.length() < minSize) {
			file.delete();
			return false;
		}

		return true;
	}

	/**
	 * Down http file via POST method
	 * @param targetURL
	 * @param postData
	 * @param filename
	 * @return
	 * @throws IOException
	 */
	public static int httpDownload(String targetURL, String postData, String filename) throws IOException {
		int length;
		int ret = -1;
		HttpURLConnection urlConnection = null;

		try {
			InputStream in;
			DataOutputStream postStream;

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
			try {
				postStream = new DataOutputStream(urlConnection.getOutputStream());
			} catch (java.net.ConnectException ex) {
				urlConnection.disconnect();
				return -1;
			}
			postStream.writeBytes(postData);
			postStream.flush();
			postStream.close();

			try {
				in = urlConnection.getInputStream();
			} catch (java.io.IOException ex) {
				urlConnection.disconnect();
				return -1;
			}

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

	/**
	 * Download http file via GET method
	 * @param targetURL
	 * @param filename
	 * @return
	 * @throws Exception
	 */
	public static int httpDownload(String targetURL, String filename) throws Exception {
		int length;
		HttpURLConnection urlConnection = null;
		InputStream in = null;

		URL url = new URL(targetURL);
		urlConnection = (HttpURLConnection) url.openConnection();

		try {
			in = urlConnection.getInputStream();
		} catch (java.io.IOException ex) {
			urlConnection.disconnect();
			return -1;
		}
		FileOutputStream file = new FileOutputStream(filename);

		byte[] buffer = new byte[1024];
		while ((length = in.read(buffer)) > 0) {
			file.write(buffer, 0, length);
		}

		file.close();
		in.close();

		urlConnection.disconnect();

		return 0;
	}

	public static void main(String[] args) {
		try {
			int year = 2015;
			int month = 9; 
			final String url = "http://www.twse.com.tw/ch/trading/indices/MI_5MINS_HIST/MI_5MINS_HIST.php";
			final String twDate = String.format("%03d%02d", year - 1911, month);
			final String filename = String.format(DataPath.DailyTaiExPath + "%04d%02d.htm", year, month);

			Log.info("Download daily trade " + twDate);

			if (year < 2004 || (year == 2004 && month < 2))
				throw new Exception("Date is earlier than 2004/2");

			String postData = String.format("myear=%03d&mmon=%02d",
					year - 1911, month);

			if (httpDownload(url, postData, filename) != 0) {
				Log.warn("Fail");
				return;
			}
		} catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			System.exit(-1);
		}
	}
}
