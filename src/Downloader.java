import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.sql.ResultSet;
import java.sql.Statement;
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

		Log.info_("Download daily trade " + twDate + " :");

		if (year < 2004 && month < 2 && day < 11)
			throw new Exception("Date is earlier than 2004/2/11");

		String postData = String.format("download=%s&selectType=%s&qdate=%s", URLEncoder.encode(FILE_TYPE, "UTF-8"),
				URLEncoder.encode(DATA_TYPE, "UTF-8"), URLEncoder.encode(twDate, "UTF-8"));

		if (download(urlTWSE, postData, filename) != 0) {
			Log.info_("Fail\n");
			return -1;
		}

		Log.info_("Done\n");
		return 0;
	}

	public static void supplementDailyTradeStocks(Calendar startCal) throws Exception {
		Calendar endCal = Calendar.getInstance();
		if (startCal.compareTo(endCal) > 0)
			throw new Exception("Date order error");

		while (startCal.compareTo(endCal) <= 0) {
			downloadDailyTradeStocks(startCal);
			startCal.add(Calendar.DATE, 1);
		}
	}

	public static int downloadRevenue(int year, int month) throws Exception {
		final String inUrl = String.format("http://mops.twse.com.tw/nas/t21/sii/t21sc03_%d_%d_0.html", year - 1911,
				month);
		final String exUrl = String.format("http://mops.twse.com.tw/nas/t21/sii/t21sc03_%d_%d_1.html", year - 1911,
				month);
		final String inFilename = String.format(Environment.MonthlyRevenue + "%04d%02d_sii_0.html", year, month);
		final String exFilename = String.format(Environment.MonthlyRevenue + "%04d%02d_sii_1.html", year, month);
		int ret;

		if (year < 2013)
			throw new Exception("Year is earlier than 2013");

		Log.info_("Download Revenue " + year + "-" + month + ". sii_0:");

		ret = download(inUrl, inFilename);
		if (ret != 0) {
			Log.info_("Fail\n");
			return ret;
		}

		Log.info_("Done  sii_1:");
		ret = download(exUrl, exFilename);
		if (ret != 0) {
			Log.info_("Fail\n");
			return ret;
		}
		Log.info_("Done\n");
		return 0;
	}

	/**
	 * 下載從指定年月到現在為止的檔案
	 * 
	 * @param year
	 *            指定年
	 * @param month
	 *            指定月 (1-base)
	 * @throws Exception
	 */
	public static void supplementRevenue(int year, int month) throws Exception {
		Calendar endCal = Calendar.getInstance();
		int eYear = endCal.get(Calendar.YEAR);
		int eMonth = endCal.get(Calendar.MONTH) + 1;
		while (year < eYear || (year == eYear && month <= eMonth)) {
			if (downloadRevenue(year, month) != 0)
				break;

			if (++month > 12) {
				year++;
				month = 1;
			}
		}
	}

	public static int downloadIncomeStatement(int year, int quarter) throws Exception {
		final String url = "http://mops.twse.com.tw/mops/web/t163sb04?";
		final String filename = String.format(Environment.QuarterIncomeStatement + "%04d_Q%d.html", year, quarter);

		if (year < 2013)
			throw new Exception("Year is earlier than 2013");

		Log.info_(String.format("Download Income Statement %04d Q%d: ", year, quarter));

		String postData = String.format("step=1&firstin=true&off=1&year=%s&season=%s",
				URLEncoder.encode(String.valueOf(year - 1911), "UTF-8"),
				URLEncoder.encode(String.valueOf(quarter), "UTF-8"));

		if (download(url, postData, filename) != 0) {
			Log.info_("Fail\n");
			return -1;
		}

		Log.info_("Done\n");
		return 0;
	}

	public static void downloadBasicData() throws Exception {
		int ret;
		String filename, url;
		MyDB db = new MyDB();
		Statement stmt = db.conn.createStatement();
		ResultSet result = stmt.executeQuery("SELECT Code FROM company");

		while (result.next()) {
			
			String code = result.getString(("Code"));
			Log.info_(code);
			
			Log.info_("資產負債季表:");
			filename = Environment.QuarterDebt + code + ".html";
			url = "http://www.emega.com.tw/z/zc/zcp/zcpa/zcpa_" + code + ".asp.htm";
			ret = download(url, filename);
			if (ret != 0) {
				Log.info_("Fail\n");
				//continue;
			}
			Log.info_("Done. ");
			
			Log.info_("資產負債年表:");
			filename = Environment.YearDebt + code + ".html";
			url = "http://www.emega.com.tw/z/zc/zcp/zcpb/zcpb_" + code + ".asp.htm";
			ret = download(url, filename);
			if (ret != 0) {
				Log.info_("Fail\n");
				//continue;
			}
			Log.info_("Done. ");
			
			Log.info_("損益季表:");
			filename = Environment.QuarterIncome + code + ".html";
			url = "http://www.emega.com.tw/z/zc/zcq/zcq_" + code + ".asp.htm";
			ret = download(url, filename);
			if (ret != 0) {
				Log.info_("Fail\n");
				//continue;
			}
			Log.info_("Done. ");
			
			Log.info_("損益年表:");
			filename = Environment.YearIncome + code + ".html";
			url = "http://www.emega.com.tw/z/zc/zcq/zcqa/zcqa_" + code + ".asp.htm";
			ret = download(url, filename);
			if (ret != 0) {
				Log.info_("Fail\n");
				//continue;
			}
			Log.info_("Done. ");

			Log.info_(" 財務比率季表:");
			filename = Environment.QuarterFinancial + code + ".html";
			url = "http://www.emega.com.tw/z/zc/zcr/zcr_" + code + ".asp.htm";
			ret = download(url, filename);
			if (ret != 0) {
				Log.info_("Fail\n");
				//continue;
			}
			Log.info_("Done. ");
			
			Log.info_("財務比率年表:");
			filename = Environment.YearFinancial + code + ".html";
			url = "http://www.emega.com.tw/z/zc/zcr/zcra/zcra_" + code + ".asp.htm";
			ret = download(url, filename);
			if (ret != 0) {
				Log.info_("Fail\n");
				//continue;
			}
			Log.info_("Done. ");
			
			Log.info_("現金流量表:");
			filename = Environment.CashFlow + code + ".html";
			url = "http://www.emega.com.tw/z/zc/zc3/zc3_" + code + ".asp.htm";
			ret = download(url, filename);
			if (ret != 0) {
				Log.info_("Fail\n");
				//continue;
			}
			Log.info_("Done. ");
						
			Log.info_("股本形成 :");
			filename = Environment.Capitalization + code + ".html";
			url = "http://www.emega.com.tw/z/zc/zcb/zcb_" + code + ".asp.htm";
			ret = download(url, filename);
			if (ret != 0) {
				Log.info_("Fail\n");
				//continue;
			}
			Log.info_("Done. ");
			
			Log.info_("股利政策:");
			filename = Environment.Dividend + code + ".html";
			url = "http://www.emega.com.tw/z/zc/zcc/zcc_" + code + ".asp.htm";
			ret = download(url, filename);
			if (ret != 0) {
				Log.info_("Fail\n");
				//continue;
			}
			Log.info_("Done. ");
			
			Log.info_("經營績效 :");
			filename = Environment.Performance + code + ".html";
			url = "http://www.emega.com.tw/z/zc/zcd/zcd_" + code + ".asp.htm";
			ret = download(url, filename);
			if (ret != 0) {
				Log.info_("Fail\n");
				//continue;
			}
			Log.info_("Done. ");
			
			Log.info_("獲利能力 :");
			filename = Environment.Profitability + code + ".html";
			url = "http://www.emega.com.tw/z/zc/zce/zce_" + code + ".asp.htm";
			ret = download(url, filename);
			if (ret != 0) {
				Log.info_("Fail\n");
				//continue;
			}
			Log.info_("Done.");
			Log.info_("\n");
		}
	}

	public static void main(String[] args) {
		Calendar cal = Calendar.getInstance(); // 現在時間
		cal.set(2015, 1, 1); // 月份是 0-base的
		// Calendar cal2 = (Calendar) cal.clone();
		// cal2.set(2015, 9, 23); // 月份是 0-base的
		try {
			MyDB db = new MyDB();
			Calendar startTradeDate = db.getLastTradeDate();
			Calendar startRevenu = db.getLastRevenue();
			db.close();

			supplementDailyTradeStocks(startTradeDate);
			// supplementRevenue(startRevenu.get(Calendar.YEAR),
			// startRevenu.get(Calendar.MONTH) + 1);

			// download("http://mops.twse.com.tw/nas/t21/sii/t21sc03_104_7_0.html",
			// "Data\\每月營收總表\\t21sc03_104_7_0.html");
			//downloadIncomeStatement(2015, 1);
			//downloadBasicData();

		} catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			System.exit(-1);
		}
	}
}
