package lstockv2;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.Scanner;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * @author LenticeTsai
 *
 */
public class Downloader {

	private static final Logger log = LogManager.getLogger(Downloader.class.getName());

	private enum QuarterTableType {
		BALANCE_SHEET, INCOME_STATEMENT, CASHFLOW_STATEMENT
	}

	private static String filename;
	private static String url;

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
	 * Download HTTP file via POST method
	 * 
	 * @param targetURL
	 * @param postData
	 * @param filename
	 * @return
	 * @throws IOException
	 */
	public static boolean httpPost(String targetURL, String postData, String filename) {
		int length;
		HttpURLConnection urlConnection = null;

		try {
			InputStream in;
			DataOutputStream postStream;

			URL url = new URL(targetURL);

			// Set URL connection contain
			urlConnection = (HttpURLConnection) url.openConnection();
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
			e.printStackTrace();
			log.warn("targetURL=" + targetURL + " postData=" + postData + " filename=" + filename);
			System.exit(-1);
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
	public static boolean httpGet(String targetURL, String filename) {
		int length;
		HttpURLConnection urlConnection = null;
		InputStream in = null;

		try {
			URL url = new URL(targetURL);
			urlConnection = (HttpURLConnection) url.openConnection();
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
			e.printStackTrace();
			log.warn("targetURL=" + targetURL + " filename=" + filename);
			System.exit(-1);
			return false;
		} finally {
			if (urlConnection != null) {
				urlConnection.disconnect();
			}
		}

		return true;
	}

	// 下載每日台灣加權股價指數
	// 台灣加權股價指數是記錄在每個月資料中，因此下載整個月的資料，再從中取得所需資料
	public static boolean dailyTaiEx(final int year, final int month) {
		final String url = "http://www.twse.com.tw/ch/trading/indices/MI_5MINS_HIST/MI_5MINS_HIST.php";
		final String filename = String.format(DataPath.MONTHLY_TAIEX_PATH + "%04d%02d.htm", year, month);

		log.info("Download daily TaiEx to " + filename);

		if (year < 2004 || (year == 2004 && month < 2)) {
			log.error("Date is earlier than 2004/02");
			return false;
		}

		// example: "year=105&mmon=03"
		final String postData = String.format("myear=%03d&mmon=%02d", year - 1911, month);

		if (!httpPost(url, postData, filename)) {
			log.warn(String.format("Download daily TaiEx %04d_%02d failed", year, month));
			return false;
		}

		return true;
	}

	// 下載每日全部個股收盤行情
	public static boolean dailyStocks(final int year, final int month, final int day) {
		final int twYear = year - 1911;
		final String urlTWSE = "http://www.twse.com.tw/ch/trading/exchange/MI_INDEX/MI_INDEX.php";
		final String twDate = String.format("%03d/%02d/%02d", twYear, month, day);
		final String filename = String.format(DataPath.DAILY_TRADE_STOCKS_PATH + "%04d%02d%02d.csv", year, month, day);

		log.info("Download daily Stocks to" + filename);

		if (year < 2004 || (year == 2004 && month < 2) || (year == 2004 && month == 2 && day < 11)) {
			log.error("Date is earlier than 2004/02");
			return false;
		}

		String twDateEncoded;
		try {
			twDateEncoded = URLEncoder.encode(twDate, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			// System.err.println(e.getMessage());
			e.printStackTrace();
			log.warn("twDate " + twDate);
			return false;
		}

		final String postData = String.format("download=csv&qdate=%s&selectType=ALLBUT0999", twDateEncoded);
		if (!httpPost(urlTWSE, postData, filename)) {
			log.warn(String.format("Download daily stocks %04d_%02d_%02d failed", year, month, day));
			return false;
		}

		return true;
	}

	// 下載月營收總表
	public static boolean monthlyRevenue(final int year, final int month) {

		final int twYear = year - 1911;

		if (year < 2004) {
			log.error("Date is earlier than 2004");
			return false;
		}

		String url, filename;
		if (year < 2013) {
			url = String.format("http://mops.twse.com.tw/nas/t21/sii/t21sc03_%d_%d.html", twYear, month);
			filename = String.format(DataPath.MONTHLY_REVENUE_PATH + "%04d%02d_sii.html", year, month);
		} else {
			// 市場別：國內
			url = String.format("http://mops.twse.com.tw/nas/t21/sii/t21sc03_%d_%d_0.html", twYear, month);
			filename = String.format(DataPath.MONTHLY_REVENUE_PATH + "%04d%02d_sii_0.html", year, month);
			log.info("Download revenue to " + filename);
			if (!httpGet(url, filename)) {
				log.warn(String.format("Download monthly revenue %04d_%02d failed", year, month));
				return false;
			}

			// 市場別：國外
			url = String.format("http://mops.twse.com.tw/nas/t21/sii/t21sc03_%d_%d_1.html", twYear, month);
			filename = String.format(DataPath.MONTHLY_REVENUE_PATH + "%04d%02d_sii_1.html", year, month);
			log.info("Download revenue to " + filename);
			if (!httpGet(url, filename)) {
				log.warn(String.format("Download monthly revenue %04d_%02d failed", year, month));
				return false;
			}
		}

		return true;
	}

	private static boolean quarterlyGetDownloadInfo(final int year, final int quarter, final QuarterTableType tableType,
	        final boolean idv, // TRUE: 個別財報 , FALSE: 合併財報
	        final String companyID, final boolean isFinancial, final boolean useIFRSs) {

		final int twYear = year - 1911;

		String formAction = "";
		String folderPath = "";

		switch (tableType) {
		case BALANCE_SHEET:
			folderPath = DataPath.QUARTERLY_BALANCE_SHEET;
			if (idv) {
				formAction = "/mops/web/ajax_t05st31";
			} else {
				if (useIFRSs)
					formAction = "/mops/web/ajax_t164sb03";
				else
					formAction = "/mops/web/ajax_t05st33";
			}
			break;
		case INCOME_STATEMENT:
			folderPath = DataPath.QUARTERLY_INCOME_STATEMENT;
			if (idv) {
				formAction = "/mops/web/ajax_t05st32";
			} else {
				if (useIFRSs)
					formAction = "/mops/web/ajax_t164sb04";
				else
					formAction = "/mops/web/ajax_t05st34";
			}
			break;
		case CASHFLOW_STATEMENT:
			folderPath = DataPath.QUARTERLY_CASHFLOW_STATEMENT;
			if (idv) {
				formAction = "/mops/web/ajax_t05st36";
			} else {
				if (useIFRSs)
					formAction = "/mops/web/ajax_t164sb05";
				else
					formAction = "/mops/web/ajax_t05st39";
			}
			break;
		}

		if (idv) {
			filename = String.format(folderPath + "%s_%04d_%d_idv.html", companyID, year, quarter);
		} else {
			filename = String.format(folderPath + "%s_%04d_%d.html", companyID, year, quarter);
		}

		String postData;
		if (useIFRSs && (isFinancial || companyID.equals("5871") || companyID.equals("2841")))
			postData = "encodeURIComponent=1&id=&key=&TYPEK=sii&step=2&firstin=1&";
		else if (!useIFRSs && isFinancial)
			postData = "encodeURIComponent=1&check2858=Y&firstin=1&keyword4=&TYPEK=sii&checkbtn=&firstin=1&encodeURIComponent=1&queryName=co_id&off=1&code1=&isnew=false&TYPEK2=&step=1&";
		else
			postData = "step=1&firstin=1&off=1&keyword4=&code1=&TYPEK2=&checkbtn=&queryName=co_id&TYPEK=all&isnew=false&";

		try {
			postData = postData + String.format("co_id=%s&year=%s&season=0%s", URLEncoder.encode(companyID, "UTF-8"),
			        URLEncoder.encode(String.valueOf(twYear), "UTF-8"),
			        URLEncoder.encode(String.valueOf(quarter), "UTF-8"));
		} catch (UnsupportedEncodingException e) {
			// System.err.println(e.getMessage());
			e.printStackTrace();
			log.warn("year=" + year + " quarter=" + quarter + " companyID=" + companyID);
			return false;
		}

		url = "http://mops.twse.com.tw" + formAction + "?" + postData;

		return true;
	}

	private static boolean quarterlyNeedRetry(File file) {

		if (!file.isFile() || file.length() > 1024)
			return false;

		try {
			Scanner scannerUTF8 = new Scanner(file, "UTF-8");
			while (scannerUTF8.hasNextLine()) {
				final String lineFromFile = scannerUTF8.nextLine();
				if (lineFromFile.contains("查詢過於頻繁") || lineFromFile.contains("資料庫連線時發生下述問題")) {
					scannerUTF8.close();
					return true;
				}
			}
			scannerUTF8.close();

			Scanner scannerMS950 = new Scanner(file, "MS950");
			while (scannerMS950.hasNextLine()) {
				final String lineFromFile = scannerMS950.nextLine();
				if (lineFromFile.contains("查詢過於頻繁")) {
					scannerMS950.close();
					return true;
				}
			}
			scannerMS950.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			return false;
		}

		return false;
	}

	private static boolean quarterlyNoQuarterDataExist(File file) {

		if (!file.isFile() || file.length() > 1024)
			return false;

		try {
			Scanner scanner = new Scanner(file, "UTF-8");
			while (scanner.hasNextLine()) {
				final String lineFromFile = scanner.nextLine();
				if (lineFromFile.contains("此公司代號不存") || lineFromFile.contains("公司已下市")
				        || lineFromFile.contains("不繼續公開發行") || lineFromFile.contains("查無需求資料")
				        || lineFromFile.contains("查無所需資料") || lineFromFile.contains("請至合併財務報表查詢")
				        || lineFromFile.contains("無應編製合併財報之子公司") || lineFromFile.contains("第二上市（櫃）")
				        || lineFromFile.contains("第二上市(櫃)")) {

					scanner.close();
					return true;
				}
			}
			scanner.close();

		} catch (FileNotFoundException e) {
			e.printStackTrace();
			return false;
		}

		return false;
	}

	private static boolean quartelyGetTable(final int year, final int quarter, final QuarterTableType tableType,
	        final boolean idv, // 個別財報: True, 合併財報: False
	        final String companyID, final boolean isFinancial, final boolean useIFRSs) {

		final int MAX_DOWNLOAD_RETRY = 20;

		quarterlyGetDownloadInfo(year, quarter, tableType, idv, companyID, isFinancial, useIFRSs);

		// check if already exist a valid file.
		if (new File(filename).isFile()) {
			return true;
		}

		boolean result = false;
		try {
			for (int iRetry = 0; iRetry <= MAX_DOWNLOAD_RETRY; iRetry++) {
				if (!httpGet(url, filename))
					continue;

				File file = new File(filename);
				if (quarterlyNeedRetry(file)) {
					Thread.sleep(10000);
					continue;
				}

				if (quarterlyNoQuarterDataExist(file)) {
					result = true;
					break;
				}

				log.warn("Unknow file contain " + filename);
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		return result;
	}

	public static boolean quarterlyBasicTables(final int year, final int quarter, final String companyID,
	        final boolean useIFRSs, final boolean isFinancial) {

		boolean result = true;

		if (!quartelyGetTable(year, quarter, QuarterTableType.BALANCE_SHEET, false, companyID, isFinancial, useIFRSs))
			result = false;

		if (!quartelyGetTable(year, quarter, QuarterTableType.INCOME_STATEMENT, false, companyID, isFinancial,
		        useIFRSs))
			result = false;

		if (!quartelyGetTable(year, quarter, QuarterTableType.CASHFLOW_STATEMENT, false, companyID, isFinancial,
		        useIFRSs))
			result = false;

		if (!useIFRSs) {
			if (!quartelyGetTable(year, quarter, QuarterTableType.BALANCE_SHEET, true, companyID, isFinancial,
			        useIFRSs))
				result = false;

			if (!quartelyGetTable(year, quarter, QuarterTableType.INCOME_STATEMENT, true, companyID, isFinancial,
			        useIFRSs))
				result = false;

			if (!quartelyGetTable(year, quarter, QuarterTableType.CASHFLOW_STATEMENT, true, companyID, isFinancial,
			        useIFRSs))
				result = false;
		}

		return result;
	}

	// 下載年度除權息資料
	public static boolean annualDividend(final int year) {
		try {
			final String url = String.format(
			        "http://goodinfo.tw/StockInfo/StockDividendScheduleList.asp?MARKET_CAT=上市&INDUSTRY_CAT=全部&YEAR=%d",
			        year);
			final String filename = String.format(DataPath.ANNUAL_DIVIDEND + "GoodInfo_%d.htm", year);

			log.info("Download annual dividend to " + filename);

			if (!httpGet(url, filename)) {
				log.warn("Download %d dividend failed", year);
				return false;
			}
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}

		try {
			Thread.sleep(10000);
		} catch (InterruptedException e) {
			e.printStackTrace();
			System.exit(-1);
		}
		return true;
	}

	public static void main(String[] args) throws InterruptedException {
		for (int i = 2003; i < 2014; i++) {
			annualDividend(i);
			Thread.sleep(10000);
		}
	}
}
