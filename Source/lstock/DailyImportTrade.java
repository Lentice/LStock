package lstock;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.sql.Date;
import java.util.Calendar;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import lstock.ui.RebuildUI;

public class DailyImportTrade implements Runnable {
	private static final Logger log = LogManager.getLogger(DailyImportTrade.class.getName());

	private static final String FOLDER_PATH = DataPath.DAILY_TRADE_STOCKS_PATH;
	private static final long MIN_DOWNLOAD_GAP = 1000;
	private static final Downloader downloader = new Downloader(MIN_DOWNLOAD_GAP);

	private static MyStatement companyST;
	private static MyStatement dailyST;
	private static MyStatement taiExST;
	
	private static final Object uiLock = new Object();
	private static int totalDays;
	private static int oldPercentage;
	private static int daysImported;

	private int year;
	private int month;
	private int day;
	private String fullFilePath;

	DailyImportTrade(final Calendar cal) {
		year = cal.get(Calendar.YEAR);
		month = cal.get(Calendar.MONTH) + 1;
		day = cal.get(Calendar.DATE);

		fullFilePath = FOLDER_PATH + String.format("%04d%02d%02d.csv", year, month, day);
	}
	
	public static int daysBetween(Calendar first, Calendar second)
	{
		long msDiff = second.getTimeInMillis() - first.getTimeInMillis();
	    return (int)(msDiff / (24 * 60 * 60 * 1000));
	}

	/**
	 * 刪除最後取得的資料 最後的檔案可能是尚未更新的資料
	 */
/*
	 private static boolean removeLatestFile(final Calendar cal) {
		final File dir = new File(FOLDER_PATH);
		if (!dir.isDirectory())
			return false;

		final File[] files = dir.listFiles();
		if (files.length == 0)
			return false;

		Comparator<File> comparator = new Comparator<File>() {
			@Override
			public int compare(File f1, File f2) {
				return ((File) f1).getName().compareTo(((File) f2).getName());
			}
		};

		Arrays.sort(files, comparator);
		final File lastfile = files[files.length - 1];
		log.info("Remove last file " + lastfile.getName());
		lastfile.delete();
	
		return true;
	}
*/

	// 下載每日全部個股收盤行情
	private boolean download() {
		final File file = new File(fullFilePath);
		if (file.isFile()) {
			log.debug("Daily trade file already exist: " + fullFilePath);
			return true;
		}

		final int twYear = year - 1911;
		final String urlTWSE = "http://www.twse.com.tw/ch/trading/exchange/MI_INDEX/MI_INDEX.php";
		final String twDate = String.format("%03d/%02d/%02d", twYear, month, day);

		if (year < 2004 || (year == 2004 && month < 2) || (year == 2004 && month == 2 && day < 11)) {
			log.error("Date is earlier than 2004/02");
			return false;
		}

		String twDateEncoded;
		try {
			twDateEncoded = URLEncoder.encode(twDate, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
			log.warn("twDate " + twDate);
			return false;
		}

		final String postData = String.format("download=csv&qdate=%s&selectType=ALLBUT0999", twDateEncoded);
		if (!downloader.httpPost(urlTWSE, postData, fullFilePath)) {
			log.warn(String.format("Download daily stocks %04d_%02d_%02d failed", year, month, day));
			return false;
		}
		log.info("Downloaded daily Stocks to" + fullFilePath);
		RebuildUI.addDownload(fullFilePath);

		return true;
	}

	public void run() {
		final String uiMessage = String.format("匯入收盤行情: %04d-%02d-%02d", year, month, day);
		
		try {
			RebuildUI.addProcess(uiMessage);
			
			if (!download())
				return;
			
			final Date date = Date.valueOf(String.format("%04d-%02d-%02d", year, month, day));
			final File file = new File(fullFilePath);
			BufferedReader fileBR = new BufferedReader(new InputStreamReader(new FileInputStream(file), "MS950"), 40960);
			
			String[] taiExData;
			String line;
			int idx;
			boolean noData = true;
			while ((line = fileBR.readLine()) != null) {
				if (line.contains("查無資料")) {
					log.debug("查無資料: " + fullFilePath);
					fileBR.close();
					return;
				}

				if (line.contains("總計(1~12)")) { // 取得大盤成交資料
					taiExData = parseDailyCsvLine(line);

					idx = 1;
					synchronized (taiExST.AcquireLock()) {
						taiExST.setDate(idx++, date); // Date
						taiExST.setBigInt(idx++, taiExData[1]); // 成交金額
						taiExST.setBigInt(idx++, taiExData[2]); // 成交股數
						taiExST.setBigInt(idx++, taiExData[3]); // 成交筆數
						taiExST.addBatch();
					}
				}

				if (line.contains("證券名稱")) { // 找到個股的標題
					noData = false;
					break;
				}
			}

			if (noData) {
				log.warn("內容無法判別： " + fullFilePath);
				fileBR.close();
				return;
			}

			while ((line = fileBR.readLine()) != null) {
				if (line.length() < 10)
					continue;

				String[] row = parseDailyCsvLine(line);

				// TODO: FIX ME
				try {
					Integer.parseInt(row[0]);
				} catch (NumberFormatException | NullPointerException e) {
					continue;
				}

				// companyST and dailyST share the same lock to avoid race
				// condition: dailyST was add while company is not exist in
				// companyST
				synchronized (companyST.AcquireLock()) {
					idx = 1;
					companyST.setInt(idx++, Integer.parseInt(row[0])); // StockNum
					companyST.setChar(idx++, row[0]); // Code
					companyST.setChar(idx++, row[1]); // Name
					companyST.setDate(idx++, date); // first_update
					companyST.setDate(idx++, date); // last_update
					companyST.setFloat(idx++, row[8]); // 現價
					companyST.setFloat(idx++, row[15]); // 本益比
					companyST.addBatch();

					idx = 1;
					dailyST.setDate(idx++, date); // Date
					dailyST.setInt(idx++, row[0]); // StockNumm
					dailyST.setBigInt(idx++, row[2]); // 成交股數
					dailyST.setBigInt(idx++, row[3]); // 成交筆數
					dailyST.setBigInt(idx++, row[4]); // 成交金額
					dailyST.setFloat(idx++, row[5]); // 開盤價
					dailyST.setFloat(idx++, row[6]); // 最高價
					dailyST.setFloat(idx++, row[7]); // 最低價
					dailyST.setFloat(idx++, row[8]); // 收盤價
					dailyST.setFloat(idx++, row[15]); // 本益比
					dailyST.addBatch();
				}
			}

			fileBR.close();
			

		} catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);
		} finally {
			RebuildUI.removeProcess(uiMessage);
			synchronized (uiLock) {
				int percentage = ++daysImported * 100 / totalDays;
				if (oldPercentage != percentage) {
					oldPercentage = percentage;
					RebuildUI.updateProgressBar(percentage);
				}
			}
		}
	}

	private String[] parseDailyCsvLine(String line) {
		String splitRegex = "\",\""; // 把 「","」當作分隔符號

		// 去頭 「="」 或 「"」 去尾「"」
		if (line.charAt(0) == '=')
			line = line.substring(2, line.length() - 1);
		else if (line.charAt(0) == '"')
			line = line.substring(1, line.length() - 1);
		else
			splitRegex = ","; // 把 「,」當作分隔符號

		String[] rowData = line.split(splitRegex);
		for (int i = 0; i < rowData.length; i++) {
			rowData[i] = rowData[i].trim();

			// 未交易 沒有數值
			if (rowData[i].equals("--"))
				rowData[i] = null;
		}

		// log.trace(Arrays.toString(rowData));
		return rowData;
	}

	public static void importToDB(MyDB db) {
		
		Calendar startCal = db.getLastDailyTradeDate();
		startCal.add(Calendar.DATE, 1);
		
		Calendar endCal = Calendar.getInstance();
		if (endCal.get(Calendar.HOUR_OF_DAY) < 19) {
			endCal.add(Calendar.DATE, -1);
		}

		totalDays = daysBetween(startCal, endCal);
		if (totalDays == 0) {
			RebuildUI.updateProgressBar(100);
			return;
		}
		
		RebuildUI.updateProgressBar(0);
		daysImported = 0;
		oldPercentage = 0;

		new File(FOLDER_PATH).mkdirs(); // create folder if it was not exist

		final String insertComp = "INSERT INTO company (StockNum, Code, Name, first_update, last_update, 現價, 本益比) " 
				+ "VALUES (?, ?, ?, ?, ?, ?, ?) "
				+ "ON DUPLICATE KEY UPDATE " 
				+ "現價 = IF(VALUES(last_update) > last_update AND NOT ISNULL(VALUES(現價)), VALUES(現價), 現價)"
				+ ", 本益比 = IF(ISNULL(VALUES(本益比)), 本益比, VALUES(本益比))"
				+ ", last_update = GREATEST(VALUES(last_update), last_update)"
				+ ", first_update = LEAST(VALUES(first_update), first_update)";
				
		companyST = new MyStatement(db.conn, insertComp);

		taiExST = new MyStatement(db.conn);
		taiExST.setStatementInsertAndUpdate("daily_summary", "Date", "成交金額", "成交股數", "成交筆數");

		dailyST = new MyStatement(db.conn);
		dailyST.setStatementInsertAndUpdate("daily", "Date", "StockNum", "成交股數", "成交筆數", "成交金額", "開盤價", "最高價", "最低價",
				"收盤價", "本益比");

		
		//removeLatestFile(startCal);

		
		MyThreadPool threadPool = new MyThreadPool();

		while (startCal.compareTo(endCal) <= 0) {

			threadPool.add(new DailyImportTrade(startCal));

			startCal.add(Calendar.DATE, 1);
		}
		threadPool.waitFinish();
		

		companyST.close();
		dailyST.close();
		taiExST.close();
	}

	public static void main(String[] args) throws Exception {
		MyDB db = new MyDB();
		importToDB(db);
		db.close();
		log.info("Done!!");
	}
}
