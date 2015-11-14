import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.net.URLEncoder;
import java.sql.Date;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

/**
 * 發行量加權股價指數歷史資料 (月表)
 * 
 * @author LenticeTsai
 *
 */
class DailyTaiEx {
	static final String folderPath = Environment.DailyTaiExPath;
	static final int NUM_COLUME = 5;

	int year;
	int month;

	File file;

	String[][] data;

	DailyTaiEx(int year, int month) throws Exception {
		this.year = year;
		this.month = month;

		String filename = String.format("%04d%02d.htm", year, month);
		file = new File(folderPath + filename);

		if (!file.exists()) {
			download(year, month);
			file = new File(folderPath + filename);
		}
	}

	boolean parse() throws Exception {
		if (!file.exists()) {
			Log.warn("檔案不存在: " + file.getPath());
			return false;
		}

		Document doc = Jsoup.parse(file, "MS950");
		Elements eTables = doc.getElementsByClass("board_trad");
		if (eTables.size() == 0)
			throw new Exception("eTables size is 0");

		Element eTable = eTables.first();
		Elements eTRs = eTable.select("tbody > tr");
		if (eTRs.size() == 0)
			throw new Exception("eTRs size is 0");

		data = new String[eTRs.size() - 2][NUM_COLUME];
		for (int i = 0; i < data.length; i++) {
			Elements eTDs = eTRs.get(i + 2).children();
			String[] date = HtmlParser.getText(eTDs.get(0)).split("/");
			data[i][0] = String.format("%04d-%02d-%s", year, month, date[2]);
			data[i][1] = HtmlParser.getText(eTDs.get(1));
			data[i][2] = HtmlParser.getText(eTDs.get(2));
			data[i][3] = HtmlParser.getText(eTDs.get(3));
			data[i][4] = HtmlParser.getText(eTDs.get(4));
		}

		return true;
	}

	public void importToDB(MyStatement stm) throws Exception {
		Log.info("Process " + file.getName());

		if (!parse())
			return;

		for (int j = 0; j < data.length; j++) {
			int idx = 1;
			stm.setDate(idx++, Date.valueOf(data[j][0])); // Date
			stm.setDecimal(idx++, data[j][2]); // 開盤指數
			stm.setDecimal(idx++, data[j][1]); // 最高指數
			stm.setDecimal(idx++, data[j][3]); // 最低指數
			stm.setDecimal(idx++, data[j][4]); // 收盤指數
			stm.addBatch();
		}
	}

	int download(int year, int month) throws Exception {
		final String url = "http://www.twse.com.tw/ch/trading/indices/MI_5MINS_HIST/MI_5MINS_HIST.php";
		final String filename = String.format(folderPath + "%04d%02d.htm", year, month);

		Log.info("Download to " + filename);

		if (year < 2004 || (year == 2004 && month < 2))
			throw new Exception("Date is earlier than 2004/02");

		String postData = String.format("myear=%03d&mmon=%02d", year - 1911, month);

		if (Downloader.httpDownload(url, postData, filename) != 0) {
			Log.warn("Fail");
			return -1;
		}

		return 0;
	}

	public static void supplementDB(MyDB db) throws Exception {

		File importDir = new File(folderPath);
		if (!importDir.exists()) {
			Log.warn("路徑不存在: " + importDir);
			System.exit(-1);
		}

		MyStatement stm = new MyStatement(db.conn);
		stm.setInsertOnDuplicateStatement("daily_summary", "Date", "開盤指數", "最高指數", "最低指數", "收盤指數");

		Calendar endCal = Calendar.getInstance();
		int eYear = endCal.get(Calendar.YEAR);
		int eMonth = endCal.get(Calendar.MONTH) + 1;

		int yearMonth = db.getLastDailyTaiExDate();
		int year = yearMonth / 100;
		int month = yearMonth % 100;

		while (year < eYear || (year == eYear && month <= eMonth)) {

			DailyTaiEx taiEx = new DailyTaiEx(year, month);
			taiEx.importToDB(stm);

			if (++month > 12) {
				year++;
				month = 1;
			}
		}
		stm.close();
	}
}

class DailyTradeStocks {
	static final String folderPath = Environment.DailyTradeStocksPath;
	int year;
	int month;
	int day;
	File file;
	boolean noData = true;
	Date date;

	BufferedReader fileBR;
	String[] taiExData = null;

	Iterator<String[]> data;

	DailyTradeStocks(int year, int month, int day) throws Exception {
		this.year = year;
		this.month = month;
		this.day = day;

		String filename = String.format("%04d%02d%02d.csv", year, month, day);
		file = new File(folderPath + filename);

		if (!file.exists()) {
			download();
			file = new File(folderPath + filename);
		}
	}

	boolean parse() throws Exception {
		String line;

		if (!file.exists()) {
			Log.warn("檔案不存在: " + file.getPath());
			return false;
		}

		Log.info("Process: " + file.getPath());
		fileBR = new BufferedReader(new InputStreamReader(new FileInputStream(file), "MS950"));
		while ((line = fileBR.readLine()) != null) {
			if (line.contains("查無資料")) {
				Log.dbg("查無資料");
				return false;
			}

			if (line.contains("1.一般股票")) {
				taiExData = parseDailyCsvLine(line);

			}

			if (line.contains("證券名稱")) {
				noData = false;
				break;
			}
		}

		List<String[]> rows = new ArrayList<>();
		while ((line = fileBR.readLine()) != null) {
			if (line.indexOf(',') == -1)
				continue;

			String[] row = parseDailyCsvLine(line);
			if (!isStringInt(row[0]))
				continue;

			rows.add(row);
		}

		data = rows.iterator();
		date = Date.valueOf(String.format("%04d-%02d-%02d", year, month, day));
		fileBR.close();

		return true;
	}

	public void importToDB(MyDB db) throws Exception {
		if (!parse())
			return;

		String insertComp = "INSERT INTO company (StockNum, Code, Name, last_update) " + "VALUES (?, ?, ?, ?) "
				+ "ON DUPLICATE KEY UPDATE last_update = IF(last_update < VALUES(last_update), VALUES(last_update), last_update)";
		MyStatement companyST = new MyStatement(db.conn, insertComp);

		MyStatement taiEx = new MyStatement(db.conn);
		taiEx.setInsertOnDuplicateStatement("daily_summary", "Date", "成交金額", "成交股數", "成交筆數");

		MyStatement dailyST = new MyStatement(db.conn);
		dailyST.setInsertIgnoreStatement("daily", "Date", "StockNum", "成交股數", "成交筆數", "成交金額", "開盤價", "最高價", "最低價",
				"收盤價", "本益比");

		int idx;
		while (data.hasNext()) {

			String[] row = data.next();
			idx = 1;
			companyST.setInt(idx++, Integer.parseInt(row[0])); // StockNum
			companyST.setChar(idx++, row[0]); // Code
			companyST.setChar(idx++, row[1]); // Name
			companyST.setDate(idx++, date); // last_update
			companyST.addBatch();

			idx = 1;
			dailyST.setDate(idx++, date); // Date
			dailyST.setInt(idx++, row[0]); // StockNum
			dailyST.setBigInt(idx++, row[2]); // 成交股數
			dailyST.setBigInt(idx++, row[3]); // 成交筆數
			dailyST.setBigInt(idx++, row[4]); // 成交金額
			dailyST.setDecimal(idx++, row[5]); // 開盤價
			dailyST.setDecimal(idx++, row[6]); // 最高價
			dailyST.setDecimal(idx++, row[7]); // 最低價
			dailyST.setDecimal(idx++, row[8]); // 收盤價
			dailyST.setDecimal(idx++, row[15]); // 本益比
			dailyST.addBatch();
		}

		idx = 1;
		taiEx.setDate(idx++, date); // Date
		taiEx.setBigInt(idx++, taiExData[1]); // 成交金額
		taiEx.setBigInt(idx++, taiExData[2]); // 成交股數
		taiEx.setBigInt(idx++, taiExData[3]); // 成交筆數
		taiEx.addBatch();

		companyST.close();
		dailyST.close();
		taiEx.close();
	}

	protected boolean isStringFloat(String s) {
		try {
			Float.parseFloat(s);
			return true;
		} catch (NumberFormatException ex) {
			return false;
		}
	}

	protected boolean isStringInt(String s) {
		try {
			Integer.parseInt(s);
			return true;
		} catch (NumberFormatException ex) {
			return false;
		}
	}

	protected String[] parseDailyCsvLine(String line) {
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
			if (rowData[i].length() == 2 && rowData[i].compareTo("--") == 0)
				rowData[i] = null;
		}

		Log.verbose(Arrays.toString(rowData));
		return rowData;
	}

	/**
	 * 下載每日收盤行情(全部(不含權證、牛熊證))
	 * 
	 * @param cal
	 * @return
	 * @throws Exception
	 */
	int download() throws Exception {
		final String urlTWSE = "http://www.twse.com.tw/ch/trading/exchange/MI_INDEX/MI_INDEX.php";
		final String twDate = String.format("%03d/%02d/%02d", year - 1911, month, day);
		final String filename = String.format(folderPath + "%04d%02d%02d.csv", year, month, day);

		Log.info("Download daily trade " + twDate);

		if (year < 2004 || (year == 2004 && month < 2) || (year == 2004 && month == 2 && day < 11))
			throw new Exception("Date is earlier than 2004/2/11");

		String postData = String.format("download=csv&qdate=%s&selectType=ALLBUT0999",
				URLEncoder.encode(twDate, "UTF-8"));

		if (Downloader.httpDownload(urlTWSE, postData, filename) != 0) {
			Log.warn("Fail");
			return -1;
		}

		return 0;
	}

	public static void supplementDB(MyDB db) throws Exception {

		File importDir = new File(Environment.DailyTradeStocksPath);
		if (!importDir.exists()) {
			Log.warn("路徑不存在: " + importDir);
			System.exit(-1);
		}

		Calendar startCal = db.getLastDailyTradeDate();
		Calendar endCal = Calendar.getInstance();
		while (startCal.compareTo(endCal) <= 0) {

			DailyTradeStocks trade = new DailyTradeStocks(startCal.get(Calendar.YEAR), startCal.get(Calendar.MONTH) + 1,
					startCal.get(Calendar.DATE));
			trade.importToDB(db);

			startCal.add(Calendar.DATE, 1);
		}
	}
}

public class ImportDaily {

	public static boolean isValidYear(int year, String lastUpdate) throws Exception {
		int lastDate = Integer.parseInt(lastUpdate);
		int lastYear = lastDate / 10000;
		int lastMonth = lastDate / 100 % 100;

		if (year < lastYear - 1)
			return true;
		else if (year == lastYear - 1 && (lastMonth > 3))
			return true;

		return false;
	}

	/**
	 * 刪除最後取得的資料 最後一天可能是取得尚未更新的資料
	 */
	public static void removeLatestFile(String folderPath) {
		File dir = new File(folderPath);
		if (!dir.exists())
			return;

		File[] files = dir.listFiles();
		if (files.length == 0)
			return;

		Comparator<File> comparator = new Comparator<File>() {
			@Override
			public int compare(File f1, File f2) {
				return ((File) f1).getName().compareTo(((File) f2).getName());
			}
		};

		Arrays.sort(files, comparator);
		File lastfile = files[files.length - 1];
		Log.info("Delete file " + lastfile.getName());
		lastfile.delete();
	}

	public static void main(String[] args) {
		MyDB db = null;

		try {
			db = new MyDB();
			removeLatestFile(Environment.DailyTradeStocksPath);
			DailyTradeStocks.supplementDB(db);

			removeLatestFile(Environment.DailyTaiExPath);
			DailyTaiEx.supplementDB(db);

			db.close();
			Log.info("Done!!");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
