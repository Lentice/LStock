import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Date;
import java.util.Arrays;
import java.util.Calendar;

class DailyTradeParser {
	boolean noData;
	Date date;
	BufferedReader fileBR;

	DailyTradeParser(File file) throws Exception {
		String line;
		date = null;
		noData = true;

		fileBR = new BufferedReader(new InputStreamReader(new FileInputStream(file), "MS950"));
		while ((line = fileBR.readLine()) != null) {
			if (line.contains("查無資料")) {
				Log.dbg("查無資料");
				return;
			}

			if (date == null && line.contains("大盤統計資訊")) {
				String sDate = parseChtDate(line.replace("大盤統計資訊", "").trim());
				date = Date.valueOf(sDate);
				Log.verbose("Get date: " + sDate);
			}

			if (line.contains("證券名稱")) {
				noData = false;
				break;
			} else
				continue;
		}
	}

	protected static boolean isStringInt(String s) {
		try {
			Integer.parseInt(s);
			return true;
		} catch (NumberFormatException ex) {
			return false;
		}
	}

	protected static boolean isStringFloat(String s) {
		try {
			Float.parseFloat(s);
			return true;
		} catch (NumberFormatException ex) {
			return false;
		}
	}

	protected String parseChtDate(String chtDate) {
		String year, month, day;
		int iy, im, id;

		iy = chtDate.indexOf("年");
		im = chtDate.indexOf("月");
		id = chtDate.indexOf("日");
		year = chtDate.substring(0, iy);
		month = chtDate.substring(iy + 1, im);
		day = chtDate.substring(im + 1, id);

		return String.format(year + "-" + month + "-" + day);
	}

	protected String[] parseDailyCsvLine(String line) {

		// 去頭 「="」 或 「"」 去尾「"」
		if (line.charAt(0) == '=')
			line = line.substring(2, line.length() - 1);
		else
			line = line.substring(1, line.length() - 1);

		// 把 「","」當作分隔符號
		String[] rowData = line.split("\",\"");
		for (int i = 0; i < rowData.length; i++) {
			rowData[i] = rowData[i].trim();

			// 未交易 沒有數值
			if (rowData[i].length() == 2 && rowData[i].compareTo("--") == 0)
				rowData[i] = null;
		}

		Log.verbose(Arrays.toString(rowData));
		return rowData;
	}

	public Date getDate() {
		return date;
	}

	public String[] getNextData() throws IOException {
		String line;
		while ((line = fileBR.readLine()) != null) {
			if (line.indexOf(',') == -1)
				return null;

			String[] data = parseDailyCsvLine(line);
			if (!isStringInt(data[0]))
				continue;

			return data;
		}
		return null;
	}

	public void close() throws IOException {
		fileBR.close();
	}
}

public class ImportDaily {

	public static void importDailyTradeStocks(File file, MyDB dailyDB) throws Exception {
		DailyTradeParser tradeParser = new DailyTradeParser(file);
		if (tradeParser.noData)
			return;

		String insertComp = "INSERT INTO company " + "(StockNum, Code, Name, last_update) VALUES" + "(?, ?, ?, ?)"
		        + "ON DUPLICATE KEY UPDATE last_update = IF(last_update < VALUES(last_update), VALUES(last_update), last_update)";
		MyStatement companyST = new MyStatement(dailyDB.conn, insertComp);

		String insertDailyTrade = "INSERT IGNORE INTO daily "
		        + "(Date, StockNum, 成交股數, 成交筆數, 成交金額, 開盤價, 最高價, 最低價, 收盤價, 本益比) VALUES "
		        + "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
		MyStatement dailyST = new MyStatement(dailyDB.conn, insertDailyTrade);

		String[] data;
		while ((data = tradeParser.getNextData()) != null) {

			companyST.setInt(1, Integer.parseInt(data[0])); // StockNum
			companyST.setChar(2, data[0]); // Code
			companyST.setChar(3, data[1]); // Name
			companyST.setDate(4, tradeParser.getDate()); // last_update
			companyST.addBatch();

			dailyST.setDate(1, tradeParser.getDate()); // Date
			dailyST.setInt(2, data[0]); // StockNum
			dailyST.setBigInt(3, data[2]); // 成交股數
			dailyST.setBigInt(4, data[3]); // 成交筆數
			dailyST.setBigInt(5, data[4]); // 成交金額
			dailyST.setFloat(6, data[5]); // 開盤價
			dailyST.setFloat(7, data[6]); // 最高價
			dailyST.setFloat(8, data[7]); // 最低價
			dailyST.setFloat(9, data[8]); // 收盤價
			dailyST.setFloat(10, data[15]); // 本益比
			dailyST.addBatch();
		}
		companyST.close();
		dailyST.close();
		tradeParser.close();
	}

	public static void supplementDailyTradeStocks() throws Exception {

		File importDir = new File(Environment.DailyTradeStocksPath);
		if (!importDir.exists()) {
			Log.warn("路徑不存在: " + importDir);
			System.exit(-1);
		}

		MyDB db = new MyDB();

		Calendar startCal = db.getLastTradeDate();
		Calendar endCal = Calendar.getInstance();
		while (startCal.compareTo(endCal) <= 0) {
			String filename = String.format("%04d%02d%02d.csv", startCal.get(Calendar.YEAR),
			        startCal.get(Calendar.MONTH) + 1, startCal.get(Calendar.DATE));
			File importFile = new File(Environment.DailyTradeStocksPath + filename);
			if (importFile.exists()) {
				Log.info("Process: " + importFile.getName());
				importDailyTradeStocks(importFile, db);
			} else
				Log.warn("檔案不存在: " + filename);

			startCal.add(Calendar.DATE, 1);
		}

		db.close();
	}

	public static void main(String[] args) {

		try {
			supplementDailyTradeStocks();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
