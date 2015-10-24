import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.util.Arrays;

public class Importer {

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

	protected static String parseChtDate(String chtDate) {
		String year, month, day;
		int iy, im, id;

		Log.verbose(chtDate);
		iy = chtDate.indexOf("年");
		im = chtDate.indexOf("月");
		id = chtDate.indexOf("日");
		year = chtDate.substring(0, iy);
		month = chtDate.substring(iy + 1, im);
		day = chtDate.substring(im + 1, id);

		return String.format(year + "-" + month + "-" + day);
	}

	protected static String[] parseCsvLine(String line) {
		line = line.replaceAll("--", "00");
		if (line.charAt(0) == '=') 
			line = line.substring(2, line.length() - 1);
		else
			line = line.substring(1, line.length() - 1);
		
		String[] rowData = line.split("\",\"");
		for (int i = 0; i < rowData.length; i++) {
			rowData[i] = rowData[i].trim();
		}
		Log.verbose(Arrays.toString(rowData));
		return rowData;
	}

	public static int importDailyTradeStocks(File file) throws Exception {
		BufferedReader fileBR = null;
		try {
			fileBR = new BufferedReader(new InputStreamReader(new FileInputStream(file), "MS950"));
		} catch(FileNotFoundException ex) {
			Log.err("File " + file + " was not found.");
			return -1;
		} catch(Exception ex) {
			ex.printStackTrace();
		}
		
		MyDB dailyDB = new MyDB();
		String dailyQuery = "INSERT IGNORE INTO daily " 
				+ "(Date, StockNum, 成交股數, 成交筆數, 成交金額, 開盤價, 最高價, 最低價, 收盤價, 本益比) values"
				+ "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
		
		String companyQuery = "INSERT INTO company " 
				+ "(StockNum, Code, Name, last_update) values"
				+ "(?, ?, ?, ?)" + 
				"ON DUPLICATE KEY UPDATE last_update=IF(last_update < VALUES(last_update), VALUES(last_update), last_update)";
		
		PreparedStatement dailyST = dailyDB.conn.prepareStatement(dailyQuery);
		PreparedStatement companyST = dailyDB.conn.prepareStatement(companyQuery);

		String line;
		String date = null;
		boolean startParsing = false;

		try {
			while ((line = fileBR.readLine()) != null) {
				if (!startParsing) {
					if (line.contains("查無資料")) {
						Log.info("No data on that day. file: " + file.getName());
						return -1;
					}
						

					if (date == null && line.contains("大盤統計資訊")) {
						date = parseChtDate(line.replace("大盤統計資訊", "").trim());
						Log.trace("Get date: " + date);
					}
					if (line.contains("證券名稱")) {
						startParsing = true;
						continue;
					} else
						continue;
				} else {
					if (line.indexOf(',') == -1)
						break;
				}
				

				String[] rowData = parseCsvLine(line);

				if (!isStringInt(rowData[0]))
					continue;
				
				companyST.setInt(1, Integer.parseInt(rowData[0])); // StockNum
				companyST.setString(2, rowData[0]); // Code
				companyST.setString(3, rowData[1]); // Name
				companyST.setDate(4, Date.valueOf(date)); // last_update
				companyST.addBatch();

				dailyST.setDate(1, Date.valueOf(date)); // Date
				dailyST.setInt(2, Integer.parseInt(rowData[0])); // StockNum
				dailyST.setLong(3, Long.parseLong(rowData[2])); // 成交股數
				dailyST.setLong(4, Long.parseLong(rowData[3])); // 成交筆數
				dailyST.setLong(5, Long.parseLong(rowData[4])); // 成交金額
				dailyST.setFloat(6, Float.parseFloat(rowData[5])); // 開盤價
				dailyST.setFloat(7, Float.parseFloat(rowData[6])); // 最高價
				dailyST.setFloat(8, Float.parseFloat(rowData[7])); // 最低價
				dailyST.setFloat(9, Float.parseFloat(rowData[8])); // 收盤價
				dailyST.setFloat(10, Float.parseFloat(rowData[15])); // 本益比
				dailyST.addBatch();
				
				companyST.executeBatch();
				dailyST.executeBatch();
			}
		} finally {
			fileBR.close();
		}

		return 0;
	}

	public static void main(String[] args) {

		try {
			importDailyTradeStocks(new File("Data\\每日收盤行情\\20040212.csv"));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
