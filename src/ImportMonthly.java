import java.io.File;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;


class MonthlyData {
	private static final String ALL_DATA = "SELECT * FROM monthly WHERE StockNum=%s ORDER BY YearMonth";

	Integer YearMonth;
	Integer StockNum;
	Long 當月營收;
	Long 上月營收;
	Long 去年當月營收;
	Float 上月比較增減;
	Float 去年同月增減;
	Long 當月累計營收;
	Long 去年累計營收;
	Float 前期比較增減;
	
	MonthlyData() {
	}

	MonthlyData(ResultSet rs) throws SQLException {
		int colume = 1;

		/* 以下數值若不存在 則當成0 以方便計算 */
		YearMonth = (Integer) rs.getObject(colume++, Integer.class);
		StockNum = (Integer) rs.getObject(colume++, Integer.class);
		當月營收 = rs.getObject(colume++, Long.class);
		上月營收 = rs.getObject(colume++, Long.class);
		去年當月營收 = rs.getObject(colume++, Long.class);
		上月比較增減 = rs.getObject(colume++, Float.class);
		去年同月增減 = rs.getObject(colume++, Float.class);
		當月累計營收 = rs.getObject(colume++, Long.class);
		去年累計營收 = rs.getObject(colume++, Long.class);
		前期比較增減 = rs.getObject(colume++, Float.class);
	}

	public static MonthlyData[] getAllData(MyDB db, int stockNum) throws SQLException {
		Statement stm = db.conn.createStatement();
		ResultSet rs = stm.executeQuery(String.format(ALL_DATA, stockNum));
		int numRow = getNumRow(rs);
		if (numRow == 0)
			return null;

		MonthlyData[] allData = new MonthlyData[numRow];
		int iRow = 0;
		while (rs.next()) {
			allData[iRow] = new MonthlyData(rs);
			iRow++;
		}

		stm.close();
		return allData;
	}

	static int getNumRow(ResultSet result) throws SQLException {
		result.last();
		int numRow = result.getRow();
		result.beforeFirst();
		return numRow;
	}
	
	static MonthlyData getData(MonthlyData[] allData, int year, int month) throws SQLException {
		for (MonthlyData data : allData) {
			if (data.YearMonth / 100 == year && data.YearMonth % 100 == month) {
				return data;
			}
		}
		return null;
	}
}

class MonthlyRevenue {
	private static final String folderPath = Environment.MonthlyRevenuePath;
	private static final int MonthlyRevenueMaxColume = 11;

	File file;
	int year;
	int month;
	int foreign;

	boolean noData = true;
	List<String> categoryList;
	List<String[][]> tableList;

	MonthlyRevenue(int year, int month, int foreign) throws Exception {
		this.year = year;
		this.month = month;
		this.foreign = foreign;

		String filename;
		if (year >= 2013)
			filename = String.format("%04d%02d_sii_%d.html", year, month, foreign);
		else
			filename = String.format("%04d%02d_sii.html", year, month);

		file = new File(folderPath + filename);
		if (!file.exists()) {
			download(year, month, foreign);
			file = new File(folderPath + filename);
		}
	}

	boolean parse() throws IOException {
		if (!file.exists()) {
			Log.warn("檔案不存在: " + file.getPath());
			return false;
		}

		Document doc = Jsoup.parse(file, "MS950");
		Elements eAllCategories = doc.getElementsContainingOwnText("產業別：");
		if (eAllCategories.size() == 0)
			return false;

		categoryList = new ArrayList<>();
		tableList = new ArrayList<>();
		for (int i = 0; i < eAllCategories.size(); i++) {
			String name = eAllCategories.get(i).text().replaceFirst("產業別：", "").trim();
			
			int remove = name.indexOf('（');
			if (remove != -1) {
				name = name.substring(0, remove);
			}
			categoryList.add(name);
		}

		Elements eTitles = doc.getElementsContainingOwnText("公司名稱");
		for (int iTable = 0; iTable < eAllCategories.size(); iTable++) {
			Elements eTableRows = eTitles.get(iTable).parent().parent().children();

			final int dropHead = 2;
			final int dropTail = 1;
			if (eTableRows.size() <= (dropHead + dropTail)) // 標題 2 + 合計 1 rows
				continue;

			String[][] rows = new String[eTableRows.size() - dropHead - dropTail][];
			for (int iRow = dropHead; iRow < eTableRows.size() - dropTail; iRow++) {
				Elements eRow = eTableRows.get(iRow).children();
				String[] data = new String[MonthlyRevenueMaxColume];
				for (int k = 0; k < eRow.size(); k++) {
					data[k] = HtmlParser.getText(eRow.get(k));
					if (data[k] != null) {
						if (data[k].length() == 1) {
							char firstChar = data[k].charAt(0);
							if (k == 10 && Character.compare(firstChar, '-') == 0)
								data[k] = null;
							else if (k == 10 && Character.compare(firstChar, '無') == 0)
								data[k] = null;

						} else if (data[k].length() == 3 && data[k].compareTo("不適用") == 0)
							data[k] = null;
					}
				}
				rows[iRow - dropHead] = data;
			}
			tableList.add(rows);
		}

		noData = false;

		return true;
	}

	boolean importToDB(MyDB db) throws Exception {
		if (!parse())
			return false;

		String category;

		String update = "UPDATE company SET 產業別 = ? WHERE StockNum = ?";
		MyStatement companyST = new MyStatement(db.conn, update);

		MyStatement monthST = new MyStatement(db.conn);
		monthST.setInsertIgnoreStatement("monthly", "YearMonth", "StockNum", "當月營收", "上月營收", "去年當月營收", "上月比較增減",
				"去年同月增減", "當月累計營收", "去年累計營收", "前期比較增減", "備註");

		for (int i = 0; i < tableList.size(); i++) { // use tableList.size because some category have no data.
			category = categoryList.get(i);
			String[][] table = tableList.get(i);
			Log.dbg(String.format("%04d_%02d %s", year, month, category));

			for (int k = 0; k < table.length; k++) {
				String[] data = table[k];

				int idx = 1;
				companyST.setChar(idx++, category);
				companyST.setInt(idx++, data[0]); // StockNum
				companyST.addBatch();

				idx = 1;
				monthST.setInt(idx++, year * 100 + month); // YearMonth
				monthST.setInt(idx++, data[0]); // StockNum
				monthST.setBigInt(idx++, data[2]); // 當月營收
				monthST.setBigInt(idx++, data[3]); // 上月營收
				monthST.setBigInt(idx++, data[4]); // 去年當月營收
				monthST.setFloat(idx++, data[5]); // 上月比較增減
				monthST.setFloat(idx++, data[6]); // 去年同月增減
				monthST.setBigInt(idx++, data[7]); // 當月累計營收
				monthST.setBigInt(idx++, data[8]); // 去年累計營收
				monthST.setFloat(idx++, data[9]); // 前期比較增減
				monthST.setBlob(idx++, data[10]); // 備註
				monthST.addBatch();
			}
		}
		companyST.close();
		monthST.close();
		
		return true;
	}

	int download(int year, int month, int foreign) throws Exception {
		int ret;
		String url;
		String filename;

		if (year < 2004)
			throw new Exception("Year is earlier than 2004");

		if (year >= 2013) {
			if (foreign == 0) {
				url = String.format("http://mops.twse.com.tw/nas/t21/sii/t21sc03_%d_%d_0.html", year - 1911, month);
				filename = String.format(folderPath + "%04d%02d_sii_0.html", year, month);
			} else {
				url = String.format("http://mops.twse.com.tw/nas/t21/sii/t21sc03_%d_%d_1.html", year - 1911, month);
				filename = String.format(folderPath + "%04d%02d_sii_1.html", year, month);
			}
		} else {
			url = String.format("http://mops.twse.com.tw/nas/t21/sii/t21sc03_%d_%d.html", year - 1911, month);
			filename = String.format(folderPath + "%04d%02d_sii.html", year, month);
		}

		Log.info("Download to " + filename);
		ret = Downloader.httpDownload(url, filename);
		if (ret < 0) {
			Log.info("Fail");
			return ret;
		}

		return 0;
	}

	public static void supplementDB(int year, int month) throws Exception {
		Calendar endCal = Calendar.getInstance();
		int eYear = endCal.get(Calendar.YEAR);
		int eMonth = endCal.get(Calendar.MONTH) + 1;

		File importDir = new File(Environment.MonthlyRevenuePath);
		if (!importDir.exists()) {
			Log.warn("路徑不存在: " + importDir);
			System.exit(-1);
		}

		MyDB db = new MyDB();

		MonthlyRevenue revenue;
		while (year < eYear || (year == eYear && month <= eMonth)) {

			Log.info(String.format("Process %04d_%02d", year, month));

			revenue = new MonthlyRevenue(year, month, 0);
			if (!revenue.importToDB(db))
				break;

			if (year >= 2013) {
				revenue = new MonthlyRevenue(year, month, 01);
				if (!revenue.importToDB(db))
					break;
			}

			if (++month > 12) {
				year++;
				month = 1;
			}
		}

		db.close();
	}
}

public class ImportMonthly {

	public static void main(String[] args) {
		try {
			MyDB db = new MyDB();
			int yearMonth = db.getLastMonthlyRevenue();
			db.close();
			MonthlyRevenue.supplementDB(yearMonth / 100, yearMonth % 100);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}