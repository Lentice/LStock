import java.io.File;
import java.io.IOException;
import java.sql.Date;
import java.util.Calendar;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

class MonthlyRevenueParser {
	public static final int MonthlyRevenueMaxColume = 11;

	private int idxCategory = 0;
	private int numCategory = 0;
	private int idxRow = 0;
	private int numRow = 0;
	private Elements eAllCategories;
	private Elements eAllRows;

	MonthlyRevenueParser(File file) throws IOException {
		Document doc = Jsoup.parse(file, "MS950");

		eAllCategories = doc.select(":root > body > center > center > table:nth-child(10) > tbody > tr > td > table");
		numCategory = eAllCategories.size();
		idxCategory = 0;
	}

	String getNextCategory() {
		if (idxCategory >= numCategory)
			return null;

		Element eCategory = eAllCategories.get(idxCategory++);
		Element eCategoryName = eCategory.select(":root > tbody:nth-child(1) > tr:nth-child(1) > th:nth-child(1)")
				.first();
		eAllRows = eCategory.select(":root > tbody > tr:nth-child(2) > td > table > tbody > tr:nth-child(n+3)");
		numRow = eAllRows.size();
		if (numRow == 0) {
			eAllRows = eCategory.select(":root > tbody > tr:nth-child(3) > td > table > tbody > tr:nth-child(n+3)");
			numRow = eAllRows.size();
		}
		idxRow = 0;

		String name = eCategoryName.text().replaceFirst("產業別：", "");

		// 去除刮號附註
		int noteIdx = name.indexOf("（");
		if (noteIdx >= 0)
			name = name.substring(0, noteIdx);

		return name;
	}

	String[] getNextData() {
		if (idxRow >= numRow - 1)
			return null;

		Elements eRow = eAllRows.get(idxRow++).select(":root > td");
		String[] data = new String[MonthlyRevenueMaxColume];
		for (int i = 0; i < MonthlyRevenueMaxColume; i++) {
			data[i] = eRow.get(i).text().replaceAll(",", "");

			if (data[i].length() == 1) {
				char firstChar = data[i].charAt(0);
				if (Character.isSpaceChar(firstChar))
					data[i] = null;
				else if (i == 10 && Character.compare(data[i].charAt(0), '-') == 0)
					data[i] = null;
				else if (i == 10 && Character.compare(data[i].charAt(0), '無') == 0)
					data[i] = null;

			} else if (data[i].length() == 3 && data[i].compareTo("不適用") == 0)
				data[i] = null;

			Log.trace_(data[i] + ", ");
		}
		Log.trace_("\n");

		return data;
	}
}

public class ImportMonthly {

	protected static int importRevenue(int year, int month, MyDB db, int foreign) throws Exception {
		String category;
		String[] data;

		String filename = String.format("%04d%02d_sii_%d.html", year, month, foreign);
		File file = new File(Environment.MonthlyRevenue + filename);
		if (!file.exists()) {
			Log.warn("檔案不存在: " + filename);
			return -1;
		}

		Date date = Date.valueOf(String.format("%d-%d-01", year, month));
		MonthlyRevenueParser revenueParser = new MonthlyRevenueParser(file);

		String update = "UPDATE company SET 產業別 = ? WHERE StockNum = ?";
		MyStatement companyST = new MyStatement(db.conn, update);

		String insert = "INSERT IGNORE INTO monthly "
				+ "(Date, StockNum, 當月營收, 上月營收, 去年當月營收, 上月比較增減, 去年同月增減, 當月累計營收, 去年累計營收, 前期比較增減, 備註) VALUES "
				+ "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
		MyStatement dailyST = new MyStatement(db.conn, insert);

		while ((category = revenueParser.getNextCategory()) != null) {
			Log.trace(category);
			while ((data = revenueParser.getNextData()) != null) {

				companyST.setChar(1, category);
				companyST.setInt(2, data[0]); // StockNum
				companyST.addBatch();

				dailyST.setDate(1, date); // Date
				dailyST.setInt(2, data[0]); // StockNum
				dailyST.setBigInt(3, data[2]); // 當月營收
				dailyST.setBigInt(4, data[3]); // 上月營收
				dailyST.setBigInt(5, data[4]); // 去年當月營收
				dailyST.setFloat(6, data[5]); // 上月比較增減
				dailyST.setFloat(7, data[6]); // 去年同月增減
				dailyST.setBigInt(8, data[7]); // 當月累計營收
				dailyST.setBigInt(9, data[8]); // 去年累計營收
				dailyST.setFloat(10, data[9]); // 前期比較增減
				dailyST.setBlob(11, data[10]); // 備註
				dailyST.addBatch();
			}
		}
		companyST.close();
		dailyST.close();

		return 0;
	}

	public static void supplementRevenue() throws Exception {
		File importDir = new File(Environment.MonthlyRevenue);
		if (!importDir.exists()) {
			Log.warn("路徑不存在: " + importDir);
			System.exit(-1);
		}

		MyDB db = new MyDB();

		Calendar startCal = db.getLastRevenue();
		Calendar endCal = Calendar.getInstance();

		while (startCal.compareTo(endCal) <= 0) {
			int year = startCal.get(Calendar.YEAR);
			int month = startCal.get(Calendar.MONTH) + 1;

			Log.info(String.format("Process %04d%02d", year, month));
			importRevenue(year, month, db, 0);
			importRevenue(year, month, db, 1);

			startCal.add(Calendar.MONTH, 1);
		}

		db.close();
	}

	public static void main(String[] args) {
		try {
			supplementRevenue();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}