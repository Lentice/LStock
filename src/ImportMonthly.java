import java.io.File;
import java.io.IOException;
import java.util.Calendar;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

class MonthlyRevenue {
	private static final String folderPath = Environment.MonthlyRevenuePath;
	private static final int MonthlyRevenueMaxColume = 11;
	
	File file;
	int year;
	int month;
	
	boolean noData = true;;
	private int formType;
	private int idxCategory;
	private int numCategory;
	private int idxRow;
	private int numRow;
	private Elements eAllCategories;
	private Elements eAllRows;

	MonthlyRevenue(int year, int month, int foreign) throws Exception {
		this.year = year;
		this.month = month;

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

	public boolean parse() throws IOException {
		if (!file.exists()) {
			Log.warn("檔案不存在: " + file.getName());
			return false;
		}

		Document doc = Jsoup.parse(file, "MS950");

		if (year < 2012)
			formType = 0;
		else if (year < 2013)
			formType = 1;
		else
			formType = 2;

		if (formType == 2)
			eAllCategories = doc
			        .select(":root > body > center > center > table:nth-child(10) > tbody > tr > td > table");
		else if (formType <= 1)
			eAllCategories = doc.select(":root > body > center > table:nth-child(n+3)");
		numCategory = eAllCategories.size();

		if (numCategory == 0)
			return false;

		idxCategory = 0;
		idxRow = 0;
		numRow = 0;
		noData = false;

		return true;
	}

	public String getNextCategory() throws Exception {
		if (noData)
			return null;
		
		Element eCategoryName;
		if (idxCategory >= numCategory)
			return null;

		Element eCategory = eAllCategories.get(idxCategory++);
		if (formType == 0) {
			eCategoryName = eCategory.select(":root > tbody > tr:nth-child(2) > th:nth-child(1)").first();
			eAllRows = eCategory.select(":root > tbody > tr:nth-child(3) > td > table > tbody > tr:nth-child(n+3)");

			if (eCategoryName == null) {
				eCategoryName = eCategory.select(":root > tbody > tr:nth-child(1) > th:nth-child(1)").first();
				eAllRows = eCategory.select(":root > tbody > tr:nth-child(2) > td > table > tbody > tr:nth-child(n+3)");
			}
		} else if (formType == 1) {

			eCategoryName = eCategory.select(":root > tbody > tr > td > table > tbody > tr > th:nth-child(1)").first();
			eAllRows = eCategory.select(
			        ":root > tbody > tr > td > table:nth-child(2) > tbody > tr:nth-child(2) > td > table > tbody >  tr:nth-child(n+3)");
		} else if (formType == 2) {
			eCategoryName = eCategory.select(":root > tbody:nth-child(1) > tr:nth-child(1) > th:nth-child(1)").first();
			eAllRows = eCategory.select(":root > tbody > tr:nth-child(2) > td > table > tbody > tr:nth-child(n+3)");
			if (eAllRows.size() == 0) {
				eAllRows = eCategory.select(":root > tbody > tr:nth-child(3) > td > table > tbody > tr:nth-child(n+3)");
			}
		} else {
			throw new Exception("Invalid formType " + formType);
		}

		numRow = eAllRows.size();
		idxRow = 0;

		String name = eCategoryName.text().replaceFirst("產業別：", "");

		// 去除刮號附註
		int noteIdx = name.indexOf("（");
		if (noteIdx >= 0)
			name = name.substring(0, noteIdx);

		return name;
	}

	public String[] getNextData() {
		if (noData)
			return null;
		
		if (idxRow >= numRow - 1)
			return null;

		Elements eRow = eAllRows.get(idxRow++).select(":root > td");
		String[] data = new String[MonthlyRevenueMaxColume];
		for (int i = 0; i < eRow.size(); i++) {
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

			Log.verbose_(data[i] + ", ");
		}
		Log.verbose_("\n");

		return data;
	}

	public static int download(int year, int month, int foreign) throws Exception {
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

	/**
	 * 下載從指定年月到現在為止的檔案
	 * 
	 * @param year 指定年
	 * @param month 指定月 (1-base)
	 * @throws Exception
	 */
	public static void supplement(int year, int month) throws Exception {
		Calendar endCal = Calendar.getInstance();
		int eYear = endCal.get(Calendar.YEAR);
		int eMonth = endCal.get(Calendar.MONTH) + 1;

		Downloader.createFolder(folderPath);

		while (year < eYear || (year == eYear && month <= eMonth)) {
			if (download(year, month, 0) != 0)
				break;

			if (year >= 2013)
				if (download(year, month, 1) != 0)
					break;

			if (++month > 12) {
				year++;
				month = 1;
			}
		}
	}
}

public class ImportMonthly {

	protected static int importRevenue(int year, int month, MyDB db, int foreign) throws Exception {
		String category;
		String[] data;

		MonthlyRevenue revenue = new MonthlyRevenue(year, month, foreign);
		if (!revenue.parse())
			return -1;

		String update = "UPDATE company SET 產業別 = ? WHERE StockNum = ?";
		MyStatement companyST = new MyStatement(db.conn, update);

		MyStatement monthST = new MyStatement(db.conn);
		monthST.setInsertIgnoreStatement("monthly", "YearMonth", "StockNum", "當月營收", "上月營收", "去年當月營收", "上月比較增減",
		        "去年同月增減", "當月累計營收", "去年累計營收", "前期比較增減", "備註");

		while ((category = revenue.getNextCategory()) != null) {
			Log.dbg(category);
			while ((data = revenue.getNextData()) != null) {

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

		return 0;
	}

	public static void supplementRevenue(int year, int month) throws Exception {
		Calendar endCal = Calendar.getInstance();
		int eYear = endCal.get(Calendar.YEAR);
		int eMonth = endCal.get(Calendar.MONTH) + 1;

		File importDir = new File(Environment.MonthlyRevenuePath);
		if (!importDir.exists()) {
			Log.warn("路徑不存在: " + importDir);
			System.exit(-1);
		}

		MyDB db = new MyDB();

		while (year < eYear || (year == eYear && month <= eMonth)) {

			Log.info(String.format("Process %04d%02d", year, month));

			importRevenue(year, month, db, 0);
			if (year >= 2013)
				importRevenue(year, month, db, 1);

			if (++month > 12) {
				year++;
				month = 1;
			}
		}

		db.close();
	}

	public static void main(String[] args) {
		try {
			MyDB db = new MyDB();
			int yearMonth = db.getLastMonthlyRevenue();
			db.close();
			// MonthlyRevenue.supplement(yearMonth / 100, yearMonth % 100);
			supplementRevenue(yearMonth / 100, yearMonth % 100);
			// supplementRevenue(2004, 1);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}