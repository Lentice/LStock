package lstockv2;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Scanner;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class QuarterTableVerify {
	private static final Logger log = LogManager.getLogger(QuarterTableVerify.class.getName());

	private static final int MAX_DOWNLOAD_RETRY = 20;
	private static final int MAX_THREAD = 20;
	private static final long MIN_DOWNLOAD_GAP = 1000;
	private static final Downloader downloader = new Downloader(MIN_DOWNLOAD_GAP);

	private enum QuarterTableType {
		INCOME_STATEMENT, BALANCE_SHEET
	}

	private int year;
	private int quarter;
	private QuarterTableType tableType;
	private String folderPath;
	private ArrayList<HashMap<String, String>> companyData;
	private HashMap<String, String> data;
	private MyStatement stm;

	QuarterTableVerify(final int year, final int quarter, final QuarterTableType tableType, final MyStatement stm) {
		this.year = year;
		this.quarter = quarter;
		this.tableType = tableType;
		this.stm = stm;

		if (tableType == QuarterTableType.INCOME_STATEMENT)
			folderPath = DataPath.QUARTERLY_INCOME_COLLECTION;
		else
			folderPath = DataPath.QUARTERLY_BALANCE_COLLECTION;
	}

	private String getData(String... names) {
		String value = null;
		for (String name : names) {
			if (data.containsKey(name)) {
				value = data.get(name);
				if (value.length() > 0) {
					return value;
				} else
					value = null;
			}
		}

		return value;
	}

	private Integer parseInt(String... names) {
		String value = getData(names);
		try {
			return Integer.parseInt(value);
		} catch (NumberFormatException | NullPointerException e) {
			return null;
		}
	}

	private Long parseLong(String... names) {
		String value = getData(names);
		try {
			return Long.parseLong(value);
		} catch (NumberFormatException | NullPointerException e) {
			return null;
		}
	}

	private Float parseFloat(String... names) {
		String value = getData(names);
		try {
			return Float.parseFloat(value);
		} catch (NumberFormatException | NullPointerException e) {
			return null;
		}
	}

	private String getURL(final boolean idv) {
		final int twYear = year - 1911;
		String formAction;

		if (tableType == QuarterTableType.INCOME_STATEMENT) {
			if (idv) {
				formAction = "/mops/web/ajax_t51sb08";
			} else {
				if (year < 2013)
					formAction = "/mops/web/ajax_t51sb13";
				else
					formAction = "/mops/web/ajax_t163sb04";
			}
		} else {
			if (idv) {
				formAction = "/mops/web/ajax_t51sb07";
			} else {
				if (year < 2013)
					formAction = "/mops/web/ajax_t51sb12";
				else
					formAction = "/mops/web/ajax_t163sb05";
			}
		}

		String postData = String.format("encodeURIComponent=1&step=1&firstin=1&off=1&TYPEK=sii&year=%d&season=%02d",
				twYear, quarter);
		return "http://mops.twse.com.tw" + formAction + "?" + postData;
	}

	private String getFilename(final boolean idv) {
		if (idv)
			return String.format("%04d_%d_idv.html", year, quarter);
		else
			return String.format("%04d_%d.html", year, quarter);
	}

	private File getFile(final boolean idv) {
		return new File(folderPath + getFilename(idv));
	}

	private boolean downloadNeedRetry(File file) {

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

	private boolean downloadWithRetry(final boolean idv) {
		String url = getURL(idv);

		String fullFilePath = folderPath + getFilename(idv);
		File file = new File(fullFilePath);
		if (file.isFile()) {
			log.debug("Quarterly file already exist: " + fullFilePath);
			return true;
		}

		boolean result = false;
		try {
			for (int iRetry = 0; iRetry <= MAX_DOWNLOAD_RETRY; iRetry++) {
				log.info("Download retry[" + iRetry + "]: " + fullFilePath);
				if (!downloader.httpGet(url, fullFilePath)) {
					continue;
				}

				file = new File(fullFilePath);
				if (downloadNeedRetry(file)) {
					// sleep 10sec ~ 18sec
					Thread.sleep((int) (Math.random() * 8000 + 10000));
					continue;
				}

				result = true;
				break;
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		return result;
	}

	public boolean download() {

		boolean result = true;

		if (!downloadWithRetry(false))
			result = false;

		if (year < 2013) {
			if (!downloadWithRetry(true))
				result = false;
		}

		return result;
	}

	private static int[] getCurrentQuarter() {
		int[] yearQuarter = new int[2];

		Calendar endCal = Calendar.getInstance();
		int month = endCal.get(Calendar.MONTH) + 1;
		int day = endCal.get(Calendar.DATE);
		int year = endCal.get(Calendar.YEAR);
		int quarter = 0;

		if (month > 11 || (month == 11 && day > 15)) {
			quarter = 3;
		} else if (month > 8 || (month == 8 && day > 15)) {
			quarter = 2;
		} else if (month > 5 || (month == 5 && day > 15)) {
			quarter = 1;
		} else if (month > 3 || (month == 3 && day > 11)) {
			quarter = 4;
			year -= 1;
		} else {
			quarter = 3;
			year -= 1;
		}

		yearQuarter[0] = year;
		yearQuarter[1] = quarter;
		return yearQuarter;
	}

	private HashMap<String, String> getCompanyData(final String stockNum) {
		for (int i = 0; i < companyData.size(); i++) {
			HashMap<String, String> map = companyData.get(i);
			String stockNumInData = map.get("stockNum");
			if (stockNum == stockNumInData)
				return map;
		}
		return new HashMap<String, String>();
	}

	private void parseHtml(final boolean idv) throws IOException {
		File file = getFile(idv);
		if (!file.isFile()) {
			return;
		}

		log.info("Parse " + file.getPath());
		Document doc = Jsoup.parse(file, "UTF-8");
		Elements checkHeader = doc.getElementsContainingOwnText("公司名稱");

		if (checkHeader == null || checkHeader.size() == 0) {
			// throw new UnsupportedOperationException(file.getPath());
			return;
		}

		Element eTbody_old = null;
		String[] key = null;
		for (int iTable = 0; iTable < checkHeader.size(); iTable++) {

			// Get Rows
			Element eTemp = checkHeader.get(iTable).parent();
			while (!eTemp.tagName().equals("tbody")) {
				eTemp = eTemp.parent();
			}

			if (eTbody_old == eTemp)
				continue;
			else
				eTbody_old = eTemp;
			
			Elements rows = eTemp.select(":root > tr");
			for (int i = 0; i < rows.size(); i++) {
				boolean spanNextRow = false;
				
				Element row = rows.get(i);
				//log.info(String.format("row[%d] %s", i, row));
				if (row.child(0).tagName().equals("th")) {

					Elements headers = row.select(":root > th");
					key = new String[headers.size()+10];
					int nextRowIndex = 0;
					for (int j = 1; j < headers.size(); ) {
						Element header = headers.get(j);
						if (header.hasAttr("colspan")) {
							Element nextRow = rows.get(i+1);
							Elements nextRowHeaders = nextRow.select(":root > th");
							String currentData = header.text().replaceAll("（|）|\\(|\\)", "").replace("－", "-");
							for (int k = 0; k < Integer.parseInt(header.attr("colspan")); k++) {
								key[j++] = currentData + nextRowHeaders.get(nextRowIndex++).text().replaceAll("（|）|\\(|\\)", "").replace("－", "-");
							}
							spanNextRow = true;
						} else {
							key[j++] = header.text().replaceAll("（|）|\\(|\\)", "").replace("－", "-");
						}
					}
					key[0] = "stockNum";
				} else {

					Elements columns = row.select(":root > td");
					if (columns.isEmpty())
						continue;

					HashMap<String, String> map = getCompanyData(HtmlUtil.getText(columns.get(0)));
					for (int j = 0; j < columns.size(); j++) {
						String value = HtmlUtil.getText(columns.get(j));
						if (value == null || value.equals("--"))
							continue;

						if (!map.containsKey(key[j]))
							map.put(key[j], value);
					}
					companyData.add(map);
				}
				if (spanNextRow)
					i++;
			}
		}
	}

	private Long 營收() {
		Long retValue = null;
		retValue = parseLong("營業收入", "營業收入 淨額", "收入");

		return retValue;
	}

	private Long 成本() {
		Long retValue = null;
		retValue = parseLong("營業成本");

		return retValue;
	}

	private Long 業外收支() {
		Long retValue = null;
		retValue = parseLong("營業外收入及支出", "業外收入及利益");
		if (retValue == null) {
			Long 營業外收入 = parseLong("營業外收入", "營業外收入及利益");
			Long 營業外支出 = parseLong("營業外支出", "營業外費用及損失");

			if (營業外收入 == null && 營業外支出 == null)
				retValue = null;
			else
				retValue = ((營業外收入 == null) ? 0 : 營業外收入) + ((營業外支出 == null) ? 0 : 營業外支出);
		}

		return retValue;
	}
	
	private Long 綜合損益() {
		Long retValue = null;
		retValue = parseLong("本期綜合損益總額", "合併總損益", "本期淨利淨損", "本期損益");
		if (retValue == null && year < 2006)
			retValue = parseLong("稅後純益");
			

		return retValue;
	}

	private void importIncome() throws SQLException {

		synchronized (stm.AcquireLock()) {
			if (parseInt("stockNum") == 3990) //測試帳號
				return;
			
			Long 營收 = 營收();
			Long 成本 = 成本();
			stm.setObject(Integer.valueOf(year * 100 + quarter)); // YearQuarter
			stm.setObject(parseInt("stockNum")); // StockNum
			stm.setObject(營收); // 營收
			stm.setObject(成本); // 成本
			stm.setObject(parseLong("營業毛利毛損", "營業毛利")); // 毛利
			stm.setObject(parseLong("營業利益損失", "營業利益", "營業淨利淨損")); // 營業利益
			stm.setObject(業外收支()); // 業外收支
			stm.setObject(parseLong("稅前淨利淨損", "稅前純益", "繼續營業單位稅前合併淨利淨損", "繼續營業單位稅前淨利淨損", "繼續營業單位稅前淨利")); // 稅前淨利
			stm.setObject(parseLong("繼續營業單位本期淨利淨損", "稅後純益", "繼續營業單位稅後合併淨利淨損", "繼續營業單位淨利淨損", "繼續營業單位稅後淨利", "繼續營業單位稅後淨利淨損")); // 稅後淨利
			stm.setObject(綜合損益()); // 綜合損益
			stm.setObject(parseLong("淨利淨損歸屬於母公司業主")); // 母公司業主淨利
			stm.setObject(parseLong("綜合損益總額歸屬於母公司業主", "合併總損益歸屬予母公司股東")); // 母公司業主綜合損益
			stm.setObject(parseFloat("基本每股盈餘元", "每股稅後盈餘元", "每股盈餘", "每股稅後盈餘", "基本每股盈餘")); // EPS
			stm.setObject((quarter > 1) ? Boolean.TRUE : Boolean.FALSE); // 損益需更正
			stm.addBatch();
		}
	}

	private void importBalance(final boolean annual) throws SQLException {
		synchronized (stm.AcquireLock()) {
			if (parseInt("stockNum") == 3990) //測試帳號
				return;
			
			stm.setObject(Integer.valueOf(year * 100 + quarter)); // YearQuarter
			stm.setObject(parseInt("stockNum")); // StockNum
			stm.setObject(parseLong("流動資產")); // 流動資產
			stm.setObject(parseLong("非流動資產")); // 非流動資產
			stm.setObject(parseLong("資產總額")); // 總資產
			stm.setObject(parseLong("流動負債")); // 流動負債
			stm.setObject(parseLong("非流動負債")); // 非流動負債
			stm.setObject(parseLong("負債總額")); // 總負債
			stm.setObject(parseLong("保留盈餘")); // 保留盈餘
			stm.setObject(parseLong("股本")); // 股本
			stm.addBatch();
		}
	}

	public void importData() {
		try {
			companyData = new ArrayList<>();

			parseHtml(false);
			parseHtml(true); // parse idv data last.

			if (companyData.size() == 0)
				return;

			for (int i = 0; i < companyData.size(); i++) {
				data = companyData.get(i);
				if (tableType == QuarterTableType.INCOME_STATEMENT) {
					importIncome(); // import quarterly
				} else if (tableType == QuarterTableType.BALANCE_SHEET) {
					importBalance(false); // import quarterly
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}

	private void verify() throws Exception {
		MyDB db = new MyDB();
		parseHtml(false);
		if (year < 2013)
			parseHtml(true);

		Statement stOriginal = db.conn.createStatement();
		String query = "SELECT SUM(營收),SUM(成本),SUM(毛利),SUM(營業利益),SUM(業外收支),SUM(稅前淨利),SUM(稅後淨利),SUM(綜合損益),SUM(母公司業主淨利),SUM(母公司業主綜合損益),SUM(EPS)"
				+ " FROM quarterly WHERE "
				+ String.format("YearQuarter BETWEEN %d00 AND %d%d", year, year, quarter + 1);
		ResultSet rs = stOriginal.executeQuery(query);
		db.close();
	}

	public static void start() throws Exception {
		MyDB db = new MyDB();
		new File(DataPath.QUARTERLY_INCOME_COLLECTION).mkdirs();
		new File(DataPath.QUARTERLY_BALANCE_COLLECTION).mkdirs();

		int[] currentQuarter = getCurrentQuarter();
		int endYear = currentQuarter[0];
		int endQuarter = currentQuarter[1];

		int year = 2004;
		int quarter = 1;

		MyStatement stQuarterlyIncome = new MyStatement(db.conn);
		MyStatement stQuarterlyBalance = new MyStatement(db.conn);

		stQuarterlyIncome.setStatementInsertAndUpdate("quarterly_compare", "YearQuarter", "StockNum", "營收", "成本", "毛利",
				"營業利益", "業外收支", "稅前淨利", "稅後淨利", "綜合損益", "母公司業主淨利", "母公司業主綜合損益", "EPS", "損益需更正");

		stQuarterlyBalance.setStatementInsertAndUpdate("quarterly_compare", "YearQuarter", "StockNum", "流動資產", "非流動資產",
				"總資產", "流動負債", "非流動負債", "總負債", "保留盈餘", "股本");

		while (year < endYear || (year == endYear && quarter <= endQuarter)) {
			log.info(year + " " + quarter);
			QuarterTableVerify income = new QuarterTableVerify(year, quarter, QuarterTableType.INCOME_STATEMENT,
					stQuarterlyIncome);
			QuarterTableVerify balance = new QuarterTableVerify(year, quarter, QuarterTableType.BALANCE_SHEET,
					stQuarterlyBalance);

			income.download();
			balance.download();

			income.importData();
			balance.importData();
			// income.verify();
			if (++quarter > 4) { // Next quarter
				quarter = 1;
				year++;
			}
		}

		stQuarterlyIncome.close();
		stQuarterlyBalance.close();

		db.close();
	}

	public static void main(String[] args) throws Exception {
		start();
		log.info("Done !!");
	}

}
