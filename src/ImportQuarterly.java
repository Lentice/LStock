import java.io.File;
import java.net.URLEncoder;
import java.util.Calendar;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

class BasicTable {
	private static final int INCOME_STATEMENT = 0;
	private static final int BALANCE_SHEET = 1;
	private static final int CASHFLOW_STATEMENT = 2;
	private static final int INCOME_STATEMENT_IDV = 3; // IDV: 個別財報
	private static final int BALANCE_SHEET_IDV = 4;
	private static final int CASHFLOW_STATEMENT_IDV = 5;

	private static boolean useIFRSs;
	private static int year;
	private static int quarter;
	private static String code;
	private static String category;
	private static int stockNum;
	private static String url;
	private static String filename;

	/**
	 * 檢查下載是否成功: 檔案太小代表失敗
	 * 
	 * @param filename
	 * @return true: 下載成功 ; false: 下載失敗或檔案不存在
	 */
	private static boolean isValidQuarterlyData(String filename, int minSize) throws Exception {
		File file = new File(filename);

		if (!file.exists())
			return false;

		if (file.length() > minSize)
			return true;

		Document doc = Jsoup.parse(file, "UTF-8");
		if (doc.toString().contains("查詢過於頻繁")) {
			file.delete();
			return false;
		}

		return true;
	}

	public static boolean isValidQuarter(int year, int quarter, String lastUpdate) throws Exception {
		int lastDate = Integer.parseInt(lastUpdate);
		int lastYear = lastDate / 10000;
		int lastMonth = lastDate / 100 % 100;
		int lastDay = lastDate % 100;

		int lastQuarter = 0;
		if (lastMonth > 11 || (lastMonth == 11 && lastDay > 15))
			lastQuarter = 3;
		else if (lastMonth > 8 || (lastMonth == 8 && lastDay > 15))
			lastQuarter = 2;
		else if (lastMonth > 5 || (lastMonth == 5 && lastDay > 15))
			lastQuarter = 1;

		int lastYearQuarter = lastYear * 100 + lastQuarter;
		int targetYearQuarter = year * 100 + quarter;

		if (lastYearQuarter >= targetYearQuarter) {
			return true;
		}

		return false;
	}

	private static int[] getCurrentQuarter() {
		Calendar endCal = Calendar.getInstance();
		int month = endCal.get(Calendar.MONTH) + 1;
		int day = endCal.get(Calendar.DATE);
		int year = endCal.get(Calendar.YEAR);
		int quarter = 0;

		if (month > 11 || (month == 11 && day > 15))
			quarter = 3;
		else if (month > 8 || (month == 8 && day > 15))
			quarter = 2;
		else if (month > 5 || (month == 5 && day > 15))
			quarter = 1;

		int[] quarterSet = new int[2];
		quarterSet[0] = year;
		quarterSet[1] = quarter;
		return quarterSet;
	}

	private static void doDownload(int type) throws Exception {

		getDownloadInfo(type);

		if (isValidQuarterlyData(filename, 1000)) {
			Log.info("Skip existing file " + filename);
			return;
		}

		final int MAX_RETRY = 20;
		int retry = 0;
		int dwResult = -1;
		do {
			Log.info("Download to " + filename);
			try {
				dwResult = Downloader.httpDownload(url, filename);
			} catch (Exception ex) {
				// None just retry
			}

			if (retry == 0)
				Thread.sleep(200);
			else
				Thread.sleep(10000);

			if (++retry > MAX_RETRY)
				break;
		} while (dwResult < 0 || !isValidQuarterlyData(filename, 1000));
	}

	private static void getDownloadInfo(int type) throws Exception {
		String path;
		String formAction;
		String postData;

		switch (type) {
		case BALANCE_SHEET:
			path = Environment.QUARTERLY_BALANCE_SHEET;
			filename = String.format(path + "%s_%04d_%d.html", code, year, quarter);
			if (useIFRSs)
				formAction = "/mops/web/ajax_t164sb03";
			else
				formAction = "/mops/web/ajax_t05st33";

			break;
		case INCOME_STATEMENT:
			path = Environment.QUARTERLY_INCOME_STATEMENT;
			filename = String.format(path + "%s_%04d_%d.html", code, year, quarter);
			if (useIFRSs)
				formAction = "/mops/web/ajax_t164sb04";
			else
				formAction = "/mops/web/ajax_t05st34";

			break;
		case CASHFLOW_STATEMENT:
			path = Environment.QUARTERLY_CASHFLOW_STATEMENT;
			filename = String.format(path + "%s_%04d_%d.html", code, year, quarter);
			if (useIFRSs)
				formAction = "/mops/web/ajax_t164sb05";
			else
				formAction = "/mops/web/ajax_t05st39";

			break;
		case BALANCE_SHEET_IDV:
			path = Environment.QUARTERLY_BALANCE_SHEET;
			filename = String.format(path + "%s_%04d_%d_idv.html", code, year, quarter);
			formAction = "/mops/web/ajax_t05st31";
			break;
		case INCOME_STATEMENT_IDV:
			path = Environment.QUARTERLY_INCOME_STATEMENT;
			filename = String.format(path + "%s_%04d_%d_idv.html", code, year, quarter);
			formAction = "/mops/web/ajax_t05st32";
			break;
		case CASHFLOW_STATEMENT_IDV:
			path = Environment.QUARTERLY_CASHFLOW_STATEMENT;
			filename = String.format(path + "%s_%04d_%d_idv.html", code, year, quarter);
			formAction = "/mops/web/ajax_t05st36";
			break;
		default:
			throw new Exception("Type is incorrect");
		}

		if (useIFRSs && (category.compareTo("金融保險業") == 0 || stockNum == 5871 || stockNum == 2841))
			postData = "encodeURIComponent=1&id=&key=&TYPEK=sii&step=2&firstin=1&";
		else
			postData = "step=1&firstin=1&off=1&keyword4=&code1=&TYPEK2=&checkbtn=&queryName=co_id&TYPEK=all&isnew=false&";

		postData = postData + String.format("co_id=%s&year=%s&season=0%s", URLEncoder.encode(code, "UTF-8"),
				URLEncoder.encode(String.valueOf(year - 1911), "UTF-8"),
				URLEncoder.encode(String.valueOf(quarter), "UTF-8"));

		url = "http://mops.twse.com.tw" + formAction + "?" + postData;
	}

	public static void download(int inYear, int inQuarter, String inCode, String inCategory, String lastUpdate)
			throws Exception {

		if (!isValidQuarter(inYear, inQuarter, lastUpdate)) {
			Log.info("Skip invalid stock " + code);
			return;
		}

		year = inYear;
		quarter = inQuarter;
		code = inCode;
		category = inCategory;
		stockNum = Integer.parseInt(code);
		useIFRSs = MyDB.isUseIFRSs(Integer.parseInt(code), year);

		doDownload(INCOME_STATEMENT);
		doDownload(BALANCE_SHEET);
		doDownload(CASHFLOW_STATEMENT);

		if (!useIFRSs) {
			doDownload(INCOME_STATEMENT_IDV);
			doDownload(BALANCE_SHEET_IDV);
			doDownload(CASHFLOW_STATEMENT_IDV);
		}
	}

	public static void supplement(int year, int quarter) throws Exception {
		int[] currentQuarter = getCurrentQuarter();
		int endYear = currentQuarter[0];
		int endQuarter = currentQuarter[1];

		// Make sure directories were created
		Downloader.createFolder(Environment.QUARTERLY_BALANCE_SHEET);
		Downloader.createFolder(Environment.QUARTERLY_INCOME_STATEMENT);
		Downloader.createFolder(Environment.QUARTERLY_CASHFLOW_STATEMENT);

		MyDB db = new MyDB();
		CompanyInfo[] company = db.getCompanyInfo();
		db.close();

		while (true) {
			if (year > endYear || (year == endYear && quarter > endQuarter)) {
				Log.info("End");
				break;
			}

			for (int i = 0; i < company.length; i++) {
				String code = company[i].code;
				String category = company[i].category;
				String lastUpdate = company[i].lastUpdate;

				// skip no data stocks
				if (category == null || Integer.parseInt(code) < 1000 || Integer.parseInt(code) > 9999) {
					Log.info("Skip invalid stock " + code);
					continue;
				}
				download(year, quarter, code, category, lastUpdate);
			}

			// Next quarter
			if (++quarter > 4) {
				quarter = 1;
				year++;
			}
		}
	}
}

class QuarterlyParserBase {
	String[][] data = null;
	boolean noData = true;
	String code;
	int year;
	int quarter;

	QuarterlyParserBase(int year, int quarter, String code, String folderPath) throws Exception {

		String filename = code + "_" + year + "_" + quarter + ".html";
		File file = new File(folderPath + filename);
		if (!file.exists())
			return;
		
		this.code = code;
		this.year = year;
		this.quarter = quarter;

		Document doc = Jsoup.parse(file, "UTF-8");
		Elements tblHead = doc.getElementsByClass("tblHead");
		if (tblHead.size() == 0) {
			Elements no = doc.select("body > center");
			String str = no.toString();

			// TODO: 處理這些
			if (str.contains("查無需求資料") || str.contains("查無所需資料"))
				return;
			else if (str.contains("請至採IFRSs前之")) {
				Log.warn("請至採IFRSs前之 " + code + " " + year + "_" + quarter);
				return;
			} else if (str.contains("不繼續公開發行")) {
				Log.warn("不繼續公開發行 " + code + " " + year + "_" + quarter);
				return;
			} else if (str.contains("已下市")) {
				Log.warn("已下市 " + code + " " + year + "_" + quarter);
				return;
			} else if (str.contains("不存在")) {
				// Log.warn("不存在 " + code + " " + year + "_" + quarter);
				return;
			} else if (str.contains("第二上市")) {
				// Log.warn("第二上市 " + code + " " + year + "_" + quarter);
				return;
			} else {
				throw new Exception(code + " " + year + "_" + quarter);
			}
		}

		Element eTableFirstRow = tblHead.first().parent();
		Elements eTableRows = eTableFirstRow.siblingElements();
		if (eTableFirstRow.child(0).toString().contains("公司代號")) {
			throw new Exception(filename);
		}

		data = new String[eTableRows.size()][2];
		for (int i = 0; i < data.length; i++) {
			Elements eColumes = eTableRows.get(i).children();
			if (eColumes.size() < 2)
				continue;

			data[i][0] = getText(eColumes.get(0));
			data[i][1] = getText(eColumes.get(1));
		}

		noData = false;
	}
	
	private String trim(String s) {
		if (s.isEmpty())
			return s;

		int begin = s.length();
		int end = -1;
		// Log.trace("\"" + s +"\"");
		for (int i = 0; i < s.length(); i++) {
			if (!Character.isSpaceChar(s.charAt(i))) {
				begin = i;
				break;
			}
		}
		s = s.substring(begin, s.length());
		if (s.isEmpty())
			return s;

		for (int i = s.length() - 1; i >= 0; i--) {
			if (!Character.isSpaceChar(s.charAt(i))) {
				end = i;
				break;
			}
		}

		return s.substring(0, end + 1);
	}

	private String getText(Element el) {
		String text = trim(el.text().trim().replaceAll(",", ""));
		if (text.isEmpty())
			return null;
		
		return text;
	}

	public String getData(String... names) {
		if (data == null)
			return null;

		String tempData = null;
		for (String name : names) {
			Log.trace("Get data " + code + " " + year + " " + quarter + " " + name);
			for (int i = 0; i < data.length; i++) {
				String title = data[i][0];
				if (title == null || title.compareTo(name) != 0)
					continue;

				if (data[i][1] != null && data[i][1].length() > 0)
					return data[i][1];
				else
					tempData = data[i][1];
			}
		}
		if (tempData != null)
			return tempData;

		return null;
	}
	
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
}

class IncomeStatementParser extends QuarterlyParserBase {

	IncomeStatementParser(int year, int quarter, String code) throws Exception {
		super(year, quarter, code, Environment.QUARTERLY_INCOME_STATEMENT);
	}
}

class BalanceSheetParser extends QuarterlyParserBase {

	BalanceSheetParser(int year, int quarter, String code) throws Exception {
		super(year, quarter, code, Environment.QUARTERLY_BALANCE_SHEET);
	}
}

class CachflowParser extends QuarterlyParserBase {

	CachflowParser(int year, int quarter, String code) throws Exception {
		super(year, quarter, code, Environment.QUARTERLY_CASHFLOW_STATEMENT);
	}
}

public class ImportQuarterly {
	public static MyDB db;

	private static int[] getCurrentQuarter() {
		int[] quarterSet = new int[2];

		Calendar endCal = Calendar.getInstance();
		int month = endCal.get(Calendar.MONTH) + 1;
		int day = endCal.get(Calendar.DATE);
		int year = endCal.get(Calendar.YEAR);
		int quarter = 0;

		if (month > 11 || (month == 11 && day > 15))
			quarter = 3;
		else if (month > 8 || (month == 8 && day > 15))
			quarter = 2;
		else if (month > 5 || (month == 5 && day > 15))
			quarter = 1;

		quarterSet[0] = year;
		quarterSet[1] = quarter;
		return quarterSet;
	}

	private static void importBasicData(MyStatement stm, int year, int quarter, String code) throws Exception {
		IncomeStatementParser income = new IncomeStatementParser(year, quarter, code);
		BalanceSheetParser balance = new BalanceSheetParser(year, quarter, code);
		CachflowParser cashflow = new CachflowParser(year, quarter, code);

		if (income.noData && balance.noData && cashflow.noData)
			return;

		int idx = 1;
		stm.setInt(idx++, year * 100 + quarter); // YearQuarter
		stm.setInt(idx++, code); // StockNum
		stm.setBigInt(idx++, income.getData("營業收入合計")); // 營收
		stm.setBigInt(idx++, income.getData("營業成本合計")); // 成本
		stm.setBigInt(idx++, income.getData("營業毛利（毛損）")); // 毛利
		stm.setBigInt(idx++, income.getData("研究發展費用")); // 研究發展費用
		stm.setBigInt(idx++, income.getData("營業利益（損失）")); // 營業利益
		stm.setBigInt(idx++, income.getData("營業外收入及支出合計")); // 業外收支
		stm.setBigInt(idx++, income.getData("繼續營業單位稅前淨利（淨損）", "稅前淨利（淨損）")); // 稅前淨利
		stm.setBigInt(idx++, income.getData("繼續營業單位本期淨利（淨損）")); // 稅後淨利
		stm.setBigInt(idx++, income.getData("本期綜合損益總額")); // 綜合損益
		stm.setBigInt(idx++, income.getData("母公司業主（淨利／損）")); // 母公司業主淨利
		stm.setBigInt(idx++, income.getData("母公司業主（綜合損益）")); // 母公司業主綜合損益
		stm.setFloat(idx++, income.getData("基本每股盈餘")); // EPS

		stm.setBigInt(idx++, balance.getData("流動資產合計")); // 流動資產
		stm.setBigInt(idx++, balance.getData("非流動資產合計")); // 非流動資產
		stm.setBigInt(idx++, balance.getData("資產總額", "資產總計")); // 總資產
		stm.setBigInt(idx++, balance.getData("流動負債合計")); // 流動負債
		stm.setBigInt(idx++, balance.getData("非流動負債合計")); // 非流動負債
		stm.setBigInt(idx++, balance.getData("負債總額", "負債總計")); // 總負債
		stm.setBigInt(idx++, balance.getData("保留盈餘合計")); // 保留盈餘
		stm.setBigInt(idx++, balance.getData("股本合計")); // 股本

		stm.setBigInt(idx++, cashflow.getData("營業活動之淨現金流入（流出）")); // 營業現金流
		stm.setBigInt(idx++, cashflow.getData("投資活動之淨現金流入（流出）")); // 投資現金流
		stm.setBigInt(idx++, cashflow.getData("籌資活動之淨現金流入（流出）")); // 融資現金流
		stm.setTinyInt(idx++, (quarter == 1)? 1 : 0); // 更正累計現金流
		stm.addBatch();
	}

	public static void supplementBasicData(int year, int quarter) throws Exception {
		int[] currentQuarter = getCurrentQuarter();
		int endYear = currentQuarter[0];
		int endQuarter = currentQuarter[1];

		CompanyInfo[] company = db.getCompanyInfo();

		MyStatement quarterlyST = new MyStatement(db.conn);
		quarterlyST.setInsertOnDuplicateStatement("quarterly", "YearQuarter", "StockNum", "營收", "成本", "毛利", "研究發展費用",
				"營業利益", "業外收支", "稅前淨利", "稅後淨利", "綜合損益", "母公司業主淨利", "母公司業主綜合損益", "EPS", "流動資產", "非流動資產", "總資產", "流動負債",
				"非流動負債", "總負債", "保留盈餘", "股本", "營業現金流", "投資現金流", "融資現金流", "更正累計現金流");

		while (true) {
			if (year > endYear || (year == endYear && quarter > endQuarter)) {
				Log.info("End");
				break;
			}

			for (int i = 0; i < company.length; i++) {
				String code = company[i].code;
				String category = company[i].category;
				String lastUpdate = company[i].lastUpdate;

				// skip no data stocks
				int stockNum = Integer.parseInt(code);
				if (category == null || stockNum < 1000 || stockNum > 9999) {
					// Log.info("Skip invalid stock " + code);
					continue;
				}

				if (!BasicTable.isValidQuarter(year, quarter, lastUpdate))
					continue;

				if (category.compareTo("金融保險業") == 0 || stockNum == 2905 || stockNum == 2514 || stockNum == 1409
						|| stockNum == 1718) {
					// Log.info("Skip 金融保險業");
					continue;
				}

				Boolean isUseIFRSs = MyDB.isUseIFRSs(Integer.parseInt(code), year);
				// 第四季數據是全年合併 無法直接取得
				if (isUseIFRSs && quarter != 4) {
					Log.info("Import " + code + " " + year + "_" + quarter);
					importBasicData(quarterlyST, year, quarter, code);
				}
			}

			// Next quarter
			if (++quarter > 4) {
				quarter = 1;
				year++;
			}
		}
		quarterlyST.close();
	}

	public static void main(String[] args) {

		try {
			db = new MyDB();
			BasicTable.supplement(2004, 1);
			supplementBasicData(2013, 1);
			// BalanceSheetParser fp = new BalanceSheetParser(2013, 2, "2801");
			Log.info("Done");
			db.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
