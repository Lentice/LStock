import java.io.File;
import java.net.URLEncoder;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Calendar;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

class QuarterlyData {
	int YearQuarter;
	int StockNum;
	long 營收;
	long 成本;
	long 毛利;
	long 研究發展費用;
	long 營業利益;
	long 業外收支;
	long 稅前淨利;
	long 稅後淨利;
	long 綜合損益;
	long 母公司業主淨利;
	long 母公司業主綜合損益;
	float EPS;
	long 流動資產;
	long 非流動資產;
	long 總資產;
	long 流動負債;
	long 非流動負債;
	long 總負債;
	long 保留盈餘;
	long 股本;
	long 營業現金流;
	long 投資現金流;
	long 融資現金流;
	short 現金流累計需更正;
	short 第四季累計需修正;

	long 股東權益;
	float 每股淨值;
	float 單季營收年增率;
	float 近4季營收年增率;
	float 單季毛利年增率;
	float 近4季毛利年增率;
	float 單季營業利益年增率;
	float 近4季營業利益年增率;
	float 單季稅後淨利年增率;
	float 近4季稅後淨利年增率;
	float 單季EPS年增率;
	float 近4季EPS年增率;
	float 單季淨值成長率;
	float 近4季淨值成長率;
	float 單季固定資產成長率;
	float 近4季固定資產成長率;
	float 毛利率;
	float 營業利益率;
	float 稅前淨利率;
	float 稅後淨利率;
	float 業外收支比重;
	float ROA;
	float ROE;
	float 權益乘數;
	float 盈再率;
	float 負債比;
	float 流動比;
	float 速動比;
	float 利息保障倍數;
	float 營業現金對流動負債比;
	float 營業現金對負債比;
}

class QuarterlyFixAndSupplement{
	public static void run(MyDB db) throws SQLException {
		ResultSet result;
		Statement stm;

		stm = db.conn.createStatement();
		CompanyInfo[] company = db.getCompanyInfo();

		final String allQuarterData = "SELECT * FROM quarterly WHERE StockNum=";
		MyStatement cashflowStm = new MyStatement(db.conn);
		cashflowStm.setUpdateStatement("quarterly", "YearQuarter=? AND StockNum=?", "營業現金流", "投資現金流", "融資現金流",
				"現金流累計需更正");

		MyStatement quarter4Stm = new MyStatement(db.conn);
		quarter4Stm.setUpdateStatement("quarterly", "YearQuarter=? AND StockNum=?", "營收", "成本", "毛利", "研究發展費用", "營業利益",
				"業外收支", "稅前淨利", "稅後淨利", "綜合損益", "母公司業主淨利", "母公司業主綜合損益", "EPS", "營業現金流", "投資現金流", "融資現金流", "第四季累計需修正");

		for (int i = 0; i < company.length; i++) {

			int stockNum = Integer.parseInt(company[i].code);

			result = stm.executeQuery(allQuarterData + stockNum);
			int numRow = getNumRow(result);
			if (numRow == 0)
				continue;

			QuarterlyData[] data = getData(result, numRow);
			Log.trace("更正第4季累計 " + company[i].code);
			quarter4CalculateAndImport(quarter4Stm, stockNum, data);
			Log.trace("更正現金流累計 " + company[i].code);
			cashflowCalculateAndImport(cashflowStm, stockNum, data);
		}
		stm.close();
		cashflowStm.close();
		quarter4Stm.close();
	}

	static QuarterlyData[] getData(ResultSet result, int numRow) throws SQLException {
		QuarterlyData[] allQuarter = new QuarterlyData[numRow];
		int iRow = 0;
		while (result.next()) {

			QuarterlyData quarter = new QuarterlyData();
			quarter.YearQuarter = result.getInt("YearQuarter");
			quarter.營收 = result.getLong("營收");
			quarter.成本 = result.getLong("成本");
			quarter.毛利 = result.getLong("毛利");
			quarter.研究發展費用 = result.getLong("研究發展費用");
			quarter.營業利益 = result.getLong("營業利益");
			quarter.業外收支 = result.getLong("業外收支");
			quarter.稅前淨利 = result.getLong("稅前淨利");
			quarter.稅後淨利 = result.getLong("稅後淨利");
			quarter.綜合損益 = result.getLong("綜合損益");
			quarter.母公司業主淨利 = result.getLong("母公司業主淨利");
			quarter.母公司業主綜合損益 = result.getLong("母公司業主綜合損益");
			quarter.EPS = result.getFloat("EPS");

			quarter.營業現金流 = result.getLong("營業現金流");
			quarter.投資現金流 = result.getLong("投資現金流");
			quarter.融資現金流 = result.getLong("融資現金流");
			quarter.現金流累計需更正 = result.getShort("現金流累計需更正");
			quarter.第四季累計需修正 = result.getShort("第四季累計需修正");

			allQuarter[iRow] = quarter;
			iRow++;
		}

		return allQuarter;
	}

	static void quarter4CalculateAndImport(MyStatement stm, int StockNum, QuarterlyData[] data) throws SQLException {
		boolean start = false;

		for (int i = 0; i < data.length; i++) {

			int quarter = data[i].YearQuarter % 100;

			// 跳過沒有第一季的年度
			if (!start && quarter != 1)
				continue;
			start = true;

			if (data[i].第四季累計需修正 == 0 || quarter != 4)
				continue;

			data[i].營收 -= (data[i - 1].營收 + data[i - 2].營收 + data[i - 3].營收);
			data[i].成本 -= (data[i - 1].成本 + data[i - 2].成本 + data[i - 3].成本);
			data[i].毛利 -= (data[i - 1].毛利 + data[i - 2].毛利 + data[i - 3].毛利);
			data[i].研究發展費用 -= (data[i - 1].研究發展費用 + data[i - 2].研究發展費用 + data[i - 3].研究發展費用);
			data[i].營業利益 -= (data[i - 1].營業利益 + data[i - 2].營業利益 + data[i - 3].營業利益);
			data[i].業外收支 -= (data[i - 1].業外收支 + data[i - 2].業外收支 + data[i - 3].業外收支);
			data[i].稅前淨利 -= (data[i - 1].稅前淨利 + data[i - 2].稅前淨利 + data[i - 3].稅前淨利);
			data[i].稅後淨利 -= (data[i - 1].稅後淨利 + data[i - 2].稅後淨利 + data[i - 3].稅後淨利);
			data[i].綜合損益 -= (data[i - 1].綜合損益 + data[i - 2].綜合損益 + data[i - 3].綜合損益);
			data[i].母公司業主淨利 -= (data[i - 1].母公司業主淨利 + data[i - 2].母公司業主淨利 + data[i - 3].母公司業主淨利);
			data[i].母公司業主綜合損益 -= (data[i - 1].母公司業主綜合損益 + data[i - 2].母公司業主綜合損益 + data[i - 3].母公司業主綜合損益);
			data[i].EPS -= (data[i - 1].EPS + data[i - 2].EPS + data[i - 3].EPS);

			// 未修正過 只要扣掉第三季即可
			data[i].營業現金流 -= data[i - 1].營業現金流;
			data[i].投資現金流 -= data[i - 1].投資現金流;
			data[i].融資現金流 -= data[i - 1].融資現金流;
			data[i].第四季累計需修正 = 0;

			quarter4ImportToDB(stm, StockNum, data[i]);
		}
	}

	static void quarter4ImportToDB(MyStatement stm, int StockNum, QuarterlyData data) throws SQLException {
		int idx = 1;
		stm.setBigInt(idx++, data.營收);
		stm.setBigInt(idx++, data.成本);
		stm.setBigInt(idx++, data.毛利);
		stm.setBigInt(idx++, data.研究發展費用);
		stm.setBigInt(idx++, data.營業利益);
		stm.setBigInt(idx++, data.業外收支);
		stm.setBigInt(idx++, data.稅前淨利);
		stm.setBigInt(idx++, data.稅後淨利);
		stm.setBigInt(idx++, data.綜合損益);
		stm.setBigInt(idx++, data.母公司業主淨利);
		stm.setBigInt(idx++, data.母公司業主綜合損益);
		stm.setFloat(idx++, data.EPS);
		stm.setBigInt(idx++, data.營業現金流);
		stm.setBigInt(idx++, data.投資現金流);
		stm.setBigInt(idx++, data.融資現金流);
		stm.setTinyInt(idx++, data.第四季累計需修正);
		stm.setInt(idx++, data.YearQuarter);
		stm.setInt(idx++, StockNum);
		stm.addBatch();
	}

	static void cashflowCalculateAndImport(MyStatement stm, int StockNum, QuarterlyData[] data) throws SQLException {
		boolean start = false;

		for (int i = 0; i < data.length; i++) {
			int quarter = data[i].YearQuarter % 100;

			// 跳過沒有第一季的年度
			if (!start && quarter != 1)
				continue;

			start = true;

			if (data[i].現金流累計需更正 == 0)
				continue;

			if (quarter == 1 || quarter == 4) { // 第四季另外處理
				data[i].現金流累計需更正 = 0;
			} else if (quarter == 2) {
				data[i].營業現金流 = data[i].營業現金流 - data[i - 1].營業現金流;
				data[i].投資現金流 = data[i].投資現金流 - data[i - 1].投資現金流;
				data[i].融資現金流 = data[i].融資現金流 - data[i - 1].融資現金流;
				data[i].現金流累計需更正 = 0;
			} else if (quarter == 3) {
				data[i].營業現金流 = data[i].營業現金流 - data[i - 1].營業現金流 - data[i - 2].營業現金流;
				data[i].投資現金流 = data[i].投資現金流 - data[i - 1].投資現金流 - data[i - 2].投資現金流;
				data[i].融資現金流 = data[i].融資現金流 - data[i - 1].融資現金流 - data[i - 2].融資現金流;
				data[i].現金流累計需更正 = 0;
			}

			cashflowImportToDB(stm, StockNum, data[i]);
		}
	}

	static void cashflowImportToDB(MyStatement stm, int StockNum, QuarterlyData data) throws SQLException {
		int idx = 1;
		stm.setBigInt(idx++, data.營業現金流);
		stm.setBigInt(idx++, data.投資現金流);
		stm.setBigInt(idx++, data.融資現金流);
		stm.setTinyInt(idx++, data.現金流累計需更正);
		stm.setInt(idx++, data.YearQuarter);
		stm.setInt(idx++, StockNum);
		stm.addBatch();
	}

	static int getNumRow(ResultSet result) throws SQLException {
		result.last();
		int numRow = result.getRow();
		result.beforeFirst();
		return numRow;
	}
}

class QuarterlyBasicTable {
	public static final int INCOME_STATEMENT = 0;
	public static final int BALANCE_SHEET = 1;
	public static final int CASHFLOW_STATEMENT = 2;
	public static final int INCOME_STATEMENT_IDV = 3; // IDV: 個別財報
	public static final int BALANCE_SHEET_IDV = 4;
	public static final int CASHFLOW_STATEMENT_IDV = 5;

	String folderPath;
	String filename;
	File file;

	boolean useIFRSs;
	int year;
	int quarter;
	String code;
	String category;
	int tableType;

	int lastUpdateDate;
	int stockNum;
	String formAction;
	String[][] data = null;

	QuarterlyBasicTable(int year, int quarter, CompanyInfo company, int tableType) throws Exception {
		if (year < 2001)
			throw new Exception("Year is earlier than 2001");

		this.year = year;
		this.quarter = quarter;
		this.code = company.code;
		this.category = company.category;
		lastUpdateDate = Integer.parseInt(company.lastUpdate);
		this.tableType = tableType;

		stockNum = Integer.parseInt(code);
		useIFRSs = MyDB.isUseIFRSs(stockNum, year);

		getDownloadInfo();
		file = new File(filename);
		if (!file.exists()) {
			download();
			file = new File(filename);
		}
	}

	boolean parse() throws Exception {

		String filename = code + "_" + year + "_" + quarter + ".html";
		File file = new File(folderPath + filename);
		if (!file.exists())
			return false;

		Document doc = Jsoup.parse(file, "UTF-8");
		Elements tblHead = doc.getElementsByClass("tblHead");
		if (tblHead.size() == 0) {
			Elements no = doc.select("body > center");
			String str = no.toString();

			// TODO: 處理這些
			if (str.contains("查無需求資料") || str.contains("查無所需資料"))
				return false;
			else if (str.contains("請至採IFRSs前之")) {
				Log.warn("請至採IFRSs前之 " + code + " " + year + "_" + quarter);
				return false;
			} else if (str.contains("不繼續公開發行")) {
				Log.warn("不繼續公開發行 " + code + " " + year + "_" + quarter);
				return false;
			} else if (str.contains("已下市")) {
				Log.warn("已下市 " + code + " " + year + "_" + quarter);
				return false;
			} else if (str.contains("不存在")) {
				// Log.warn("不存在 " + code + " " + year + "_" + quarter);
				return false;
			} else if (str.contains("第二上市")) {
				// Log.warn("第二上市 " + code + " " + year + "_" + quarter);
				return false;
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

			data[i][0] = HtmlParser.getText(eColumes.get(0));
			data[i][1] = HtmlParser.getText(eColumes.get(1));
		}

		return true;
	}

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

	public boolean isValidQuarter(int year, int quarter) throws Exception {
		int lastYear = lastUpdateDate / 10000;
		int lastMonth = lastUpdateDate / 100 % 100;
		int lastDay = lastUpdateDate % 100;

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

	private void getDownloadInfo() throws Exception {

		switch (tableType) {
		case BALANCE_SHEET:
			folderPath = Environment.QUARTERLY_BALANCE_SHEET;
			filename = String.format(folderPath + "%s_%04d_%d.html", code, year, quarter);
			if (useIFRSs)
				formAction = "/mops/web/ajax_t164sb03";
			else
				formAction = "/mops/web/ajax_t05st33";

			break;
		case INCOME_STATEMENT:
			folderPath = Environment.QUARTERLY_INCOME_STATEMENT;
			filename = String.format(folderPath + "%s_%04d_%d.html", code, year, quarter);
			if (useIFRSs)
				formAction = "/mops/web/ajax_t164sb04";
			else
				formAction = "/mops/web/ajax_t05st34";

			break;
		case CASHFLOW_STATEMENT:
			folderPath = Environment.QUARTERLY_CASHFLOW_STATEMENT;
			filename = String.format(folderPath + "%s_%04d_%d.html", code, year, quarter);
			if (useIFRSs)
				formAction = "/mops/web/ajax_t164sb05";
			else
				formAction = "/mops/web/ajax_t05st39";

			break;
		case BALANCE_SHEET_IDV:
			folderPath = Environment.QUARTERLY_BALANCE_SHEET;
			filename = String.format(folderPath + "%s_%04d_%d_idv.html", code, year, quarter);
			formAction = "/mops/web/ajax_t05st31";
			break;
		case INCOME_STATEMENT_IDV:
			folderPath = Environment.QUARTERLY_INCOME_STATEMENT;
			filename = String.format(folderPath + "%s_%04d_%d_idv.html", code, year, quarter);
			formAction = "/mops/web/ajax_t05st32";
			break;
		case CASHFLOW_STATEMENT_IDV:
			folderPath = Environment.QUARTERLY_CASHFLOW_STATEMENT;
			filename = String.format(folderPath + "%s_%04d_%d_idv.html", code, year, quarter);
			formAction = "/mops/web/ajax_t05st36";
			break;
		default:
			throw new Exception("Type is incorrect");
		}
	}

	void download() throws Exception {
		final int MAX_DOWNLOAD_RETRY = 20;

		if (!isValidQuarter(year, quarter)) {
			Log.info("Skip invalid stock " + code);
			return;
		}

		if (isValidQuarterlyData(filename, 1000)) {
			Log.info("Skip existing file " + filename);
			return;
		}

		String postData;
		if (useIFRSs && (category.compareTo("金融保險業") == 0 || stockNum == 5871 || stockNum == 2841))
			postData = "encodeURIComponent=1&id=&key=&TYPEK=sii&step=2&firstin=1&";
		else
			postData = "step=1&firstin=1&off=1&keyword4=&code1=&TYPEK2=&checkbtn=&queryName=co_id&TYPEK=all&isnew=false&";

		postData = postData + String.format("co_id=%s&year=%s&season=0%s", URLEncoder.encode(code, "UTF-8"),
				URLEncoder.encode(String.valueOf(year - 1911), "UTF-8"),
				URLEncoder.encode(String.valueOf(quarter), "UTF-8"));

		String url = "http://mops.twse.com.tw" + formAction + "?" + postData;

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

			if (++retry > MAX_DOWNLOAD_RETRY)
				break;
		} while (dwResult < 0 || !isValidQuarterlyData(filename, 1000));
	}

	public String getData(String... names) {
		if (data == null)
			return null;

		String tempData = null;
		for (String name : names) {
			// Log.dbg("Get " + code + " " + year + "_" + quarter + " " + name);
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

	private static void importBasicData(MyStatement stm, int year, int quarter, CompanyInfo company) throws Exception {
		QuarterlyBasicTable income = new QuarterlyBasicTable(year, quarter, company,
				QuarterlyBasicTable.INCOME_STATEMENT);
		QuarterlyBasicTable balance = new QuarterlyBasicTable(year, quarter, company,
				QuarterlyBasicTable.BALANCE_SHEET);
		QuarterlyBasicTable cashflow = new QuarterlyBasicTable(year, quarter, company,
				QuarterlyBasicTable.CASHFLOW_STATEMENT);

		income.parse();
		balance.parse();
		cashflow.parse();

		int idx = 1;
		stm.setInt(idx++, year * 100 + quarter); // YearQuarter
		stm.setInt(idx++, company.code); // StockNum
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
		stm.setTinyInt(idx++, (quarter == 1 || quarter == 4) ? 0 : 1); // 現金流累計需更正
		stm.setTinyInt(idx++, (quarter == 4) ? 1 : 0); // 第四季累計需修正
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
				"非流動負債", "總負債", "保留盈餘", "股本", "營業現金流", "投資現金流", "融資現金流", "現金流累計需更正", "第四季累計需修正");

		while (true) {
			if (year > endYear || (year == endYear && quarter > endQuarter)) {
				Log.info("End");
				break;
			}

			for (int i = 0; i < company.length; i++) {
				String code = company[i].code;
				String category = company[i].category;

				// skip no data stocks
				int stockNum = Integer.parseInt(code);
				if (category == null || stockNum < 1000 || stockNum > 9999) {
					// Log.info("Skip invalid stock " + code);
					continue;
				}

				if (category.compareTo("金融保險業") == 0 || stockNum == 2905 || stockNum == 2514 || stockNum == 1409
						|| stockNum == 1718) {
					// Log.info("Skip 金融保險業");
					continue;
				}

				Boolean isUseIFRSs = MyDB.isUseIFRSs(Integer.parseInt(code), year);
				// 第四季數據是全年合併 無法直接取得
				if (isUseIFRSs) {
					Log.info("Import " + code + " " + year + "_" + quarter);
					importBasicData(quarterlyST, year, quarter, company[i]);
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
			supplementBasicData(2013, 1);
			QuarterlySplitCashflow.run(db);
			Log.info("Done");
			db.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
