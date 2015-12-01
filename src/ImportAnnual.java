import java.io.File;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Calendar;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

class AnnualData {
	private static final String ALL_DATA = "SELECT * FROM annual WHERE StockNum=%s AND 總資產 > 0 ORDER BY Year";

	Integer Year;
	Integer StockNum;
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
	long 存貨;
	long 預付款項;
	long 非流動資產;
	long 備供出售金融資產;
	long 持有至到期日金融資產;
	long 以成本衡量之金融資產;
	long 採用權益法之投資淨額;
	long 固定資產;
	long 總資產;
	long 流動負債;
	long 非流動負債;
	long 總負債;
	long 保留盈餘;
	long 股本;
	long 營業現金流;
	long 投資現金流;
	long 融資現金流;
	long 自由現金流;
	Boolean 現金流累計需更正;
	Boolean 第四季累計需修正;

	Long 股東權益;
	Float 每股淨值;
	Long 長期投資;
	Float 毛利率;
	Float 營業利益率;
	Float 稅前淨利率;
	Float 稅後淨利率;
	Float 總資產週轉率;
	Float 權益乘數;
	Float 業外收支比重;
	Float ROA;
	Float ROE;
	Float 負債比;
	Float 流動比;
	Float 速動比;
	Float 營業現金對流動負債比;
	Float 營業現金對負債比;
	Float 營業現金流對淨利比;
	Float 自由現金流對淨利比;
	Float 利息保障倍數;
	Date 除息日期;
	float 現金股利;
	Date 除權日期;
	float 盈餘配股;
	float 資本公積;
	float 除權除息參考價;

	AnnualData() {

	}

	AnnualData(ResultSet rs) throws SQLException {
		int colume = 1;

		/* 以下數值若不存在 則當成0 以方便計算 */
		Year = (Integer) rs.getObject(colume++, Integer.class);
		StockNum = (Integer) rs.getObject(colume++, Integer.class);
		營收 = rs.getObject(colume++, Long.class);
		成本 = rs.getObject(colume++, Long.class);
		毛利 = rs.getObject(colume++, Long.class);
		研究發展費用 = rs.getObject(colume++, Long.class);
		營業利益 = rs.getObject(colume++, Long.class);
		業外收支 = rs.getObject(colume++, Long.class);
		稅前淨利 = rs.getObject(colume++, Long.class);
		稅後淨利 = rs.getObject(colume++, Long.class);
		綜合損益 = rs.getObject(colume++, Long.class);
		母公司業主淨利 = rs.getObject(colume++, Long.class);
		母公司業主綜合損益 = rs.getObject(colume++, Long.class);
		EPS = rs.getObject(colume++, Float.class);
		流動資產 = rs.getObject(colume++, Long.class);
		存貨 = rs.getObject(colume++, Long.class);
		預付款項 = rs.getObject(colume++, Long.class);
		非流動資產 = rs.getObject(colume++, Long.class);
		備供出售金融資產 = rs.getObject(colume++, Long.class);
		持有至到期日金融資產 = rs.getObject(colume++, Long.class);
		以成本衡量之金融資產 = rs.getObject(colume++, Long.class);
		採用權益法之投資淨額 = rs.getObject(colume++, Long.class);
		固定資產 = rs.getObject(colume++, Long.class);
		總資產 = (Long) rs.getObject(colume++);
		流動負債 = rs.getObject(colume++, Long.class);
		非流動負債 = rs.getObject(colume++, Long.class);
		總負債 = rs.getObject(colume++, Long.class);
		保留盈餘 = rs.getObject(colume++, Long.class);
		股本 = rs.getObject(colume++, Long.class);
		營業現金流 = rs.getObject(colume++, Long.class);
		投資現金流 = rs.getObject(colume++, Long.class);
		融資現金流 = rs.getObject(colume++, Long.class);
		自由現金流 = rs.getObject(colume++, Long.class);

		/* 以下數值若不存在 則當成null 以方便判斷是否已經存在 */
		股東權益 = (Long) rs.getObject(colume++);
		每股淨值 = (Float) rs.getObject(colume++);
		長期投資 = (Long) rs.getObject(colume++);
		毛利率 = (Float) rs.getObject(colume++);
		營業利益率 = (Float) rs.getObject(colume++);
		稅前淨利率 = (Float) rs.getObject(colume++);
		稅後淨利率 = (Float) rs.getObject(colume++);
		總資產週轉率 = (Float) rs.getObject(colume++);
		權益乘數 = (Float) rs.getObject(colume++);
		業外收支比重 = (Float) rs.getObject(colume++);
		ROA = (Float) rs.getObject(colume++);
		ROE = (Float) rs.getObject(colume++);
		負債比 = (Float) rs.getObject(colume++);
		流動比 = (Float) rs.getObject(colume++);
		速動比 = (Float) rs.getObject(colume++);
		營業現金對流動負債比 = (Float) rs.getObject(colume++);
		營業現金對負債比 = (Float) rs.getObject(colume++);
		營業現金流對淨利比 = (Float) rs.getObject(colume++);
		自由現金流對淨利比 = (Float) rs.getObject(colume++);
		利息保障倍數 = (Float) rs.getObject(colume++);

		除息日期 = (Date) rs.getObject(colume++);
		現金股利 = rs.getObject(colume++, Float.class);
		除權日期 = (Date) rs.getObject(colume++);
		盈餘配股 = rs.getObject(colume++, Float.class);
		資本公積 = rs.getObject(colume++, Float.class);
		除權除息參考價 = rs.getObject(colume++, Float.class);
	}

	public static AnnualData[] getAllData(MyDB db, int stockNum) throws SQLException {
		Statement stm = db.conn.createStatement();
		ResultSet rs = stm.executeQuery(String.format(ALL_DATA, stockNum));
		int numRow = getNumRow(rs);
		if (numRow == 0)
			return null;

		AnnualData[] allQuarter = new AnnualData[numRow];
		int iRow = 0;
		while (rs.next()) {
			allQuarter[iRow] = new AnnualData(rs);
			iRow++;
		}

		stm.close();
		return allQuarter;
	}

	static int getNumRow(ResultSet result) throws SQLException {
		result.last();
		int numRow = result.getRow();
		result.beforeFirst();
		return numRow;
	}

	static AnnualData getData(AnnualData[] allYear, int year) throws SQLException {
		for (AnnualData data : allYear) {
			if (data.Year == year) {
				return data;
			}
		}
		return null;
	}
}

class AnnualSupplement {
	public static void run(MyDB db) throws Exception {
		Company[] companies = Company.getAllCompanies(db);

		MyStatement supplementStm = new MyStatement(db.conn);
		supplementStm.setUpdateStatement("annual", "Year=? AND StockNum=?", "自由現金流", "股東權益", "每股淨值", "長期投資", "毛利率",
				"營業利益率", "稅前淨利率", "稅後淨利率", "總資產週轉率", "權益乘數", "業外收支比重", "ROA", "ROE", "負債比", "流動比", "速動比", "營業現金對流動負債比",
				"營業現金對負債比", "營業現金流對淨利比", "自由現金流對淨利比");

		for (Company company : companies) {

			int stockNum = Integer.parseInt(company.code);

			AnnualData[] data = AnnualData.getAllData(db, stockNum);
			if (data == null)
				continue;

			Log.trace("補完剩餘欄位 " + company.code);
			supplementOtherField(supplementStm, stockNum, data);
		}

		supplementStm.close();
	}

	static void supplementOtherField(MyStatement stm, int StockNum, AnnualData[] allYear) throws SQLException {

		for (AnnualData qdata : allYear) {
			if (qdata.股東權益 != null)
				continue;

			qdata.自由現金流 = qdata.營業現金流 - qdata.投資現金流;

			qdata.股東權益 = qdata.總資產 - qdata.總負債;
			if (qdata.股本 != 0)
				qdata.每股淨值 = (float) qdata.股東權益 / (qdata.股本 / 10); // 每股10元

			if (qdata.長期投資 == null || qdata.長期投資 == 0)
				qdata.長期投資 = qdata.備供出售金融資產 + qdata.持有至到期日金融資產 + qdata.以成本衡量之金融資產 + qdata.採用權益法之投資淨額;

			if (qdata.營收 != 0) {
				qdata.毛利率 = (float) qdata.毛利 / qdata.營收;
				qdata.營業利益率 = (float) qdata.營業利益 / qdata.營收;
				qdata.稅前淨利率 = (float) qdata.稅前淨利 / qdata.營收;
				qdata.稅後淨利率 = (float) qdata.稅後淨利 / qdata.營收;
			}
			if (qdata.稅前淨利 != 0)
				qdata.業外收支比重 = (float) qdata.業外收支 / qdata.稅前淨利;

			if (qdata.總資產 != 0) {
				qdata.ROA = (float) qdata.稅後淨利 / qdata.總資產;
				qdata.負債比 = (float) qdata.總負債 / qdata.總資產;
				qdata.總資產週轉率 = (float) qdata.營收 / qdata.總資產;
			}

			if (qdata.股東權益 != 0) {
				qdata.ROE = (float) qdata.稅後淨利 / qdata.股東權益;
				qdata.權益乘數 = (float) qdata.總資產 / qdata.股東權益;
			}

			if (qdata.流動負債 != 0) {
				qdata.流動比 = (float) qdata.流動資產 / qdata.流動負債;
				qdata.速動比 = (float) (qdata.流動資產 - qdata.存貨 - qdata.預付款項) / qdata.流動負債;
				qdata.營業現金對流動負債比 = (float) qdata.營業現金流 / qdata.流動負債;
			}

			if (qdata.總負債 != 0)
				qdata.營業現金對負債比 = (float) qdata.營業現金流 / qdata.總負債;
			
			if (qdata.稅後淨利 != 0) {
				qdata.營業現金流對淨利比 = (float) qdata.營業現金流 / qdata.稅後淨利;
				qdata.自由現金流對淨利比 = (float) qdata.自由現金流 / qdata.稅後淨利;
			}

			// TODO: data.利息保障倍數 =

			supplementImportToDB(stm, StockNum, qdata);
		}
	}

	static void supplementImportToDB(MyStatement stm, int StockNum, AnnualData data) throws SQLException {
		stm.setObject(data.自由現金流);
		stm.setObject(data.股東權益);
		stm.setObject(data.每股淨值);
		stm.setObject(data.長期投資);
		stm.setObject(data.毛利率);
		stm.setObject(data.營業利益率);
		stm.setObject(data.稅前淨利率);
		stm.setObject(data.稅後淨利率);
		stm.setObject(data.總資產週轉率);
		stm.setObject(data.權益乘數);
		stm.setObject(data.業外收支比重);
		stm.setObject(data.ROA);
		stm.setObject(data.ROE);
		stm.setObject(data.負債比);
		stm.setObject(data.流動比);
		stm.setObject(data.速動比);

		stm.setObject(data.營業現金對流動負債比);
		stm.setObject(data.營業現金對負債比);
		stm.setObject(data.營業現金流對淨利比);
		stm.setObject(data.自由現金流對淨利比);
		// TODO: 利息保障倍數

		stm.setObject(data.Year);
		stm.setObject(StockNum);
		stm.addBatch();
	}
}

// 股利
class Dividend {
	class DividendInfo {
		String code;
		String cashDiv; // 現金股利
		String RetainedEarningsDiv; // 盈餘配股
		String CapitalReserveDiv; // 公積配股
		Date exRightDate; // 除權日
		Date exDivDate; // 除息日
		String refPrice; // 除權除息參考價
	}

	static final String folderPath = Environment.ANNUAL_DIVIDEND;
	private static final int MaxColume = 11;

	int year;
	boolean noData = true;
	File file;
	DividendInfo[] divInfo;

	Dividend(int year) throws Exception {
		if (year < 2001)
			throw new Exception("Year is earlier than 2001");

		this.year = year;

		String filename = String.format(folderPath + "Capital_%04d.html", year);
		file = new File(filename);
		if (!file.exists()) {
			download(year);
			file = new File(filename);
		}
	}

	boolean parse() throws Exception {
		String temp;
		if (!file.exists()) {
			Log.warn("檔案不存在: " + file.getPath());
			return false;
		}

		Document doc = Jsoup.parse(file, "MS950");
		Elements eTables = doc.select("#detial_right_title > div > table:nth-child(5) > tbody");
		if (eTables.size() == 0)
			throw new Exception("Can not find valid table");

		Element eTable = eTables.first();
		Elements eTRs = eTable.select(":root > tr");
		if (eTRs.size() == 0)
			throw new Exception("Can not find valid TR");

		divInfo = new DividendInfo[eTRs.size()];
		for (int i = 0; i < eTRs.size(); i++) {
			Elements td = eTRs.get(i).children();
			if (td.size() != MaxColume)
				continue;

			Elements eField0 = td.get(0).select("a[href]");
			if (eField0.size() == 0)
				continue;

			DividendInfo info = new DividendInfo();
			info.code = eField0.first().text().split(" ")[0].trim();
			info.cashDiv = HtmlParser.getText(td.get(2));
			temp = HtmlParser.getText(td.get(3));
			info.exDivDate = (temp == null) ? null : Date.valueOf(temp);
			info.RetainedEarningsDiv = HtmlParser.getText(td.get(5));
			info.CapitalReserveDiv = HtmlParser.getText(td.get(6));
			temp = HtmlParser.getText(td.get(7));
			info.exRightDate = (temp == null) ? null : Date.valueOf(temp);
			info.refPrice = HtmlParser.getText(td.get(10));

			divInfo[i] = info;
		}

		noData = false;
		return true;
	}

	int download(int year) throws Exception {
		if (year < 2001)
			throw new Exception("Year is earlier than 2001");

		final String formAction = "/bulletin/all_List.asp?xt=1&xy=1";
		final String url = "http://www.capital.com.tw/" + formAction;
		final String postData = String
				.format("VD1=1&xYY=%d&xeMM=13&mkt=tse&s1=none&keyword=&imageField2.x=30&imageField2.y=8", year);
		final String filename = String.format(folderPath + "Capital_%04d.html", year);

		Log.info("Download dividend info " + year);
		int ret = Downloader.httpDownload(url, postData, filename);
		if (ret != 0) {
			Log.info("Fail");
			return ret;
		}

		return 0;
	}

	private void importToDB(MyStatement stm) throws Exception {
		Log.info("Import Dividend " + year);

		if (!parse())
			return;

		doImport(stm);
	}

	private void doImport(MyStatement stm) throws Exception {

		for (int i = 0; i < divInfo.length; i++) {
			if (divInfo[i] == null)
				continue;

			int idx = 1;
			stm.setInt(idx++, year - 1); // Year
			stm.setInt(idx++, divInfo[i].code); // StockNum
			stm.setDecimal(idx++, divInfo[i].cashDiv); // 現金股利
			stm.setDecimal(idx++, divInfo[i].RetainedEarningsDiv); // 盈餘配股
			stm.setDecimal(idx++, divInfo[i].CapitalReserveDiv); // 資本公積
			stm.setDate(idx++, divInfo[i].exDivDate); // 除息日期
			stm.setDate(idx++, divInfo[i].exRightDate); // 除權日期
			stm.setDecimal(idx++, divInfo[i].refPrice); // 除權除息參考價
			Log.info("Import Dividend " + divInfo[i].code + " " + year + " ");
			stm.addBatch();
		}
	}

	public static void supplementDB(MyDB db, int year) throws Exception {
		Calendar cal = Calendar.getInstance();
		int currentYear = cal.get(Calendar.YEAR);

		MyStatement stm = new MyStatement(db.conn);
		stm.setInsertOnDuplicateStatement("annual", "Year", "StockNum", "現金股利", "盈餘配股", "資本公積", "除息日期", "除權日期",
				"除權除息參考價");

		for (; year <= currentYear; year++) {

			Dividend div = new Dividend(year);
			div.importToDB(stm);
		}
		stm.close();
	}
}

public class ImportAnnual {
	public static MyDB db;

		private static void importBasicDataNoIFRSs(MyStatement stm, int year, Company company) throws Exception {
		QuarterlyBasicTable income;
		QuarterlyBasicTable balance;

		income = new QuarterlyBasicTable(year, 4, company, QuarterlyBasicTable.INCOME_STATEMENT);
		if (!income.parse()) {
			income = new QuarterlyBasicTable(year, 4, company, QuarterlyBasicTable.INCOME_STATEMENT_IDV);
			if (!income.parse())
				return;
		}

		balance = new QuarterlyBasicTable(year, 4, company, QuarterlyBasicTable.BALANCE_SHEET);
		if (!balance.parse()) {
			balance = new QuarterlyBasicTable(year, 4, company, QuarterlyBasicTable.BALANCE_SHEET_IDV);
			if (!balance.parse())
				return;
		}

		int idx = 1;
		stm.setInt(idx++, year); // Year
		stm.setInt(idx++, company.code); // StockNum
		stm.setBigInt(idx++, income.getData("營業收入合計", "收入")); // 營收
		stm.setBigInt(idx++, income.getData("營業成本合計", "支出")); // 成本
		stm.setBigInt(idx++, income.getData("營業毛利(毛損)")); // 毛利
		stm.setBigInt(idx++, income.getData("研究發展費用")); // 研究發展費用
		stm.setBigInt(idx++, income.getData("營業淨利(淨損)")); // 營業利益
		
		long 業外收支 = 0;
		String temp = income.getData("營業外收入及利益");
		if (temp != null)
			業外收支 += Long.parseLong(temp);
		
		temp = income.getData("營業外費用及損失");
		if (temp != null)
			業外收支 -= Long.parseLong(temp);
		
		stm.setBigInt(idx++, 業外收支); // 業外收支
		stm.setBigInt(idx++, income.getData("繼續營業部門稅前淨利(淨損)", "繼續營業單位稅前淨利(淨損)", "繼續營業單位稅前淨益(淨損)")); // 稅前淨利
		stm.setBigInt(idx++, income.getData("繼續營業部門淨利(淨損)", "繼續營業單位淨利(淨損)", "合併總損益")); // 稅後淨利
		stm.setBigInt(idx++, income.getData("本期淨利(淨損)", "合併淨損益")); // 綜合損益
		stm.setFloat(idx++, income.getData("普通股每股盈餘", "基本每股盈餘")); // EPS

		stm.setBigInt(idx++, balance.getData("流動資產")); // 流動資產
		stm.setBigInt(idx++, balance.getData("存 貨", "存貨")); // 存貨
		stm.setBigInt(idx++, balance.getData("預付款項")); // 預付款項
		stm.setBigInt(idx++, balance.getData("基金及投資", "基金及長期投資", "基金與投資")); // 長期投資
		stm.setBigInt(idx++, balance.getData("固定資產淨額", "固定資產")); // 固定資產
		stm.setBigInt(idx++, balance.getData("資產總計", "資產", "資產合計")); // 總資產

		stm.setBigInt(idx++, balance.getData("流動負債合計", "流動負債")); // 流動負債
		stm.setBigInt(idx++, balance.getData("負債總計", "負債總額")); // 總負債
		stm.setBigInt(idx++, balance.getData("保留盈餘合計")); // 保留盈餘
		stm.setBigInt(idx++, balance.getData("普通股股本", "股 本", "股本")); // 股本

		stm.addBatch();
	}

	private static void importBasicData(MyStatement stm, int year, Company company) throws Exception {
		QuarterlyBasicTable income = new QuarterlyBasicTable(year, 4, company, QuarterlyBasicTable.INCOME_STATEMENT);
		QuarterlyBasicTable balance = new QuarterlyBasicTable(year, 4, company, QuarterlyBasicTable.BALANCE_SHEET);
		QuarterlyBasicTable cashflow = new QuarterlyBasicTable(year, 4, company,
				QuarterlyBasicTable.CASHFLOW_STATEMENT);

		boolean incomeResult = income.parse();
		boolean balanceResult = balance.parse();
		boolean cashflowResult = cashflow.parse();

		if (!incomeResult || !balanceResult || !cashflowResult)
			return;

		int idx = 1;
		stm.setInt(idx++, year); // Year
		stm.setInt(idx++, company.code); // StockNum
		stm.setBigInt(idx++, income.getData("營業收入合計", "收入合計")); // 營收
		stm.setBigInt(idx++, income.getData("營業成本合計", "支出合計")); // 成本
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
		stm.setBigInt(idx++, balance.getData("存貨")); // 存貨
		stm.setBigInt(idx++, balance.getData("預付款項")); // 預付款項
		stm.setBigInt(idx++, balance.getData("不動產、廠房及設備")); // 非流動資產
		stm.setBigInt(idx++, balance.getData("備供出售金融資產－非流動淨額", "備供出售金融資產－非流動")); // 備供出售金融資產
		stm.setBigInt(idx++, balance.getData("持有至到期日金融資產－非流動淨額", "持有至到期日金融資產－非流動")); // 持有至到期日金融資產
		stm.setBigInt(idx++, balance.getData("以成本衡量之金融資產－非流動淨額", "以成本衡量之金融資產－非流動")); // 以成本衡量之金融資產
		stm.setBigInt(idx++, balance.getData("採用權益法之投資淨額", "採用權益法之投資")); // 採用權益法之投資淨額
		stm.setBigInt(idx++, balance.getData("非流動資產合計")); // 固定資產
		stm.setBigInt(idx++, balance.getData("資產總額", "資產總計")); // 總資產

		stm.setBigInt(idx++, balance.getData("流動負債合計")); // 流動負債
		stm.setBigInt(idx++, balance.getData("非流動負債合計")); // 非流動負債
		stm.setBigInt(idx++, balance.getData("負債總額", "負債總計")); // 總負債
		stm.setBigInt(idx++, balance.getData("保留盈餘合計")); // 保留盈餘
		stm.setBigInt(idx++, balance.getData("股本合計")); // 股本

		stm.setBigInt(idx++, cashflow.getData("營業活動之淨現金流入（流出）")); // 營業現金流
		stm.setBigInt(idx++, cashflow.getData("投資活動之淨現金流入（流出）")); // 投資現金流
		stm.setBigInt(idx++, cashflow.getData("籌資活動之淨現金流入（流出）")); // 融資現金流
		stm.addBatch();
	}

	public static void supplementBasicData(int year) throws Exception {
		Calendar cal = Calendar.getInstance();
		int currentYear = cal.get(Calendar.YEAR);

		Company[] company = Company.getAllCompanies(db);

		MyStatement queryIFRSs = new MyStatement(db.conn);
		queryIFRSs.setInsertOnDuplicateStatement("annual", "Year", "StockNum", "營收", "成本", "毛利", "研究發展費用", "營業利益",
				"業外收支", "稅前淨利", "稅後淨利", "綜合損益", "母公司業主淨利", "母公司業主綜合損益", "EPS", "流動資產", "存貨", "預付款項", "非流動資產",
				"備供出售金融資產", "持有至到期日金融資產", "以成本衡量之金融資產", "採用權益法之投資淨額", "固定資產", "總資產", "流動負債", "非流動負債", "總負債", "保留盈餘",
				"股本", "營業現金流", "投資現金流", "融資現金流");

		MyStatement queryNoIFRSs = new MyStatement(db.conn);
		queryNoIFRSs.setInsertOnDuplicateStatement("annual", "Year", "StockNum", "營收", "成本", "毛利", "研究發展費用", "營業利益",
				"業外收支", "稅前淨利", "稅後淨利", "綜合損益", "EPS", "流動資產", "存貨", "預付款項", "長期投資", "固定資產", "總資產", "流動負債", "總負債",
				"保留盈餘", "股本");

		for (; year < currentYear; year++) {
			for (int i = 0; i < company.length; i++) {
				String code = company[i].code;
				String category = company[i].category;

				// skip no data stocks
				int stockNum = Integer.parseInt(code);
				if (category == null || stockNum < 1000 || stockNum > 9999) {
					Log.info(code + " skipped: invalid stock");
					continue;
				}

				if (!company[i].isValidYear(year))
					continue;

				if (category.compareTo("金融保險業") == 0 || stockNum == 2807) {
					Log.info(code + " skipped: 金融保險業");
					continue;
				}
				
				if (stockNum == 2905 || stockNum == 2514 || stockNum == 1409 || stockNum == 1718) {
					Log.info(code + " Skipped: 表格格式與人不同");
					continue;
				}
				
				if (stockNum == 8463)
					Log.info("");

				Log.info("Import BasicData " + code + " " + year);
				Boolean isUseIFRSs = MyDB.isUseIFRSs(Integer.parseInt(code), year);
				// 第四季數據是全年合併 無法直接取得
				if (isUseIFRSs) {
					importBasicData(queryIFRSs, year, company[i]);
				} else {
					importBasicDataNoIFRSs(queryNoIFRSs, year, company[i]);
				}
			}
		}
		queryIFRSs.close();
	}

	public static void main(String[] args) {

		try {
			db = new MyDB();
			int year = db.getLastAnnualRevenue();
			supplementBasicData(year);
			Dividend.supplementDB(db, year + 1);
			AnnualSupplement.run(db);
			Log.info("Done");
			db.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
