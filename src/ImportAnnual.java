import java.io.File;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

class AnnualData {
	private static final String ALL_DATA = "SELECT * FROM annual WHERE StockNum=%s AND 總資產 > 0 ORDER BY Year";

	Integer year;
	Integer stockNum;
	long 營收;
	long 成本;
	long 毛利;
	long 營業利益;
	long 業外收支;
	long 稅前淨利;
	long 稅後淨利;
	long 綜合損益;
	long 母公司業主淨利;
	long 母公司業主綜合損益;
	float EPS;
	long 流動資產;
	long 現金及約當現金;
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
	long 利息費用;
	long 營業現金流;
	long 投資現金流;
	long 融資現金流;
	long 自由現金流;

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
	Float 存貨周轉率;
	Float 負債比;
	Float 流動比;
	Float 速動比;
	Float 利息保障倍數;
	Float 營業現金對流動負債比;
	Float 營業現金對負債比;
	Float 營業現金流對淨利比;
	Float 自由現金流對淨利比;
	Date 除息日期;
	float 現金股利;
	Date 除權日期;
	float 盈餘配股;
	float 資本公積;
	float 除權除息參考價;

	AnnualData() {

	}

	AnnualData(ResultSet rs) throws SQLException {

		/* 以下數值若不存在 則當成0 以方便計算 */
		year = (Integer) rs.getObject("Year", Integer.class);
		stockNum = (Integer) rs.getObject("StockNum", Integer.class);
		營收 = rs.getObject("營收", Long.class);
		成本 = rs.getObject("成本", Long.class);
		毛利 = rs.getObject("毛利", Long.class);
		營業利益 = rs.getObject("營業利益", Long.class);
		業外收支 = rs.getObject("業外收支", Long.class);
		稅前淨利 = rs.getObject("稅前淨利", Long.class);
		稅後淨利 = rs.getObject("稅後淨利", Long.class);
		綜合損益 = rs.getObject("綜合損益", Long.class);
		母公司業主淨利 = rs.getObject("母公司業主淨利", Long.class);
		母公司業主綜合損益 = rs.getObject("母公司業主綜合損益", Long.class);
		EPS = rs.getObject("EPS", Float.class);
		流動資產 = rs.getObject("流動資產", Long.class);
		現金及約當現金 = rs.getObject("現金及約當現金", Long.class);
		存貨 = rs.getObject("存貨", Long.class);
		預付款項 = rs.getObject("預付款項", Long.class);
		非流動資產 = rs.getObject("非流動資產", Long.class);
		備供出售金融資產 = rs.getObject("備供出售金融資產", Long.class);
		持有至到期日金融資產 = rs.getObject("持有至到期日金融資產", Long.class);
		以成本衡量之金融資產 = rs.getObject("以成本衡量之金融資產", Long.class);
		採用權益法之投資淨額 = rs.getObject("採用權益法之投資淨額", Long.class);
		固定資產 = rs.getObject("固定資產", Long.class);
		總資產 = rs.getObject("總資產", Long.class);
		流動負債 = rs.getObject("流動負債", Long.class);
		非流動負債 = rs.getObject("非流動負債", Long.class);
		總負債 = rs.getObject("總負債", Long.class);
		保留盈餘 = rs.getObject("保留盈餘", Long.class);
		股本 = rs.getObject("股本", Long.class);
		利息費用 = rs.getObject("利息費用", Long.class);
		營業現金流 = rs.getObject("營業現金流", Long.class);
		投資現金流 = rs.getObject("投資現金流", Long.class);
		融資現金流 = rs.getObject("融資現金流", Long.class);
		自由現金流 = rs.getObject("自由現金流", Long.class);

		/* 以下數值若不存在 則當成null 以方便判斷是否已經存在 */
		股東權益 = (Long) rs.getObject("股東權益");
		每股淨值 = (Float) rs.getObject("每股淨值");
		長期投資 = (Long) rs.getObject("長期投資");
		毛利率 = (Float) rs.getObject("毛利率");
		營業利益率 = (Float) rs.getObject("營業利益率");
		稅前淨利率 = (Float) rs.getObject("稅前淨利率");
		稅後淨利率 = (Float) rs.getObject("稅後淨利率");
		總資產週轉率 = (Float) rs.getObject("總資產週轉率");
		權益乘數 = (Float) rs.getObject("權益乘數");
		業外收支比重 = (Float) rs.getObject("業外收支比重");
		ROA = (Float) rs.getObject("ROA");
		ROE = (Float) rs.getObject("ROE");
		存貨周轉率 = (Float) rs.getObject("存貨周轉率");
		負債比 = (Float) rs.getObject("負債比");
		流動比 = (Float) rs.getObject("流動比");
		速動比 = (Float) rs.getObject("速動比");
		利息保障倍數 = (Float) rs.getObject("利息保障倍數");
		營業現金對流動負債比 = (Float) rs.getObject("營業現金對流動負債比");
		營業現金對負債比 = (Float) rs.getObject("營業現金對負債比");
		營業現金流對淨利比 = (Float) rs.getObject("營業現金流對淨利比");
		自由現金流對淨利比 = (Float) rs.getObject("自由現金流對淨利比");

		除息日期 = (Date) rs.getObject("除息日期");
		現金股利 = rs.getObject("現金股利", Float.class);
		除權日期 = (Date) rs.getObject("除權日期");
		盈餘配股 = rs.getObject("盈餘配股", Float.class);
		資本公積 = rs.getObject("資本公積", Float.class);
		除權除息參考價 = rs.getObject("除權除息參考價", Float.class);
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
			if (data.year == year) {
				return data;
			}
		}
		return null;
	}
}

class AnnualSupplement implements Runnable {

	static MyStatement cashflowStm;
	static MyStatement quarter4Stm;
	static MyStatement supplementStm;
	static Object lock = new Object();
	static MyDB db;

	Company company;
	QuarterlyData[] data;

	AnnualSupplement(Company company) {
		this.company = company;
	}

	public void run() {

		try {
			AnnualData[] data = AnnualData.getAllData(db, company.stockNum);
			if (data == null)
				return;

			Log.trace("補完剩餘欄位 " + company.code);
			supplementOtherField(supplementStm, company.stockNum, data);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	void supplementOtherField(MyStatement stm, int StockNum, AnnualData[] allYear) throws SQLException {

		for (AnnualData data : allYear) {
			if (data.股東權益 != null)
				continue;

			AnnualData past1Y = AnnualData.getData(allYear, data.year - 1);

			data.自由現金流 = data.營業現金流 + data.投資現金流;

			data.股東權益 = data.總資產 - data.總負債;
			if (data.股本 != 0)
				data.每股淨值 = (float) data.股東權益 / (data.股本 / 10); // 每股10元

			if (data.長期投資 == null || data.長期投資 == 0)
				data.長期投資 = data.備供出售金融資產 + data.持有至到期日金融資產 + data.以成本衡量之金融資產 + data.採用權益法之投資淨額;

			if (data.營收 != 0) {
				data.毛利率 = (float) data.毛利 / data.營收;
				data.營業利益率 = (float) data.營業利益 / data.營收;
				data.稅前淨利率 = (float) data.稅前淨利 / data.營收;
				data.稅後淨利率 = (float) data.稅後淨利 / data.營收;
			}
			if (data.稅前淨利 != 0)
				data.業外收支比重 = (float) data.業外收支 / data.稅前淨利;

			if (data.總資產 != 0) {
				data.ROA = (float) data.稅後淨利 / data.總資產;
				data.負債比 = (float) data.總負債 / data.總資產;
				data.總資產週轉率 = (float) data.營收 / data.總資產;
			}

			if (data.股東權益 != 0) {
				data.ROE = (float) data.稅後淨利 / data.股東權益;
				data.權益乘數 = (float) data.總資產 / data.股東權益;
			}

			if (data.流動負債 != 0) {
				data.流動比 = (float) data.流動資產 / data.流動負債;
				data.速動比 = (float) (data.流動資產 - data.存貨 - data.預付款項) / data.流動負債;
				data.營業現金對流動負債比 = (float) data.營業現金流 / data.流動負債;
			}

			if (data.總負債 != 0)
				data.營業現金對負債比 = (float) data.營業現金流 / data.總負債;

			if (data.稅後淨利 != 0) {
				data.營業現金流對淨利比 = (float) data.營業現金流 / data.稅後淨利;
				data.自由現金流對淨利比 = (float) data.自由現金流 / data.稅後淨利;
			}

			if (data.利息費用 != 0)
				data.利息保障倍數 = (float) (data.稅前淨利 + data.利息費用) / data.利息費用;

			if (past1Y != null) {
				float 平均存貨 = (float) (data.存貨 + past1Y.存貨) / 2;
				if (平均存貨 != 0)
					data.存貨周轉率 = (float) data.成本 / 平均存貨;
			}

			supplementImportToDB(stm, StockNum, data);
		}
	}

	void supplementImportToDB(MyStatement stm, int StockNum, AnnualData data) throws SQLException {
		synchronized (lock) {
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
			stm.setObject(data.存貨周轉率);
			stm.setObject(data.負債比);
			stm.setObject(data.流動比);
			stm.setObject(data.速動比);

			stm.setObject(data.利息保障倍數);
			stm.setObject(data.營業現金對流動負債比);
			stm.setObject(data.營業現金對負債比);
			stm.setObject(data.營業現金流對淨利比);
			stm.setObject(data.自由現金流對淨利比);

			stm.setObject(data.year);
			stm.setObject(StockNum);
			stm.addBatch();
		}
	}

	public static void calculate(MyDB myDB) throws Exception {
		db = myDB;
		Company[] companies = Company.getAllCompanies(db);

		supplementStm = new MyStatement(db.conn);
		supplementStm.setUpdateStatement("annual", "Year=? AND StockNum=?", "自由現金流", "股東權益", "每股淨值", "長期投資", "毛利率",
				"營業利益率", "稅前淨利率", "稅後淨利率", "總資產週轉率", "權益乘數", "業外收支比重", "ROA", "ROE", "存貨周轉率", "負債比", "流動比", "速動比",
				"利息保障倍數", "營業現金對流動負債比", "營業現金對負債比", "營業現金流對淨利比", "自由現金流對淨利比");
		supplementStm.setBatchSize(250);

		ExecutorService service = Executors.newFixedThreadPool(10);
		List<Future<?>> futures = new ArrayList<>();

		for (Company company : companies) {
			futures.add(service.submit(new AnnualSupplement(company)));
		}

		// wait for all tasks to complete before continuing
		for (Future<?> f : futures) {
			f.get();
		}
		service.shutdownNow();

		supplementStm.close();
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
	private static final int MAX_COLUMN = 11;

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

	/**
	 * 刪除最後取得的資料，因最後資料可能是取得尚未更新的資料，所以需要重新抓取一次
	 */
	public static void removeLatestFile() {
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
		lastfile.delete();
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
			if (td.size() != MAX_COLUMN)
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
			stm.setDecimal(idx++, divInfo[i].cashDiv); // 現金股利
			stm.setDecimal(idx++, divInfo[i].RetainedEarningsDiv); // 盈餘配股
			stm.setDecimal(idx++, divInfo[i].CapitalReserveDiv); // 資本公積
			stm.setDate(idx++, divInfo[i].exDivDate); // 除息日期
			stm.setDate(idx++, divInfo[i].exRightDate); // 除權日期
			stm.setDecimal(idx++, divInfo[i].refPrice); // 除權除息參考價

			stm.setInt(idx++, year - 1); // Year
			stm.setInt(idx++, divInfo[i].code); // StockNum

			Log.info("Import Dividend " + divInfo[i].code + " " + year + " ");
			stm.addBatch();
		}
	}

	public static void supplementDB(MyDB db, int year) throws Exception {
		Calendar cal = Calendar.getInstance();
		int currentYear = cal.get(Calendar.YEAR);

		MyStatement stm = new MyStatement(db.conn);
		stm.setUpdateStatement("annual", "Year=? AND StockNum=?", "現金股利", "盈餘配股", "資本公積", "除息日期", "除權日期", "除權除息參考價");

		for (; year <= currentYear; year++) {
			Dividend div = new Dividend(year);
			div.importToDB(stm);
		}
		stm.close();
	}
}

public class ImportAnnual implements Runnable {
	public static MyDB db;
	static MyStatement queryIFRSs;
	static MyStatement queryNoIFRSs;
	static Object lock = new Object();

	int year;
	Company company;
	Boolean isUseIFRSs;

	ImportAnnual(int year, Company company) {
		this.year = year;
		this.company = company;
		isUseIFRSs = MyDB.isUseIFRSs(Integer.parseInt(company.code), year);
	}

	/**
	 * 刪除最後取得的資料，因最後資料可能是取得尚未更新的資料，所以需要重新抓取一次
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
		lastfile.delete();
	}

	public void run() {
		Log.info("Import BasicData " + company.code + " " + year);

		try {
			if (isUseIFRSs) {
				importBasicData(queryIFRSs);
			} else {
				importBasicDataNoIFRSs(queryNoIFRSs);
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(-1);
		}
	}

	private void importBasicData(MyStatement stm) throws Exception {
		QuarterlyBasicTable income = new QuarterlyBasicTable(year, 4, company, QuarterlyBasicTable.INCOME_STATEMENT);
		QuarterlyBasicTable balance = new QuarterlyBasicTable(year, 4, company, QuarterlyBasicTable.BALANCE_SHEET);
		QuarterlyBasicTable cashflow = new QuarterlyBasicTable(year, 4, company,
				QuarterlyBasicTable.CASHFLOW_STATEMENT);

		boolean incomeResult = income.parse();
		boolean balanceResult = balance.parse();
		boolean cashflowResult = cashflow.parse();

		if (!incomeResult || !balanceResult || !cashflowResult)
			return;

		synchronized (lock) {
			stm.setObject(Integer.valueOf(year)); // Year
			stm.setObject(Integer.valueOf(company.code)); // StockNum

			stm.setObject(income.營收()); // 營收
			stm.setObject(income.成本()); // 成本
			stm.setObject(income.毛利()); // 毛利
			stm.setObject(income.營業利益()); // 營業利益
			stm.setObject(income.業外收支()); // 業外收支
			stm.setObject(income.parseLong("繼續營業單位稅前淨利（淨損）", "稅前淨利（淨損）", "繼續營業單位稅前損益", "繼續營業單位稅前純益（純損）")); // 稅前淨利
			stm.setObject(income.parseLong("繼續營業單位本期淨利（淨損）", "繼續營業單位本期稅後淨利（淨損）", "繼續營業單位本期純益（純損）")); // 稅後淨利
			stm.setObject(income.parseLong("本期綜合損益總額", "本期綜合損益總額（稅後）")); // 綜合損益
			stm.setObject(income.parseLong("母公司業主（淨利／損）", "母公司業主（淨利／淨損）")); // 母公司業主淨利
			stm.setObject(income.parseLong("母公司業主（綜合損益）")); // 母公司業主綜合損益
			stm.setObject(income.parseFloat("基本每股盈餘")); // EPS

			stm.setObject(balance.parseLong("流動資產合計")); // 流動資產
			stm.setObject(balance.parseLong("現金及約當現金")); // 現金及約當現金
			stm.setObject(balance.parseLong("存貨")); // 存貨
			stm.setObject(balance.parseLong("預付款項")); // 預付款項
			stm.setObject(balance.parseLong("非流動資產合計")); // 非流動資產
			stm.setObject(balance.parseLong("備供出售金融資產－非流動淨額", "備供出售金融資產－非流動")); // 備供出售金融資產
			stm.setObject(balance.parseLong("持有至到期日金融資產－非流動淨額", "持有至到期日金融資產－非流動")); // 持有至到期日金融資產
			stm.setObject(balance.parseLong("以成本衡量之金融資產－非流動淨額", "以成本衡量之金融資產－非流動")); // 以成本衡量之金融資產
			stm.setObject(balance.parseLong("採用權益法之投資淨額", "採用權益法之投資")); // 採用權益法之投資淨額
			stm.setObject(balance.parseLong("不動產、廠房及設備", "不動產及設備－淨額", "不動產及設備合計")); // 固定資產
			stm.setObject(balance.parseLong("資產總額", "資產總計")); // 總資產

			stm.setObject(balance.parseLong("流動負債合計")); // 流動負債
			stm.setObject(balance.parseLong("非流動負債合計")); // 非流動負債
			stm.setObject(balance.parseLong("負債總額", "負債總計")); // 總負債
			stm.setObject(balance.parseLong("保留盈餘合計")); // 保留盈餘
			stm.setObject(balance.parseLong("股本合計", "股本")); // 股本

			stm.setObject(cashflow.parseLong("利息費用")); // 利息費用
			stm.setObject(cashflow.parseLong("營業活動之淨現金流入（流出）")); // 營業現金流
			stm.setObject(cashflow.parseLong("投資活動之淨現金流入（流出）")); // 投資現金流
			stm.setObject(cashflow.parseLong("籌資活動之淨現金流入（流出）")); // 融資現金流
			stm.addBatch();
		}
	}

	private void importBasicDataNoIFRSs(MyStatement stm) throws Exception {
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

		synchronized (lock) {
			stm.setObject(Integer.valueOf(year)); // Year
			stm.setObject(Integer.valueOf(company.code)); // StockNum

			stm.setObject(income.營收()); // 營收
			stm.setObject(income.成本()); // 成本
			stm.setObject(income.毛利()); // 毛利
			stm.setObject(income.營業利益()); // 營業利益
			stm.setObject(income.業外收支()); // 業外收支
			stm.setObject(income.parseLong("繼續營業部門稅前淨利(淨損)", "繼續營業單位稅前淨利(淨損)", "繼續營業單位稅前淨益(淨損)")); // 稅前淨利
			stm.setObject(income.parseLong("繼續營業部門淨利(淨損)", "繼續營業單位淨利(淨損)", "合併總損益")); // 稅後淨利
			stm.setObject(income.parseLong("本期淨利(淨損)", "合併淨損益", "合併總損益")); // 綜合損益
			stm.setObject(income.parseFloat("普通股每股盈餘", "基本每股盈餘")); // EPS

			stm.setObject(balance.parseLong("流動資產")); // 流動資產
			stm.setObject(balance.parseLong("現金及約當現金")); // 現金及約當現金
			stm.setObject(balance.parseLong("存 貨", "存貨")); // 存貨
			stm.setObject(balance.parseLong("預付款項")); // 預付款項
			stm.setObject(balance.parseLong("基金及投資", "基金及長期投資", "基金與投資")); // 長期投資
			stm.setObject(balance.parseLong("固定資產淨額", "固定資產")); // 固定資產
			stm.setObject(balance.parseLong("資產總計", "資產", "資產合計")); // 總資產

			stm.setObject(balance.parseLong("流動負債合計", "流動負債")); // 流動負債
			stm.setObject(balance.parseLong("負債總計", "負債總額")); // 總負債
			stm.setObject(balance.parseLong("保留盈餘合計")); // 保留盈餘
			stm.setObject(balance.parseLong("普通股股本", "股 本", "股本")); // 股本
			stm.addBatch();
		}
	}

	public static void supplementBasicData(MyDB myDB, int year) throws Exception {
		Calendar cal = Calendar.getInstance();
		int currentYear = cal.get(Calendar.YEAR);

		db = myDB;
		Company[] companies = Company.getAllCompanies(db);

		queryIFRSs = new MyStatement(db.conn);
		queryIFRSs.setInsertOnDuplicateStatement("annual", "Year", "StockNum", "營收", "成本", "毛利", "營業利益", "業外收支", "稅前淨利",
				"稅後淨利", "綜合損益", "母公司業主淨利", "母公司業主綜合損益", "EPS", "流動資產", "現金及約當現金", "存貨", "預付款項", "非流動資產", "備供出售金融資產",
				"持有至到期日金融資產", "以成本衡量之金融資產", "採用權益法之投資淨額", "固定資產", "總資產", "流動負債", "非流動負債", "總負債", "保留盈餘", "股本", "利息費用",
				"營業現金流", "投資現金流", "融資現金流");
		queryIFRSs.setBatchSize(250);

		queryNoIFRSs = new MyStatement(db.conn);
		queryNoIFRSs.setInsertOnDuplicateStatement("annual", "Year", "StockNum", "營收", "成本", "毛利", "營業利益", "業外收支",
				"稅前淨利", "稅後淨利", "綜合損益", "EPS", "流動資產", "現金及約當現金", "存貨", "預付款項", "長期投資", "固定資產", "總資產", "流動負債", "總負債",
				"保留盈餘", "股本");
		queryNoIFRSs.setBatchSize(250);

		ExecutorService service = Executors.newFixedThreadPool(10);
		List<Future<?>> futures = new ArrayList<>();

		for (; year < currentYear; year++) {
			for (Company company : companies) {
				// skip no data stocks
				int stockNum = company.stockNum;

				if (company.category == null || stockNum < 1000 || stockNum > 9999) {
					Log.info(company.code + " skipped: invalid stock");
					continue;
				}

				if (!company.isValidYear(year)) {
					Log.info(company.code + " skipped: 已下市");
					continue;
				}

				if (company.isFinancial() && year < 2013) {
					Log.info(company.code + " skipped: 金融保險業");
					continue;
				}

				if (stockNum == 2905 || stockNum == 2514 || stockNum == 1409 || stockNum == 1718) {
					Log.info(company.code + " Skipped: 表格格式與人不同");
					continue;
				}

				if (stockNum == 2841 && year < 2006) {
					Log.info(company.code + " Skipped: 表格內容殘缺");
					continue;
				}

				futures.add(service.submit(new ImportAnnual(year, company)));
			}
		}

		// wait for all tasks to complete before continuing
		for (Future<?> f : futures) {
			f.get();
		}
		service.shutdownNow();

		queryNoIFRSs.close();
		queryIFRSs.close();
	}

	public static void main(String[] args) {

		try {
			MyDB db = new MyDB();
			int year = db.getLastAnnualRevenue();
			supplementBasicData(db, year);

			Dividend.removeLatestFile();
			Dividend.supplementDB(db, year + 1);

			AnnualSupplement.calculate(db);
			Log.info("Done");
			db.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
