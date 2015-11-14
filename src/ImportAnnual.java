import java.io.File;
import java.sql.Date;
import java.util.Calendar;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;



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
		
		for (int i = 0; i < divInfo.length; i ++) {
			if (divInfo[i] == null)
				continue;

			int idx = 1;
			stm.setInt(idx++, year); // Year
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

	private static void importBasicData(MyStatement stm, int year, CompanyInfo company) throws Exception {
		QuarterlyBasicTable income = new QuarterlyBasicTable(year, 4, company, QuarterlyBasicTable.INCOME_STATEMENT);
		QuarterlyBasicTable balance = new QuarterlyBasicTable(year, 4, company, QuarterlyBasicTable.BALANCE_SHEET);
		QuarterlyBasicTable cashflow = new QuarterlyBasicTable(year, 4, company, QuarterlyBasicTable.CASHFLOW_STATEMENT);

		income.parse(); 
		balance.parse();
		cashflow.parse();

		int idx = 1;
		stm.setInt(idx++, year); // Year
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
		stm.addBatch();
	}

	public static void supplementBasicData(int year) throws Exception {
		Calendar cal = Calendar.getInstance();
		int currentYear = cal.get(Calendar.YEAR);

		CompanyInfo[] company = db.getCompanyInfo();

		MyStatement annualST = new MyStatement(db.conn);
		annualST.setInsertOnDuplicateStatement("annual", "Year", "StockNum", "營收", "成本", "毛利", "研究發展費用", "營業利益", "業外收支",
				"稅前淨利", "稅後淨利", "綜合損益", "母公司業主淨利", "母公司業主綜合損益", "EPS", "流動資產", "非流動資產", "總資產", "流動負債", "非流動負債", "總負債",
				"保留盈餘", "股本", "營業現金流", "投資現金流", "融資現金流");

		for (; year < currentYear; year++) {
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

				if (!isValidYear(year, lastUpdate))
					continue;

				if (category.compareTo("金融保險業") == 0 || stockNum == 2905 || stockNum == 2514 || stockNum == 1409
						|| stockNum == 1718) {
					// Log.info("Skip 金融保險業");
					continue;
				}

				Boolean isUseIFRSs = MyDB.isUseIFRSs(Integer.parseInt(code), year);
				// 第四季數據是全年合併 無法直接取得
				if (isUseIFRSs) {
					Log.info("Import BasicData " + code + " " + year);
					importBasicData(annualST, year, company[i]);
				}
			}
		}
		annualST.close();
	}

	public static void main(String[] args) {

		try {
			db = new MyDB();

			supplementBasicData(2013);
			Dividend.supplementDB(db, 2002);
			Log.info("Done");
			db.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
