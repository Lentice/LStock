package lstockv2;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class ImportQuarterly implements Runnable {
	private static final Logger log = LogManager.getLogger(ImportQuarterly.class.getName());

	private static final int MAX_DOWNLOAD_RETRY = 20;
	private static final int MAX_THREAD = 20;
	private static final long MIN_DOWNLOAD_GAP = 1000;
	private static final Downloader downloader = new Downloader(MIN_DOWNLOAD_GAP);

	private enum QuarterTableType {
		INCOME_STATEMENT, BALANCE_SHEET, CASHFLOW_STATEMENT
	}

	private int year;
	private int quarter;
	private QuarterTableType tableType;
	private Company company;
	private int stockNum;
	private MyStatement quarterlyST;
	private MyStatement annualST;
	private boolean isFinancial;
	private boolean useIFRSs;
	private String folderPath;
	private Hashtable<String, String> data;

	// TEST, TODO: Remove Me
	static HashMap<String, String> hash = new HashMap<>();

	ImportQuarterly(final Company company, final int year, final int quarter, final QuarterTableType tableType,
	        final MyStatement quarterlyST, final MyStatement annualST) {
		this.year = year;
		this.quarter = quarter;
		this.company = company;
		this.tableType = tableType;
		this.quarterlyST = quarterlyST;
		this.annualST = annualST;
		stockNum = company.stockNum;
		isFinancial = company.isFinancial();

		useIFRSs = isUseIFRSs();

		if (tableType == QuarterTableType.INCOME_STATEMENT)
			folderPath = DataPath.QUARTERLY_INCOME_STATEMENT;
		else if (tableType == QuarterTableType.BALANCE_SHEET)
			folderPath = DataPath.QUARTERLY_BALANCE_SHEET;
		else if (tableType == QuarterTableType.CASHFLOW_STATEMENT)
			folderPath = DataPath.QUARTERLY_CASHFLOW_STATEMENT;
		else
			folderPath = "UNKNOW";
	}

	private String getFilename(final boolean idv) {
		if (idv)
			return String.format("%d_%04d_%d_idv.html", stockNum, year, quarter);
		else
			return String.format("%d_%04d_%d.html", stockNum, year, quarter);
	}

	private File getFile(final boolean idv) {
		return new File(folderPath + getFilename(idv));
	}

	private boolean isUseIFRSs() {
		if (year < 2013)
			return false;

		if (year == 2013 || year == 2014) {
			if (stockNum == 1107 || stockNum == 1408 || stockNum == 1606 || stockNum == 2381 || stockNum == 2396
			        || stockNum == 2523)
				return false;
			else
				return true;
		}

		return true;
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

	private boolean noQuarterDataExist(File file) {

		if (!file.isFile() || file.length() > 1024)
			return false;

		try {
			Scanner scanner = new Scanner(file, "UTF-8");
			while (scanner.hasNextLine()) {
				final String lineFromFile = scanner.nextLine();
				if (lineFromFile.contains("此公司代號不存") || lineFromFile.contains("公司已下市")
				        || lineFromFile.contains("不繼續公開發行") || lineFromFile.contains("查無需求資料")
				        || lineFromFile.contains("查無所需資料") || lineFromFile.contains("請至合併財務報表查詢")
				        || lineFromFile.contains("無應編製合併財報之子公司") || lineFromFile.contains("第二上市（櫃）")
				        || lineFromFile.contains("第二上市(櫃)") || lineFromFile.contains("金控公司財報以合併基礎編製")) {

					scanner.close();
					return true;
				}
			}
			scanner.close();

		} catch (FileNotFoundException e) {
			e.printStackTrace();
			return false;
		}

		return false;
	}

	// idv=TRUE: 個別財報 , idv=FALSE: 合併財報
	private String getURL(final boolean idv) {
		String formAction = "";
		switch (tableType) {
		case INCOME_STATEMENT:
			if (idv) {
				formAction = "/mops/web/ajax_t05st32";
			} else {
				if (useIFRSs)
					formAction = "/mops/web/ajax_t164sb04";
				else
					formAction = "/mops/web/ajax_t05st34";
			}
			break;
		case BALANCE_SHEET:
			if (idv) {
				formAction = "/mops/web/ajax_t05st31";
			} else {
				if (useIFRSs)
					formAction = "/mops/web/ajax_t164sb03";
				else
					formAction = "/mops/web/ajax_t05st33";
			}
			break;
		case CASHFLOW_STATEMENT:
			if (idv) {
				formAction = "/mops/web/ajax_t05st36";
			} else {
				if (useIFRSs)
					formAction = "/mops/web/ajax_t164sb05";
				else
					formAction = "/mops/web/ajax_t05st39";
			}
			break;
		}

		final int twYear = year - 1911;
		String postData;
		if (useIFRSs && (isFinancial || stockNum == 5871 || stockNum == 2841)) {
			postData = "encodeURIComponent=1&id=&key=&TYPEK=sii&step=2&firstin=1";
		} else if (!useIFRSs && isFinancial) {
			postData = "encodeURIComponent=1&check2858=Y&firstin=1&keyword4=&TYPEK=sii&checkbtn=&firstin=1&encodeURIComponent=1&queryName=co_id&off=1&code1=&isnew=false&TYPEK2=&step=1";
		} else {
			postData = "step=1&firstin=1&off=1&keyword4=&code1=&TYPEK2=&checkbtn=&queryName=co_id&TYPEK=all&isnew=false";
		}

		try {
			postData = postData + String.format("&co_id=%d&year=%s&season=0%s", stockNum,
			        URLEncoder.encode(String.valueOf(twYear), "UTF-8"),
			        URLEncoder.encode(String.valueOf(quarter), "UTF-8"));
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
			log.warn("year=" + year + " quarter=" + quarter + " companyID=" + stockNum);
			return null;
		}

		return "http://mops.twse.com.tw" + formAction + "?" + postData;
	}

	private static boolean downloadCashflowSummary(Company company) throws Exception {
		final String folder = DataPath.CASHFLOW_SUM;
		final int MAX_RETRY = 20;
		final String url = "http://goodinfo.tw/StockInfo/StockCashFlow.asp?";

		downloader.enableProxy();
		try {
			for (int idv = 0; idv <= 1; idv++) {
				String fullFilePath, reportCat;
				if (idv == 0) {
					fullFilePath = folder + String.format("%d.html", company.stockNum);
					reportCat = "M_QUAR";
				} else {
					fullFilePath = folder + String.format("%d_idv.html", company.stockNum);
					reportCat = "QUAR";
				}

				File file = new File(fullFilePath);
				if (file.isFile() && file.length() > 4096) {
					log.debug("Cashflow file already exist: " + file.getPath());
					continue;
				}

				String getData = String.format("STOCK_ID=%d&RPT_CAT=%s", company.stockNum, reportCat);

				int retry = 0;
				while (retry < MAX_RETRY) {
					downloader.httpGet(url + getData, fullFilePath);
					log.info(String.format("Download[%d] to %s", retry, fullFilePath));
					// 10 ~ 14 seconds
					downloader.setMinDownloadGap((int) (Math.random() * 4000 + 10000));

					file = new File(fullFilePath);
					if (file.isFile() && file.length() > 1024) {
						break;
					} else {
						retry++;
						file.delete();
						downloader.nextProxy();
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		downloader.disableProxy();
		downloader.setMinDownloadGap(MIN_DOWNLOAD_GAP);
		return true;
	}

	// idv=TRUE: 個別財報 , idv=FALSE: 合併財報
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
					Thread.sleep(10000);
					continue;
				}

				file = new File(fullFilePath);
				if (downloadNeedRetry(file)) {
					// sleep 10~15sec
					Thread.sleep((int) (Math.random() * 5000 + 10000));
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

	private boolean download() {

		boolean result = true;

		if (!downloadWithRetry(false))
			result = false;

		if (!useIFRSs) {
			if (!downloadWithRetry(true))
				result = false;
		}

		return result;
	}

	private static boolean parseGoodInfoCashflowHtml(final boolean idv, final Company company,
	        HashMap<String, Float[]> quarterDataList) throws Exception {
		File file;
		if (idv)
			file = new File(DataPath.CASHFLOW_SUM + String.format("%d_idv.html", company.stockNum));
		else
			file = new File(DataPath.CASHFLOW_SUM + String.format("%d.html", company.stockNum));
		if (!file.isFile()) {
			return false;
		}

		Document doc = Jsoup.parse(file, "UTF-8");
		Elements tbody = doc.select("#divDetail > table > tbody");
		if (tbody.isEmpty())
			return false;

		Elements rows = tbody.select(":root > tr");
		Float[] cashflow; // 營業活動現金流, 投資活動現金流, 融資活動現金流
		for (int i = 0; i < rows.size(); i++) {
			Elements columns = rows.get(i).select(":root > td");
			if (columns.size() < 17)
				continue;

			String yearQuarter = HtmlUtil.getText(columns.get(0));
			if (yearQuarter == null || !yearQuarter.contains("Q"))
				continue;

			yearQuarter = yearQuarter.replace("Q", "0");
			cashflow = quarterDataList.get(yearQuarter);
			if (cashflow == null) {
				cashflow = new Float[3];
				quarterDataList.put(yearQuarter, cashflow);
			}

			Float 營業活動現金流 = HtmlUtil.getFloat(columns.get(9));
			Float 投資活動現金流 = HtmlUtil.getFloat(columns.get(10));
			Float 融資活動現金流 = HtmlUtil.getFloat(columns.get(11));
			if (cashflow[0] == null || cashflow[0] == 0)
				cashflow[0] = 營業活動現金流 == null ? null : 營業活動現金流 * 100000;
			if (cashflow[1] == null || cashflow[1] == 0)
				cashflow[1] = 投資活動現金流 == null ? null : 投資活動現金流 * 100000;
			if (cashflow[2] == null || cashflow[2] == 0)
				cashflow[2] = 融資活動現金流 == null ? null : 融資活動現金流 * 100000;

		}

		return true;
	}

	private boolean parseHtml(final boolean idv) throws Exception {
		File file = getFile(idv);
		if (!file.isFile()) {
			return false;
		}

		if (noQuarterDataExist(file))
			return false;

		Document doc = Jsoup.parse(file, "UTF-8");
		Elements checkTarget = null;
		if (tableType == QuarterTableType.INCOME_STATEMENT) {
			checkTarget = doc.getElementsContainingOwnText("費用");
		} else if (tableType == QuarterTableType.BALANCE_SHEET) {
			checkTarget = doc.getElementsContainingOwnText("權益");
		} else if (tableType == QuarterTableType.CASHFLOW_STATEMENT) {
			checkTarget = doc.getElementsContainingOwnText("本期稅前淨利");
		}

		if (checkTarget == null || checkTarget.size() == 0) {
			throw new UnsupportedOperationException(folderPath + file.getName());
		}

		Element eTemp = checkTarget.first().parent();
		while (!eTemp.tagName().equals("tbody"))
			eTemp = eTemp.parent();

		Elements rows = eTemp.children();

		for (int i = 0; i < rows.size(); i++) {
			Elements columns = rows.get(i).select(":root > td");
			if (columns.size() < 2)
				continue;

			String key = HtmlUtil.getText(columns.get(0));
			if (key == null)
				continue;

			String value = HtmlUtil.getText(columns.get(1));
			if (value == null)
				continue;

			// 去除括號，避免後面還要處理不同括號
			key = key.replaceAll("（|）|\\(|\\)", "").replaceAll("\\s+", "").replace("－", "-");
			value = value.replace(".00", "");

			if (!data.containsKey(key))
				data.put(key, value);

			// TEST, TODO: Remove Me
			if (key.contains("流動資產")) {
				hash.put(key, key);
			}
		}

		return true;
	}

	private Long parseLong(String... names) {
		for (String name : names) {
			if (data.containsKey(name)) {
				String value = data.get(name);
				if (value.length() > 0) {
					try {
						return Long.parseLong(value);
					} catch (NumberFormatException | NullPointerException e) {
						continue;
					}
				}
			}
		}
		return null;
	}

	private Float parseFloat(String... names) {
		for (String name : names) {
			if (data.containsKey(name)) {
				String value = data.get(name);
				if (value.length() > 0) {
					try {
						return Float.parseFloat(value);
					} catch (NumberFormatException | NullPointerException e) {
						continue;
					}
				}
			}
		}
		return null;
	}

	private Long 營收() {
		Long 營收 = null;
		if (useIFRSs) {
			if (company.isFinancial())
				營收 = parseLong("收益合計", "營業收入合計", "淨收益");
			else
				營收 = parseLong("營業收入合計", "收入合計");
		} else {
			if (company.isFinancial()) {
				if (company.stockNum == 2854 || company.stockNum == 2855 || company.stockNum == 2856
				        || company.stockNum == 6004 || company.stockNum == 6005 || company.stockNum == 6012)
					營收 = parseLong("營收總計");
				else if (company.stockNum == 2820 && year == 2004)
					營收 = parseLong("收益");
				else if (company.stockNum == 2820 && year == 2005)
					營收 = parseLong("收益");
				else if ((year >= 2006 && year <= 2010) && (company.stockNum == 2816 || company.stockNum == 2823
				        || company.stockNum == 2832 || company.stockNum == 2833 || company.stockNum == 2850
				        || company.stockNum == 2851 || company.stockNum == 2852 || company.stockNum == 2867)) {
					Long va;
					營收 = Long.valueOf(0);
					if ((va = parseLong("保費收入")) != null)
						營收 += va;
					if ((va = parseLong("再保佣金收入")) != null)
						營收 += va;
					if ((va = parseLong("攤回再保賠款與給付")) != null)
						營收 += va;
					if ((va = parseLong("收回保費準備")) != null)
						營收 += va;
					if ((va = parseLong("收回特別準備")) != null)
						營收 += va;
					if ((va = parseLong("收回賠款準備")) != null)
						營收 += va;
					if ((va = parseLong("收回未決賠款準備")) != null)
						營收 += va;
					if ((va = parseLong("利息收入")) != null)
						營收 += va;
					if ((va = parseLong("處分及投資利益")) != null)
						營收 += va;
					if ((va = parseLong("不動產投資利益")) != null)
						營收 += va;
					if ((va = parseLong("其他營業收入")) != null)
						營收 += va;
					if ((va = parseLong("手續費收入")) != null)
						營收 += va;
					if ((va = parseLong("金融資產評價利益")) != null)
						營收 += va;
					if ((va = parseLong("採權益法認列之投資收益")) != null)
						營收 += va;
					if ((va = parseLong("買賣票券及證券利益")) != null)
						營收 += va;
					if ((va = parseLong("分離帳戶保險商品收益")) != null)
						營收 += va;
				} else
					營收 = parseLong("營業收入合計", "營業收入", "淨收益", "收入");
			} else
				營收 = parseLong("營業收入合計", "收入");
		}

		return 營收;
	}

	private Long 成本() {
		Long v1, v2;
		Long 成本 = null;
		if (useIFRSs) {
			if (company.isFinancial()) {
				v1 = parseLong("營業成本合計", "支出及費用合計", "支出合計", "營業費用合計", "營業費用");
				v2 = parseLong("呆帳費用及保證責任準備提存");
				if (v1 != null)
					成本 = v1.longValue();
				if (成本 != null && v2 != null)
					成本 += v2.longValue();
			} else
				成本 = parseLong("營業成本合計", "支出及費用合計", "支出合計", "營業費用合計", "營業費用");
		} else {
			if (company.isFinancial()) {
				if (company.stockNum == 2854 || company.stockNum == 2855 || company.stockNum == 2856
				        || company.stockNum == 6004 || company.stockNum == 6005 || company.stockNum == 6012)
					成本 = parseLong("支出總計");
				else if (company.stockNum == 2820 && year == 2004)
					成本 = parseLong("費損");
				else if (company.stockNum == 2820 && year == 2005)
					成本 = parseLong("費損");
				else if ((year >= 2006 && year <= 2010) && (company.stockNum == 2816 || company.stockNum == 2823
				        || company.stockNum == 2832 || company.stockNum == 2833 || company.stockNum == 2850
				        || company.stockNum == 2851 || company.stockNum == 2852 || company.stockNum == 2867)) {
					Long va;
					成本 = Long.valueOf(0);
					if ((va = parseLong("再保費支出")) != null)
						成本 += va;
					if ((va = parseLong("承保費用")) != null)
						成本 += va;
					if ((va = parseLong("佣金支出")) != null)
						成本 += va;
					if ((va = parseLong("保險賠款與給付")) != null)
						成本 += va;
					if ((va = parseLong("提存保費準備")) != null)
						成本 += va;
					if ((va = parseLong("提存特別準備")) != null)
						成本 += va;
					if ((va = parseLong("提存未決賠款準備")) != null)
						成本 += va;
					if ((va = parseLong("安定基金支出")) != null)
						成本 += va;
					if ((va = parseLong("提存賠款準備")) != null)
						成本 += va;
					if ((va = parseLong("利息費用")) != null)
						成本 += va;
					if ((va = parseLong("金融資產評價損失")) != null)
						成本 += va;
					if ((va = parseLong("兌換損失")) != null)
						成本 += va;
					if ((va = parseLong("不動產投資費用及損失")) != null)
						成本 += va;
					if ((va = parseLong("金融負債評價損失")) != null)
						成本 += va;
					if ((va = parseLong("買賣票券及證券損失")) != null)
						成本 += va;
					if ((va = parseLong("其他營業成本")) != null)
						成本 += va;
					if ((va = parseLong("分離帳戶保險商品費用")) != null)
						成本 += va;
					if ((va = parseLong("處分投資損失")) != null)
						成本 += va;
				} else {
					v1 = parseLong("營業費用", "營業成本合計", "支出");
					v2 = parseLong("呆帳費用及保證責任準備提存", "呆帳費用");
					if (v1 != null)
						成本 = v1.longValue();
					if (成本 != null && v2 != null)
						成本 += v2.longValue();
				}
			} else
				成本 = parseLong("營業成本合計", "支出", "營業費用合計");
		}

		return 成本;
	}

	private Long 毛利(final Long 營收, final Long 成本) {
		Long 毛利 = null;
		if (useIFRSs) {
			毛利 = parseLong("營業毛利毛損淨額", "營業毛利毛損");
			if (毛利 == null) {
				if (營收 != null && 成本 != null)
					毛利 = 營收 - 成本;
			}
		} else {
			毛利 = parseLong("營業毛利毛損");
			if (毛利 == null) {
				if (營收 != null && 成本 != null)
					毛利 = 營收 - 成本;
			}
		}

		return 毛利;
	}

	private Long 營業利益(final Long 營收, final Long 成本) {
		Long 營業利益 = null;
		if (useIFRSs) {
			營業利益 = parseLong("營業利益損失", "營業利益");
			if (營業利益 == null) {
				if (營收 != null && 成本 != null)
					營業利益 = 營收 - 成本;
			}
		} else {
			營業利益 = parseLong("營業淨利淨損", " 營業利益損失");
			if (營業利益 == null) {
				if (營收 != null && 成本 != null)
					營業利益 = 營收 - 成本;
			}
		}

		return 營業利益;
	}

	private Long 業外收支() {
		Long 業外收支 = null;
		if (useIFRSs) {
			業外收支 = parseLong("營業外收入及支出合計", "營業外損益合計");
		} else {
			業外收支 = Long.valueOf(0);
			Long v1 = parseLong("營業外收入及利益");
			Long v2 = parseLong("營業外費用及損失");
			if (v1 != null)
				業外收支 = v1.longValue();
			if (v2 != null)
				業外收支 -= v2.longValue();
		}

		return 業外收支;
	}

	private Long 綜合損益() {

		Long 綜合損益 = null;
		if (useIFRSs) {
			綜合損益 = parseLong("本期綜合損益總額", "本期綜合損益總額稅後", "本期稅後淨利淨損");
		} else {

			if (year == 2005 && (quarter == 2 || quarter == 4) && company.isFinancial() && company.stockNum != 2820) {
				綜合損益 = parseLong("合併淨損益", "合併總損益", "本期損益淨損", "本期損益");
				if (綜合損益 == null)
					綜合損益 = parseLong("繼續營業部門稅後淨利淨損", "繼續營業部門淨利淨損", "稅後純益"); // 沒有正確的綜合損益，且EPS名稱亂填，導致綜合損益得到float數值。
			} else {
				綜合損益 = parseLong("合併總損益合併報表用", "合併總損益", "本期損益", "本期損益淨損", "合併淨損益", "本期淨利淨損", "稅後純益");
				if (綜合損益 == null)
					綜合損益 = parseLong("繼續營業部門稅後淨利淨損", "繼續營業部門淨利淨損"); // 沒有正確的綜合損益
			}
		}

		return 綜合損益;
	}

	private Float eps() {
		Float eps = null;
		if (useIFRSs) {
			eps = parseFloat("基本每股盈餘");
		} else {
			if (company.stockNum == 2801 && year == 2004 && quarter == 3) {
				eps = parseFloat("繼續營業部門淨利淨損");// EPS名稱亂填
			} else if (company.stockNum == 2801 && year == 2005 && quarter == 3) {
				eps = parseFloat("繼續營業部門淨利淨損");// EPS名稱亂填
			} else if (company.stockNum == 2834 && year == 2004 && quarter == 4) {
				eps = parseFloat("繼續營業部門淨利淨損");// EPS名稱亂填
			} else if (company.stockNum == 2834 && year == 2005 && quarter == 4) {
				eps = parseFloat("繼續營業部門淨利淨損");// EPS名稱亂填
			} else if (company.stockNum == 2837 && year == 2005 && quarter == 1) {
				eps = parseFloat("繼續營業部門淨利淨損");// EPS名稱亂填
			} else if (company.stockNum == 2812 && year == 2005 && quarter == 3) {
				eps = parseFloat("繼續營業部門淨利淨損");// EPS名稱亂填
			} else if (company.stockNum == 2836 && year == 2005 && quarter == 1) {
				eps = parseFloat("繼續營業部門淨利淨損");// EPS名稱亂填
			} else if (company.stockNum == 2383 && year == 2006 && quarter == 2) {
				eps = parseFloat("稀釋每股盈餘");// EPS名稱亂填
			} else if (company.stockNum == 6289 && year == 2012 && quarter == 2) {
				eps = parseFloat("稀釋每股盈餘");// EPS內容錯誤
			} else if (company.stockNum == 1474 && year == 2011 && quarter == 2) {
				eps = parseFloat("稀釋每股盈餘");// EPS內容錯誤
			} else if (company.stockNum == 3033 && year == 2008 && quarter == 1) {
				eps = parseFloat("稀釋每股盈餘");// EPS內容錯誤
			} else if (company.stockNum == 2102 && year == 2004 && quarter == 1) {
				eps = parseFloat("完全稀釋每股盈餘");// EPS名稱亂填
			} else if (company.stockNum == 2417 && year == 2004 && quarter == 1) {
				eps = parseFloat("完全稀釋每股盈餘");// EPS名稱亂填
			} else if (company.stockNum == 2359 && year == 2010 && quarter == 3) {
				eps = Float.valueOf((float) 0.66);// EPS不存在
			} else if (company.stockNum == 2849 && year == 2008 && quarter == 1) {
				eps = Float.valueOf((float) -0.28);// EPS不存在
			} else if (company.stockNum == 8422 && year == 2009 && quarter == 4) {
				eps = Float.valueOf((float) 1.424280462184874);// EPS不存在
			} else if (company.stockNum == 2801 && year == 2004 && quarter == 1) {
				eps = Float.valueOf((float) 0.07);// EPS不存在
			} else if (company.stockNum == 2807 && year == 2004 && quarter == 1) {
				eps = Float.valueOf((float) 0.54);// EPS不存在
			} else if (company.stockNum == 2812 && year == 2004 && quarter == 1) {
				eps = Float.valueOf((float) 0.16);// EPS不存在
			} else if (company.stockNum == 2820 && year == 2004 && quarter == 1) {
				eps = Float.valueOf((float) 0.37);// EPS不存在
			} else if (company.stockNum == 2834 && year == 2004 && quarter == 1) {
				eps = Float.valueOf((float) 0.04);// EPS不存在
			} else if (company.stockNum == 2836 && year == 2004 && quarter == 1) {
				eps = Float.valueOf((float) 0.32);// EPS不存在
			} else if (company.stockNum == 2838 && year == 2004 && quarter == 1) {
				eps = Float.valueOf((float) 0.26);// EPS不存在
			} else if (company.stockNum == 2845 && year == 2004 && quarter == 1) {
				eps = Float.valueOf((float) 1.04);// EPS不存在
			} else if (company.stockNum == 2847 && year == 2004 && quarter == 1) {
				eps = Float.valueOf((float) 0.27);// EPS不存在
			} else if (company.stockNum == 2849 && year == 2004 && quarter == 1) {
				eps = Float.valueOf((float) 0.10);// EPS不存在
			} else if (year == 2004 && company.isFinancial()) {
				eps = parseFloat("簡單每股盈餘", "普通股每股盈餘", "基本每股盈餘", "每股盈餘", "完全稀釋每股盈餘", "稀釋每股盈餘");
				if (eps == null || eps == 0 || Math.abs(eps) > 1000)
					eps = parseFloat("本期淨利淨損"); // EPS名稱亂填
				if (eps == null || Math.abs(eps) > 1000)
					eps = parseFloat("繼續營業部門淨利淨損"); // EPS名稱亂填
			} else if (year == 2005 && company.isFinancial()) {
				eps = parseFloat("簡單每股盈餘", "普通股每股盈餘", "基本每股盈餘", "每股盈餘", "完全稀釋每股盈餘", "稀釋每股盈餘");
				if (eps == null || eps == 0 || Math.abs(eps) > 1000)
					eps = parseFloat("本期淨利淨損"); // EPS名稱亂填
				if (eps == null || Math.abs(eps) > 1000)
					eps = parseFloat("繼續營業部門淨利淨損"); // EPS名稱亂填
			} else {
				eps = parseFloat("簡單每股盈餘", "普通股每股盈餘", "基本每股盈餘", "每股盈餘");
				if (eps == null || Math.abs(eps) > 1000)
					eps = parseFloat("稀釋每股盈餘", "完全稀釋每股盈餘");
			}
		}

		return eps;
	}

	private Long 長期投資() {
		Long 長期投資 = null;
		if (useIFRSs) {
			Long va;
			長期投資 = Long.valueOf(0);
			if ((va = parseLong("備供出售金融資產-非流動淨額", "備供出售金融資產-非流動")) != null)
				長期投資 += va;
			if ((va = parseLong("持有至到期日金融資產-非流動淨額", "持有至到期日金融資產-非流動")) != null)
				長期投資 += va;
			if ((va = parseLong("以成本衡量之金融資產-非流動淨額", "以成本衡量之金融資產-非流動")) != null)
				長期投資 += va;
			if ((va = parseLong("採用權益法之投資淨額", "採用權益法之投資", "採權益法之長期股權投資")) != null)
				長期投資 += va;
			if ((va = parseLong("投資性不動產淨額")) != null)
				長期投資 += va;
			if (長期投資 == 0) {
				if ((va = parseLong("投資合計")) != null)
					長期投資 += va;
			}

		} else {
			長期投資 = parseLong("基金及投資", "基金及長期投資", "基金與投資");
			if (長期投資 == null || 長期投資 == 0) {
				Long va;
				長期投資 = Long.valueOf(0);
				if ((va = parseLong("長期股權投資")) != null)
					長期投資 += va;
				if ((va = parseLong("其他長期投資")) != null)
					長期投資 += va;

			}
		}

		return 長期投資;
	}

	private Long 總負債() {
		Long 總負債 = null;
		if (useIFRSs) {
			總負債 = parseLong("負債總額", "負債總計", "負債合計");
		} else {
			總負債 = parseLong("負債總計", "負債總額", "負債合計", "負債");
			if (總負債 == null || 總負債 == 0) {
				Long va;
				總負債 = Long.valueOf(0);
				if ((va = parseLong("流動負債合計", "流動負債")) != null)
					總負債 += va;
				if ((va = parseLong("長期附息負債", "長期負債")) != null)
					總負債 += va;
				if ((va = parseLong("營業及負債準備")) != null)
					總負債 += va;
				if ((va = parseLong("其他負債")) != null)
					總負債 += va;
				if ((va = parseLong("央行及銀行同業融資")) != null)
					總負債 += va;
			}
		}

		return 總負債;
	}

	private Long 股本() {
		Long 股本 = null;
		if (useIFRSs) {
			Long va;
			股本 = Long.valueOf(0);
			if ((va = parseLong("股本合計", "股本")) != null)
				股本 += va;
			if (股本 == 0) {
				if ((va = parseLong("普通股股本")) != null)
					股本 += va;
				if ((va = parseLong("特別股股本")) != null)
					股本 += va;
				if ((va = parseLong("特別股轉換普通股權利證書")) != null)
					股本 += va;
			}
		} else {
			Long va;
			股本 = Long.valueOf(0);
			if ((va = parseLong("股 本", "股本")) != null)
				股本 += va;
			if (股本 == 0) {
				if ((va = parseLong("普通股股本", "普通股")) != null)
					股本 += va;
				if ((va = parseLong("特別股股本", "特別股")) != null)
					股本 += va;
			}

			if (股本 == 0) {
				if (company.stockNum == 5854 && year == 2009)
					股本 = Long.valueOf(54855000);
				else if (company.stockNum == 5531 && year == 2005 && quarter == 1)
					股本 = Long.valueOf(815661);
				else if (company.stockNum == 4144 && year == 2009 && quarter == 4) {
					股本 = Long.valueOf(115002);
				}
			}
		}

		return 股本;
	}

	private void importIncome(final boolean annual) throws SQLException {
		final MyStatement stm = annual ? annualST : quarterlyST;

		synchronized (stm.AcquireLock()) {

			Long 營收 = 營收();
			Long 成本 = 成本();
			if (annual)
				stm.setObject(Integer.valueOf(year)); // Year
			else
				stm.setObject(Integer.valueOf(year * 100 + quarter)); // YearQuarter
			stm.setObject(Integer.valueOf(stockNum)); // StockNum
			stm.setObject(營收); // 營收
			stm.setObject(成本); // 成本
			stm.setObject(毛利(營收, 成本)); // 毛利
			stm.setObject(營業利益(營收, 成本)); // 營業利益
			stm.setObject(業外收支()); // 業外收支
			stm.setObject(parseLong("稅前淨利淨損", "稅前純益純損", "繼續營業單位稅前損益", "繼續營業單位稅前淨利淨損", "繼續營業單位稅前淨益淨損", "繼續營業單位稅前純益純損",
			        "繼續營業單位稅前合併淨利淨損", "繼續營業部門稅前淨益淨損", "繼續營業部門稅前損益", "繼續營業部門稅前淨利淨損")); // 稅前淨利
			stm.setObject(parseLong("稅後純益", "繼續營業單位稅後純益純損", "繼續營業單位本期稅後淨利淨損", "繼續營業部門稅後損益", "繼續營業單位本期純益純損",
			        "繼續營業單位稅後淨利淨損", "繼續營業單位稅後損益", "繼續營業單位稅後合併淨利淨損", "繼續營業部門稅後淨利淨損", "列計非常損益及會計原則變動累積影響數前損益",
			        "列計非常損益及會計原則變動之累積影響數前淨利淨額", "繼續營業單位本期淨利淨損", "繼續營業部門淨利淨損", "繼續營業單位淨利損", "繼續營業單位淨利淨損")); // 稅後淨利
			stm.setObject(綜合損益()); // 綜合損益
			stm.setObject(parseLong("母公司業主淨利／損", "母公司業主淨利／淨損")); // 母公司業主淨利
			stm.setObject(parseLong("母公司業主綜合損益")); // 母公司業主綜合損益
			stm.setObject(eps()); // EPS
			if (!annual)
				stm.setObject((quarter == 4) ? Boolean.TRUE : Boolean.FALSE); // 第四季損益需更正
			stm.addBatch();
		}
	}

	private void importBalance(final boolean annual) throws SQLException {
		final MyStatement stm = annual ? annualST : quarterlyST;

		synchronized (stm.AcquireLock()) {
			if (annual)
				stm.setObject(Integer.valueOf(year)); // Year
			else
				stm.setObject(Integer.valueOf(year * 100 + quarter)); // YearQuarter
			stm.setObject(Integer.valueOf(stockNum)); // StockNum
			stm.setObject(parseLong("流動資產合計", "流動資產總額", "流動資產總計", "流動資產")); // 流動資產
			stm.setObject(parseLong("現金及約當現金")); // 現金及約當現金
			stm.setObject(parseLong("存 貨", "存貨")); // 存貨
			stm.setObject(parseLong("預付款項")); // 預付款項
			stm.setObject(parseLong("非流動資產合計", "非流動資產總額", "非流動資產總計", "非流動資產")); // 非流動資產
			stm.setObject(長期投資()); // 長期投資
			stm.setObject(parseLong("不動產、廠房及設備", "不動產及設備-淨額", "不動產及設備合計", "不動產、廠房及設備淨額", "固定資產淨額", "固定資產合計", "固定資產")); // 固定資產
			stm.setObject(parseLong("資產總額", "資產總計", "資產合計", "資產")); // 總資產

			stm.setObject(parseLong("流動負債合計", "流動負債總額", "流動負債總計", "流動負債")); // 流動負債
			stm.setObject(parseLong("非流動負債合計", "非流動負債總額", "非流動負債總計")); // 非流動負債
			stm.setObject(總負債()); // 總負債
			stm.setObject(parseLong("保留盈餘合計", "保留盈餘總額", "保留盈餘總計", "保留盈餘")); // 保留盈餘
			stm.setObject(股本()); // 股本
			stm.addBatch();
		}
	}

	private void importCashflow(final boolean annual) throws SQLException {
		final MyStatement stm = annual ? annualST : quarterlyST;

		synchronized (stm.AcquireLock()) {
			Long 營業現金流 = parseLong("營業活動之淨現金流入流出");
			Long 投資現金流 = parseLong("投資活動之淨現金流入流出");
			Long 融資現金流 = parseLong("籌資活動之淨現金流入流出");
			Long 自由現金流 = null;
			Long 淨現金流 = null;
			if (營業現金流 != null && 投資現金流 != null) {
				自由現金流 = 營業現金流 - 投資現金流;
				if (融資現金流 != null)
					淨現金流 = 自由現金流 + 融資現金流;
			}

			if (annual)
				stm.setObject(Integer.valueOf(year)); // Year
			else
				stm.setObject(Integer.valueOf(year * 100 + quarter)); // YearQuarter
			stm.setObject(Integer.valueOf(stockNum)); // StockNum
			stm.setObject(parseLong("利息費用")); // 利息費用
			stm.setObject(營業現金流); // 營業現金流
			stm.setObject(投資現金流); // 投資現金流
			stm.setObject(融資現金流); // 融資現金流
			stm.setObject(自由現金流); // 自由現金流
			stm.setObject(淨現金流); // 淨現金流
			if (!annual)
				stm.setObject(Boolean.TRUE); // 現金流累計需更正
			stm.addBatch();
		}
	}

	public void run() {
		try {
			data = new Hashtable<String, String>();

			if (tableType == QuarterTableType.CASHFLOW_STATEMENT && !useIFRSs) {
				// parseCashflowHtml(false);
				// parseCashflowHtml(true); // parse idv data last.
			} else {
				parseHtml(false);
				parseHtml(true); // parse idv data last.
			}

			if (data.size() == 0)
				return;

			if (tableType == QuarterTableType.INCOME_STATEMENT) {
				importIncome(false); // import quarterly
				if (quarter == 4)
					importIncome(true); // import annual
			} else if (tableType == QuarterTableType.BALANCE_SHEET) {
				importBalance(false); // import quarterly
				if (quarter == 4)
					importBalance(true); // import annual
			} else if (tableType == QuarterTableType.CASHFLOW_STATEMENT) {
				importCashflow(false); // import quarterly
				if (quarter == 4)
					importCashflow(true); // import annual
			}

		} catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}

	private static int[] getCurrentQuarter() {
		int[] yearQuarter = new int[2];

		Calendar endCal = Calendar.getInstance();
		int month = endCal.get(Calendar.MONTH) + 1;
		int day = endCal.get(Calendar.DATE);
		int year = endCal.get(Calendar.YEAR);
		int quarter = 0;

		if (month > 11 || (month == 11 && day > 29)) {
			quarter = 3;
		} else if (month > 8 || (month == 8 && day > 31)) {
			quarter = 2;
		} else if (month > 5 || (month == 5 && day > 30)) {
			quarter = 1;
		} else if (month > 3 || (month == 3 && day > 31)) {
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

	public static void supplementDB(MyDB db) throws Exception {
		// create folders if they were not exist
		new File(DataPath.QUARTERLY_INCOME_STATEMENT).mkdirs();
		new File(DataPath.QUARTERLY_BALANCE_SHEET).mkdirs();
		new File(DataPath.QUARTERLY_CASHFLOW_STATEMENT).mkdirs();

		MyStatement stQuarterlyIncome = new MyStatement(db.conn);
		MyStatement stQuarterlyBalance = new MyStatement(db.conn);
		MyStatement stQuarterlyCashflow = new MyStatement(db.conn);
		MyStatement stAnnualIncome = new MyStatement(db.conn);
		MyStatement stAnnualBalance = new MyStatement(db.conn);
		MyStatement stAnnualCashflow = new MyStatement(db.conn);

		stQuarterlyIncome.setStatementInsertAndUpdate("quarterly", "YearQuarter", "StockNum", "營收", "成本", "毛利", "營業利益",
		        "業外收支", "稅前淨利", "稅後淨利", "綜合損益", "母公司業主淨利", "母公司業主綜合損益", "EPS", "第四季損益需更正");

		stAnnualIncome.setStatementInsertAndUpdate("annual", "Year", "StockNum", "營收", "成本", "毛利", "營業利益", "業外收支",
		        "稅前淨利", "稅後淨利", "綜合損益", "母公司業主淨利", "母公司業主綜合損益", "EPS");

		stQuarterlyBalance.setStatementInsertAndUpdate("quarterly", "YearQuarter", "StockNum", "流動資產", "現金及約當現金", "存貨",
		        "預付款項", "非流動資產", "長期投資", "固定資產", "總資產", "流動負債", "非流動負債", "總負債", "保留盈餘", "股本");

		stAnnualBalance.setStatementInsertAndUpdate("annual", "Year", "StockNum", "流動資產", "現金及約當現金", "存貨", "預付款項",
		        "非流動資產", "長期投資", "固定資產", "總資產", "流動負債", "非流動負債", "總負債", "保留盈餘", "股本");

		stQuarterlyCashflow.setStatementInsertAndUpdate("quarterly", "YearQuarter", "StockNum", "利息費用", "營業現金流",
		        "投資現金流", "融資現金流", "自由現金流", "淨現金流", "現金流累計需更正");

		stAnnualCashflow.setStatementInsertAndUpdate("annual", "Year", "StockNum", "利息費用", "營業現金流", "投資現金流", "融資現金流",
		        "自由現金流", "淨現金流");

		int[] currentQuarter = getCurrentQuarter();
		int endYear = currentQuarter[0];
		int endQuarter = currentQuarter[1];

		int yearQuarterStart = db.getLastQuarterlyRevenue();
		int startYear = yearQuarterStart / 100;
		int startQuarter = yearQuarterStart % 100;

		ExecutorService service = Executors.newFixedThreadPool(MAX_THREAD);
		List<Future<?>> futures = new ArrayList<>();

		int year = startYear;
		int quarter = startQuarter;
		Company[] companies = Company.getAllValidCompanies();
		while (year < endYear || (year == endYear && quarter <= endQuarter)) {
			for (Company company : companies) {
				log.info("匯入財務報表: " + company.stockNum + " " + year + " " + quarter);

				if (!company.isValidQuarter(year, startQuarter)) {
					log.warn(company.code + " skipped: 已下市");
					continue;
				}

				ImportQuarterly impIncome = new ImportQuarterly(company, year, quarter,
				        QuarterTableType.INCOME_STATEMENT, stQuarterlyIncome, stAnnualIncome);
				if (impIncome.download()) {
					Future<?> f = service.submit(impIncome);
					futures.add(f);
				}

				ImportQuarterly impBalance = new ImportQuarterly(company, year, quarter, QuarterTableType.BALANCE_SHEET,
				        stQuarterlyBalance, stAnnualBalance);
				if (impBalance.download()) {
					Future<?> f = service.submit(impBalance);
					futures.add(f);
				}

				ImportQuarterly impCashflow = new ImportQuarterly(company, year, quarter,
				        QuarterTableType.CASHFLOW_STATEMENT, stQuarterlyCashflow, stAnnualCashflow);
				if (impCashflow.download()) {
					Future<?> f = service.submit(impCashflow);
					futures.add(f);
				}
			}

			// Next quarter
			if (++quarter > 4) {
				quarter = 1;
				year++;
			}
		}

		// wait for all tasks to complete before continuing
		for (Future<?> f : futures) {
			f.get();
		}
		service.shutdownNow();

		// TODO: 從公司開始有股價後開始查？

		// TODO:

		stQuarterlyBalance.close();
		stQuarterlyIncome.close();
		stQuarterlyCashflow.close();
		stAnnualBalance.close();
		stAnnualIncome.close();
		stAnnualCashflow.close();

		fixIncome(db);
		fixCashflow(db);
	}

	private static int getFirstYearOfCheckItem(final MyDB db, final String checkItem, final int checkValue) {

		try {
			Statement stmt = db.conn.createStatement();
			ResultSet rs = stmt
			        .executeQuery("SELECT MIN(YearQuarter) FROM quarterly WHERE " + checkItem + " = " + checkValue);
			if (!rs.first() || rs.getInt("MIN(YearQuarter)") == 0)
				return 2004;

			int year = rs.getInt("MIN(YearQuarter)") / 100;

			log.info("Last Quarter: " + year);

			return year;
		} catch (SQLException e) {
			e.printStackTrace();
			System.exit(-1);
		}

		return 2004;
	}

	private static void fixIncome(MyDB db) throws Exception {
		MyStatement stUpdate = new MyStatement(db.conn);
		stUpdate.setStatementUpdate("quarterly", "YearQuarter=? AND StockNum=?", "營收", "成本", "毛利", "營業利益", "業外收支",
		        "稅前淨利", "稅後淨利", "綜合損益", "母公司業主淨利", "母公司業主綜合損益", "EPS", "第四季損益需更正");

		int year = getFirstYearOfCheckItem(db, "第四季損益需更正", 1);

		int[] currentQuarter = getCurrentQuarter();
		int endYear = currentQuarter[0];

		Statement stOriginal = db.conn.createStatement();
		Company[] companies = Company.getAllValidCompanies();
		for (Company company : companies) {
			log.info("修正第四季損益表 " + company.stockNum);
			String query = "SELECT YearQuarter,營收,成本,毛利,營業利益,業外收支,稅前淨利,稅後淨利,綜合損益,母公司業主淨利,母公司業主綜合損益,EPS,第四季損益需更正"
			        + " FROM quarterly WHERE " + "StockNum = " + company.stockNum + " AND "
			        + String.format("YearQuarter BETWEEN %d00 AND %d00", year, endYear + 1)
			        + " ORDER BY YearQuarter ASC ";

			ResultSet rs = stOriginal.executeQuery(query);

			long sum[] = new long[10];
			long value[] = new long[10];
			float eps, sumEps = 0;

			int dataYearOld = 0;
			while (rs.next()) {
				int dataYearQuarter = rs.getObject("YearQuarter", Integer.class);
				value[0] = rs.getObject("營收", Long.class);
				value[1] = rs.getObject("成本", Long.class);
				value[2] = rs.getObject("毛利", Long.class);
				value[3] = rs.getObject("營業利益", Long.class);
				value[4] = rs.getObject("業外收支", Long.class);
				value[5] = rs.getObject("稅前淨利", Long.class);
				value[6] = rs.getObject("稅後淨利", Long.class);
				value[7] = rs.getObject("綜合損益", Long.class);
				value[8] = rs.getObject("母公司業主淨利", Long.class);
				value[9] = rs.getObject("母公司業主綜合損益", Long.class);
				eps = rs.getObject("EPS", Float.class);
				boolean 第四季損益需更正 = rs.getObject("第四季損益需更正", Boolean.class);

				int dataYear = dataYearQuarter / 100;
				if (dataYear != dataYearOld) {
					dataYearOld = dataYear;
					for (int j = 0; j < sum.length; j++)
						sum[j] = 0;
					sumEps = 0;
				}

				if (第四季損益需更正) {
					int index = 1;
					stUpdate.setBigInt(index++, value[0] - sum[0]); // 營收
					stUpdate.setBigInt(index++, value[1] - sum[1]); // 成本
					stUpdate.setBigInt(index++, value[2] - sum[2]); // 毛利
					stUpdate.setBigInt(index++, value[3] - sum[3]); // 營業利益
					stUpdate.setBigInt(index++, value[4] - sum[4]); // 業外收支
					stUpdate.setBigInt(index++, value[5] - sum[5]); // 稅前淨利
					stUpdate.setBigInt(index++, value[6] - sum[6]); // 稅後淨利
					stUpdate.setBigInt(index++, value[7] - sum[7]); // 綜合損益
					stUpdate.setBigInt(index++, value[8] - sum[8]); // 母公司業主淨利
					stUpdate.setBigInt(index++, value[9] - sum[9]); // 母公司業主綜合損益
					stUpdate.setFloat(index++, eps - sumEps); // EPS
					stUpdate.setObject(index++, Boolean.FALSE); // 第四季損益需更正
					stUpdate.setInt(index++, dataYearQuarter); // YearQuarter
					stUpdate.setInt(index++, company.stockNum); // StockNum
					stUpdate.addBatch();
				} else {
					for (int j = 0; j < sum.length; j++) {
						sum[j] += value[j];
					}
					sumEps += eps;
				}
			}
		}

		stUpdate.close();
		stOriginal.close();
	}

	private static void fixCashflow(MyDB db) throws Exception {
		MyStatement stUpdate = new MyStatement(db.conn);
		stUpdate.setStatementUpdate("quarterly", "YearQuarter=? AND StockNum=?", "利息費用", "營業現金流", "投資現金流", "融資現金流",
		        "自由現金流", "淨現金流", "現金流累計需更正");

		int year = getFirstYearOfCheckItem(db, "現金流累計需更正", 1);

		int[] currentQuarter = getCurrentQuarter();
		int endYear = currentQuarter[0];

		Statement stOriginal = db.conn.createStatement();
		Company[] companies = Company.getAllValidCompanies();
		for (Company company : companies) {
			log.info("修正現金流量表累計: " + company.stockNum);

			String query = "SELECT YearQuarter,利息費用,營業現金流,投資現金流,融資現金流,自由現金流,淨現金流,現金流累計需更正 FROM quarterly WHERE "
			        + "StockNum = " + company.stockNum + " AND "
			        + String.format("YearQuarter BETWEEN %d00 AND %d00", year, endYear + 1)
			        + " ORDER BY YearQuarter ASC ";

			ResultSet rs = stOriginal.executeQuery(query);

			long sum[] = new long[6];
			long value[] = new long[6];

			int dataYearOld = 0;
			while (rs.next()) {
				int dataYearQuarter = rs.getObject("YearQuarter", Integer.class);
				value[0] = rs.getObject("利息費用", Long.class);
				value[1] = rs.getObject("營業現金流", Long.class);
				value[2] = rs.getObject("投資現金流", Long.class);
				value[3] = rs.getObject("融資現金流", Long.class);
				value[4] = rs.getObject("自由現金流", Long.class);
				value[5] = rs.getObject("淨現金流", Long.class);
				boolean 現金流累計需更正 = rs.getObject("現金流累計需更正", Boolean.class);

				int dataYear = dataYearQuarter / 100;
				if (dataYear != dataYearOld) {
					dataYearOld = dataYear;
					for (int j = 0; j < sum.length; j++)
						sum[j] = 0;
				}

				if (現金流累計需更正) {
					for (int j = 0; j < sum.length; j++)
						value[j] -= sum[j];
				}

				for (int j = 0; j < sum.length; j++) {
					sum[j] += value[j];
				}

				if (現金流累計需更正) {
					int index = 1;
					stUpdate.setBigInt(index++, value[0]); // 利息費用
					stUpdate.setBigInt(index++, value[1]); // 營業現金流
					stUpdate.setBigInt(index++, value[2]); // 投資現金流
					stUpdate.setBigInt(index++, value[3]); // 融資現金流
					stUpdate.setBigInt(index++, value[4]); // 自由現金流
					stUpdate.setBigInt(index++, value[5]); // 淨現金流
					stUpdate.setObject(index++, Boolean.FALSE); // 現金流累計需更正
					stUpdate.setInt(index++, dataYearQuarter); // YearQuarter
					stUpdate.setInt(index++, company.stockNum); // StockNum
					stUpdate.addBatch();
				}
			}
		}

		stUpdate.close();
		stOriginal.close();
	}

	public static void supplementOldCashflow(MyDB db) throws Exception {

		MyStatement stQuarterlyCashflow = new MyStatement(db.conn);
		MyStatement stAnnualCashflow = new MyStatement(db.conn);

		stQuarterlyCashflow.setStatementInsertAndUpdate("quarterly", "YearQuarter", "StockNum", "營業現金流", "投資現金流",
		        "融資現金流", "自由現金流", "淨現金流", "現金流累計需更正");
		stAnnualCashflow.setStatementInsertAndUpdate("annual", "Year", "StockNum", "營業現金流", "投資現金流", "融資現金流", "自由現金流",
		        "淨現金流");

		Company[] companies = Company.getAllValidCompanies();

		for (Company company : companies) {
			log.info("Import old cashflow: " + company.stockNum);

			downloadCashflowSummary(company);

			HashMap<String, Float[]> quarterDataList = new HashMap<>();
			parseGoodInfoCashflowHtml(false, company, quarterDataList);
			parseGoodInfoCashflowHtml(true, company, quarterDataList);

			int year = 2004;
			int quarter = 1;
			Long sum營業現金流 = new Long(0);
			Long sum投資現金流 = new Long(0);
			Long sum融資現金流 = new Long(0);
			while (year < 2013) {
				String key = String.format("%d%02d", year, quarter);
				Float[] cashflow = quarterDataList.get(key);
				if (cashflow != null) {
					Long 營業現金流 = cashflow[0].longValue();
					Long 投資現金流 = cashflow[1].longValue();
					Long 融資現金流 = cashflow[2].longValue();
					Long 自由現金流 = null;
					Long 淨現金流 = null;
					if (營業現金流 != null && 投資現金流 != null) {
						自由現金流 = 營業現金流 - 投資現金流;
						if (融資現金流 != null)
							淨現金流 = 自由現金流 + 融資現金流;
					}

					if (營業現金流 != null)
						sum營業現金流 += 營業現金流;
					if (投資現金流 != null)
						sum投資現金流 += 營業現金流;
					if (融資現金流 != null)
						sum融資現金流 += 營業現金流;

					if (營業現金流 != null || 投資現金流 != null || 融資現金流 != null) {
						stQuarterlyCashflow.setObject(Integer.valueOf(year * 100 + quarter)); // YearQuarter
						stQuarterlyCashflow.setObject(Integer.valueOf(company.stockNum)); // StockNum
						stQuarterlyCashflow.setObject(營業現金流); // 營業現金流
						stQuarterlyCashflow.setObject(投資現金流); // 投資現金流
						stQuarterlyCashflow.setObject(融資現金流); // 融資現金流
						stQuarterlyCashflow.setObject(自由現金流); // 自由現金流
						stQuarterlyCashflow.setObject(淨現金流); // 淨現金流
						stQuarterlyCashflow.setObject(Boolean.FALSE); // 現金流累計需更正
						stQuarterlyCashflow.addBatch();

						if (quarter == 4) {
							Long sum自由現金流 = null, sum淨現金流 = null;
							if (sum營業現金流 != null && sum投資現金流 != null) {
								sum自由現金流 = sum營業現金流 - sum投資現金流;
								if (sum融資現金流 != null)
									sum淨現金流 = sum自由現金流 + sum融資現金流;
							}

							stAnnualCashflow.setObject(Integer.valueOf(year)); // Year
							stAnnualCashflow.setObject(Integer.valueOf(company.stockNum)); // StockNum
							stAnnualCashflow.setObject(sum營業現金流); // 營業現金流
							stAnnualCashflow.setObject(sum投資現金流); // 投資現金流
							stAnnualCashflow.setObject(sum融資現金流); // 融資現金流
							stAnnualCashflow.setObject(sum自由現金流); // 自由現金流
							stAnnualCashflow.setObject(sum淨現金流); // 淨現金流
							stAnnualCashflow.addBatch();
						}
					}
				}
				if (++quarter == 5) {
					quarter = 1;
					year++;
					sum營業現金流 = new Long(0);
					sum投資現金流 = new Long(0);
					sum融資現金流 = new Long(0);
				}
			}
		}

		stQuarterlyCashflow.close();
		stAnnualCashflow.close();
	}

	public static void main(String[] args) throws Exception {
		MyDB db = new MyDB();
		supplementDB(db);

		// supplementOldCashflow(db);
		db.close();
		log.info("Done!!");

		// TEST, TODO: Remove Me
		for (Object key : hash.keySet()) {
			log.info(hash.get(key));
		}
	}
}
