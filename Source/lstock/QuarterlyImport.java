package lstock;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.sql.SQLException;
import java.util.Hashtable;
import java.util.Scanner;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import lstock.ui.RebuildUI;

public class QuarterlyImport implements Runnable {
	private static final Logger log = LogManager.getLogger(QuarterlyImport.class.getName());

	private static final int MAX_DOWNLOAD_RETRY = 20;
	private static final long MIN_DOWNLOAD_GAP = 1000;
	private static final Downloader downloader = new Downloader(MIN_DOWNLOAD_GAP);
	
	private static final Object uiLock = new Object();
	private static int totalImportCount;
	private static int oldPercentage;
	private static int importedCount;

	private enum QuarterTableType {
		INCOME, BALANCE, CASHFLOW
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


	QuarterlyImport(final Company company, final int year, final int quarter, final QuarterTableType tableType,
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

		if (tableType == QuarterTableType.INCOME)
			folderPath = DataPath.QUARTERLY_INCOME_STATEMENT;
		else if (tableType == QuarterTableType.BALANCE)
			folderPath = DataPath.QUARTERLY_BALANCE_SHEET;
		else if (tableType == QuarterTableType.CASHFLOW)
			folderPath = DataPath.QUARTERLY_CASHFLOW_STATEMENT;
		else
			folderPath = "UNKNOW";
	}

	public static int quartersBetween(int startYear, int startQuarter, int endYear, int endQuarter)
	{
		return (endYear - startYear) * 4 + endQuarter - startQuarter + 1;
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
		case INCOME:
			if (idv) {
				formAction = "/mops/web/ajax_t05st32";
			} else {
				if (useIFRSs)
					formAction = "/mops/web/ajax_t164sb04";
				else
					formAction = "/mops/web/ajax_t05st34";
			}
			break;
		case BALANCE:
			if (idv) {
				formAction = "/mops/web/ajax_t05st31";
			} else {
				if (useIFRSs)
					formAction = "/mops/web/ajax_t164sb03";
				else
					formAction = "/mops/web/ajax_t05st33";
			}
			break;
		case CASHFLOW:
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
				RebuildUI.addDownload("retry[" + iRetry + "]: " + fullFilePath);
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

	private boolean parseHtml(final boolean idv) throws Exception {
		File file = getFile(idv);
		if (!file.isFile()) {
			return false;
		}

		if (noQuarterDataExist(file))
			return false;

		Document doc = Jsoup.parse(file, "UTF-8");
		Elements checkTarget = null;
		if (tableType == QuarterTableType.INCOME) {
			checkTarget = doc.getElementsContainingOwnText("費用");
		} else if (tableType == QuarterTableType.BALANCE) {
			checkTarget = doc.getElementsContainingOwnText("權益");
		} else if (tableType == QuarterTableType.CASHFLOW) {
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

	private Long get營收() {
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
				else if (company.stockNum == 2820 && (year == 2004 || year == 2005))
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

	private Long get成本() {
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
				成本 = parseLong("營業成本合計", "支出");
		}

		return 成本;
	}

	private Long get毛利(final Long 營收, final Long 成本) {
		Long 毛利;
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

	private Long get營業利益(final Long 營收, final Long 成本) {
		Long 營業利益; 
		Long 營業費用;
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
				
				營業費用 = parseLong("營業費用合計");
				if (營業利益 != null && 營業費用 != null)
					營業利益 -= 營業費用;
			}
		}

		return 營業利益;
	}

	private Long get業外收支() {
		Long 業外收支;
		if (useIFRSs) {
			業外收支 = parseLong("營業外收入及支出合計", "營業外損益合計");
		} else {
			業外收支 = Long.valueOf(0);
			Long v1 = parseLong("營業外收入及利益", "營業外收入");
			Long v2 = parseLong("營業外費用及損失");
			if (v1 != null)
				業外收支 = v1.longValue();
			if (v2 != null)
				業外收支 -= v2.longValue();
		}

		return 業外收支;
	}

	private Long get綜合損益() {

		Long 綜合損益;
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

	private Float getEPS() {
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
			} else if (company.stockNum == 2812 && year == 2005 && quarter == 3) {
				eps = parseFloat("繼續營業部門淨利淨損");// EPS名稱亂填
			} else if (company.stockNum == 2359 && year == 2010 && quarter == 3) {
				eps = Float.valueOf((float) 0.66);// EPS不存在
			} else if (company.stockNum == 2849 && year == 2008 && quarter == 1) {
				eps = Float.valueOf((float) -0.28);// EPS不存在
			} else if (company.stockNum == 2801 && year == 2004 && quarter == 1) {
				eps = Float.valueOf((float) 0.07);// EPS不存在
			} else if (company.stockNum == 1529 && year == 2006 && quarter == 1) {
				eps = Float.valueOf((float) 0.03);// EPS不存在
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
			} else if (company.stockNum == 1438 && year == 2006 && quarter == 1) {
				eps = Float.valueOf((float) -0.16);// EPS不存在
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

	private Long get長期投資() {
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

	private Long get總負債() {
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

	private Long get股本() {
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

			Long 營收 = get營收();
			Long 成本 = get成本();
			if (annual)
				stm.setObject(Integer.valueOf(year)); // Year
			else
				stm.setObject(Integer.valueOf(year * 100 + quarter)); // YearQuarter
			stm.setObject(Integer.valueOf(stockNum)); // StockNum
			stm.setObject(營收); // 營收
			stm.setObject(成本); // 成本
			stm.setObject(get毛利(營收, 成本)); // 毛利
			stm.setObject(get營業利益(營收, 成本)); // 營業利益
			stm.setObject(get業外收支()); // 業外收支
			stm.setObject(parseLong("稅前淨利淨損", "稅前純益純損", "繼續營業單位稅前損益", "繼續營業單位稅前淨利淨損", "繼續營業單位稅前淨益淨損", "繼續營業單位稅前純益純損",
			        "繼續營業單位稅前合併淨利淨損", "繼續營業部門稅前淨益淨損", "繼續營業部門稅前損益", "繼續營業部門稅前淨利淨損")); // 稅前淨利
			stm.setObject(parseLong("稅後純益", "繼續營業單位稅後純益純損", "繼續營業單位本期稅後淨利淨損", "繼續營業部門稅後損益", "繼續營業單位本期純益純損",
			        "繼續營業單位稅後淨利淨損", "繼續營業單位稅後損益", "繼續營業單位稅後合併淨利淨損", "繼續營業部門稅後淨利淨損", "列計非常損益及會計原則變動累積影響數前損益",
			        "列計非常損益及會計原則變動之累積影響數前淨利淨額", "繼續營業單位本期淨利淨損", "繼續營業部門淨利淨損", "繼續營業單位淨利損", "繼續營業單位淨利淨損")); // 稅後淨利
			stm.setObject(get綜合損益()); // 綜合損益
			stm.setObject(parseLong("母公司業主淨利／損", "母公司業主淨利／淨損", "母公司業主", "母公司股東")); // 母公司業主淨利
			stm.setObject(parseLong("母公司業主綜合損益")); // 母公司業主綜合損益
			stm.setObject(getEPS()); // EPS
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
			stm.setObject(get長期投資()); // 長期投資
			stm.setObject(parseLong("不動產、廠房及設備", "不動產及設備-淨額", "不動產及設備合計", "不動產、廠房及設備淨額", "固定資產淨額", "固定資產合計", "固定資產")); // 固定資產
			stm.setObject(parseLong("資產總額", "資產總計", "資產合計", "資產")); // 總資產

			stm.setObject(parseLong("流動負債合計", "流動負債總額", "流動負債總計", "流動負債")); // 流動負債
			stm.setObject(parseLong("非流動負債合計", "非流動負債總額", "非流動負債總計")); // 非流動負債
			stm.setObject(get總負債()); // 總負債
			stm.setObject(parseLong("保留盈餘合計", "保留盈餘總額", "保留盈餘總計", "保留盈餘")); // 保留盈餘
			stm.setObject(get股本()); // 股本
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
		
		final String uiMessage = String.format("匯入季報: %d %04d-%02d", stockNum, year, quarter);

		try {
			RebuildUI.addProcess(uiMessage);
			if (!download())
				return;
			
			
			data = new Hashtable<String, String>();

			if (tableType == QuarterTableType.CASHFLOW && !useIFRSs) {
				// parseCashflowHtml(false);
				// parseCashflowHtml(true); // parse idv data last.
			} else {
				
				if (stockNum == 5531 && year == 2005 && quarter == 1) {
					parseHtml(true);
				} else {
					parseHtml(false);
					parseHtml(true); // parse idv data last.
				}
			}

			if (data.size() == 0)
				return;

			if (tableType == QuarterTableType.INCOME) {
				importIncome(false); // import quarterly
				if (quarter == 4)
					importIncome(true); // import annual
			} else if (tableType == QuarterTableType.BALANCE) {
				importBalance(false); // import quarterly
				if (quarter == 4)
					importBalance(true); // import annual
			} else if (tableType == QuarterTableType.CASHFLOW) {
				importCashflow(false); // import quarterly
				if (quarter == 4)
					importCashflow(true); // import annual
			}

		} catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);
		} finally {
			RebuildUI.removeProcess(uiMessage);
			synchronized (uiLock) {
				int percentage = ++importedCount * 100 / totalImportCount;
				if (oldPercentage != percentage) {
					oldPercentage = percentage;
					RebuildUI.updateProgressBar(percentage);
				}
			}
		}
	}

	public static void importToDB(MyDB db) throws Exception {
		
		int[] currentQuarter = QuarterlyData.getLastAvalibleQuarter();
		int endYear = currentQuarter[0];
		int endQuarter = currentQuarter[1];

		int yearQuarterStart = db.getLastQuarterInDB();
		int year = yearQuarterStart / 100;
		int quarter = yearQuarterStart % 100;
		
		int totalQuarters = quartersBetween(year, quarter, endYear, endQuarter);
		if (totalQuarters <= 1) {
			RebuildUI.updateProgressBar(100);
			return;
		}
		RebuildUI.updateProgressBar(0);
		oldPercentage = 0;
		importedCount = 0;
		
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


		MyThreadPool threadPool = new MyThreadPool();

		Company[] companies = Company.getAllValidCompanies();
		totalImportCount = companies.length * totalQuarters * 3;  //三種財報
		
		while (year < endYear || (year == endYear && quarter <= endQuarter)) {
			for (Company company : companies) {
				log.debug("匯入財務報表: " + company.stockNum + " " + year + " " + quarter);

				if (!company.isValidQuarter(year, quarter)) {
					synchronized (uiLock) {
						int percentage = ++importedCount * 100 / totalImportCount;
						if (oldPercentage != percentage) {
							oldPercentage = percentage;
							RebuildUI.updateProgressBar(percentage);
						}
					}
					continue;
				}

				threadPool.add(new QuarterlyImport(company, year, quarter,
				        QuarterTableType.INCOME, stQuarterlyIncome, stAnnualIncome));

				threadPool.add(new QuarterlyImport(company, year, quarter, QuarterTableType.BALANCE,
				        stQuarterlyBalance, stAnnualBalance));

				threadPool.add(new QuarterlyImport(company, year, quarter,
				        QuarterTableType.CASHFLOW, stQuarterlyCashflow, stAnnualCashflow));
			}

			// Next quarter
			if (++quarter > 4) {
				quarter = 1;
				year++;
			}
		}

		threadPool.waitFinish();

		stQuarterlyBalance.close();
		stQuarterlyIncome.close();
		stQuarterlyCashflow.close();
		stAnnualBalance.close();
		stAnnualIncome.close();
		stAnnualCashflow.close();
	}

	

	public static void main(String[] args) throws Exception {
		MyDB db = new MyDB();
		
		importToDB(db);
		db.close();
		log.info("Done!!");
	}
}
