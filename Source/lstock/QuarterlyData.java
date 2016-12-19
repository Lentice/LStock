package lstock;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Calendar;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class QuarterlyData {
	@SuppressWarnings("unused")
	private static final Logger log = LogManager.getLogger(QuarterlyData.class.getName());
	
	static final String BASIC_QUERY = "YearQuarter,StockNum,營收,成本,毛利,營業利益,業外收支,"
			+ "稅前淨利,稅後淨利,綜合損益,母公司業主淨利,母公司業主綜合損益,EPS,流動資產,現金及約當現金,存貨,"
			+ "預付款項,非流動資產,長期投資,固定資產,總資產,流動負債,非流動負債,總負債,保留盈餘,股本,利息費用,營業現金流,"
			+ "投資現金流,融資現金流,自由現金流,淨現金流,季報已計算,季報有缺少";
	
	static final String CALC_REF_QUERY = "股東權益,每股淨值,毛利率,營業利益率,稅前淨利率,稅後淨利率,總資產週轉率,"
			+ "權益乘數,業外收支比重,ROA,ROE,存貨周轉率,負債比,流動比,速動比,利息保障倍數,營業現金對流動負債比,"
			+ "營業現金對負債比,營業現金流對淨利比,自由現金流對淨利比,盈再率";
	
	static final String RECENT_QUARTERS = "單季營收年增率,單季毛利年增率,單季營業利益年增率,單季稅後淨利年增率,"
			+ "單季EPS年增率,單季總資產年增率,單季淨值年增率,單季固定資產年增率,近四季稅後淨利,近四季營業現金流,近四季營業利益率,"
			+ "近四季ROE,近四季利息保障倍數,近4季營收年增率,近4季毛利年增率,近4季營業利益年增率,近4季稅後淨利年增率,近4季EPS年增率,"
			+ "近4季總資產年增率,近4季淨值年增率,近4季固定資產年增率";
	
	public static final int DATA_CALC_REF			= 1 << 0;
	public static final int DATA_RECENT_QUARTERS	= 1 << 1;
	
	int yearQuarter;
	int stockNum;
	Long 營收;
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
	long 長期投資;
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
	long 淨現金流;
	
	boolean 季報已計算;
	boolean 季報有缺少;
	Long 股東權益;
	Float 每股淨值;
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
	Float 盈再率;

	Long 近四季稅後淨利;
	Long 近四季營業現金流;
	Float 近四季營業利益率;
	Float 近四季ROE;
	Float 近四季利息保障倍數;
	Float 單季營收年增率;
	Float 近4季營收年增率;
	Float 單季毛利年增率;
	Float 近4季毛利年增率;
	Float 單季營業利益年增率;
	Float 近4季營業利益年增率;
	Float 單季稅後淨利年增率;
	Float 近4季稅後淨利年增率;
	Float 單季EPS年增率;
	Float 近4季EPS年增率;
	Float 單季總資產年增率;
	Float 近4季總資產年增率;
	Float 單季淨值年增率;
	Float 近4季淨值年增率;
	Float 單季固定資產年增率;
	Float 近4季固定資產年增率;
	
	QuarterlyData(ResultSet rs, int flags) throws SQLException {
		
		yearQuarter = rs.getObject("YearQuarter", Integer.class);
		stockNum = rs.getObject("StockNum", Integer.class);
		營收 = (Long) rs.getObject("營收");
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
		長期投資 = rs.getObject("長期投資", Long.class);
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
		淨現金流 = rs.getObject("淨現金流", Long.class);
		季報已計算 = rs.getObject("季報已計算", Boolean.class);
		季報有缺少 = rs.getObject("季報有缺少", Boolean.class);

		if (0 != (flags & DATA_CALC_REF)) {
			/* 以下數值若不存在 則當成null 以方便判斷是否已經存在 */
			股東權益 = (Long) rs.getObject("股東權益");
			每股淨值 = (Float) rs.getObject("每股淨值");
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
			盈再率 = (Float) rs.getObject("盈再率");
		}

		if (0 != (flags & DATA_RECENT_QUARTERS)) {
			單季營收年增率 = (Float) rs.getObject("單季營收年增率");
			單季毛利年增率 = (Float) rs.getObject("單季毛利年增率");
			單季營業利益年增率 = (Float) rs.getObject("單季營業利益年增率");
			單季稅後淨利年增率 = (Float) rs.getObject("單季稅後淨利年增率");
			單季EPS年增率 = (Float) rs.getObject("單季EPS年增率");
			單季總資產年增率 = (Float) rs.getObject("單季總資產年增率");
			單季淨值年增率 = (Float) rs.getObject("單季淨值年增率");
			單季固定資產年增率 = (Float) rs.getObject("單季固定資產年增率");
			近四季稅後淨利 = (Long) rs.getObject("近四季稅後淨利");
			近四季營業現金流 = (Long) rs.getObject("近四季營業現金流");
			近四季營業利益率 = (Float) rs.getObject("近四季營業利益率");
			近四季ROE = (Float) rs.getObject("近四季ROE");
			近四季利息保障倍數 = (Float) rs.getObject("近四季利息保障倍數");
			近4季營收年增率 = (Float) rs.getObject("近4季營收年增率");
			近4季毛利年增率 = (Float) rs.getObject("近4季毛利年增率");
			近4季營業利益年增率 = (Float) rs.getObject("近4季營業利益年增率");
			近4季稅後淨利年增率 = (Float) rs.getObject("近4季稅後淨利年增率");
			近4季EPS年增率 = (Float) rs.getObject("近4季EPS年增率");
			近4季總資產年增率 = (Float) rs.getObject("近4季總資產年增率");
			近4季淨值年增率 = (Float) rs.getObject("近4季淨值年增率");
			近4季固定資產年增率 = (Float) rs.getObject("近4季固定資產年增率");
		}
	}
	
	public static int[] getLastAvalibleQuarter() {
		int[] yearQuarter = new int[2];
	
		Calendar endCal = Calendar.getInstance();
		int month = endCal.get(Calendar.MONTH) + 1;
		int day = endCal.get(Calendar.DATE);
		int year = endCal.get(Calendar.YEAR);
		int quarter = 0;
	
		if (month > 11 || (month == 11 && day > 14)) {
			quarter = 3;
		} else if (month > 8 || (month == 8 && day > 14)) {
			quarter = 2;
		} else if (month > 5 || (month == 5 && day > 15)) {
			quarter = 1;
		} else if (month > 3) {
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
	
	public static QuarterlyData[] getAllCalcRefData(MyDB db, int stockNum) throws SQLException {
		Statement stm = db.conn.createStatement();
		ResultSet rs = stm.executeQuery(String.format("SELECT "
				+ BASIC_QUERY + "," + CALC_REF_QUERY
				+ " FROM quarterly WHERE StockNum=%s ORDER BY YearQuarter", stockNum));
		int numRow = MyDB.getNumRow(rs);
		if (numRow == 0)
			return null;

		QuarterlyData[] data = new QuarterlyData[numRow];
		int iRow = 0;
		while (rs.next()) {
			data[iRow++] = new QuarterlyData(rs, 0);
		}

		stm.close();
		
		int lastIdx = data.length - 1;
		int firstQ = data[0].yearQuarter / 100 * 4 + ((data[0].yearQuarter - 1) % 4);
		int lastQ = data[lastIdx].yearQuarter / 100 * 4 + ((data[lastIdx].yearQuarter - 1) % 4) ;
		int quarterCount = lastQ - firstQ + 1;
		
		QuarterlyData[] allQData = new QuarterlyData[quarterCount];
		int j = 0;
		int yearQuarter = data[0].yearQuarter;
		for (int i = 0; i < quarterCount; i++) {
			if (yearQuarter == data[j].yearQuarter) {
				allQData[i] = data[j];
				j++;
			} else {
				allQData[i] = null;
			}
			
			if (++yearQuarter % 100 == 5) {
				yearQuarter = yearQuarter - 4 + 100; 
			}
		}
		
		return allQData;
	}
	
}
