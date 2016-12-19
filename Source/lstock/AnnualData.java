package lstock;

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class AnnualData {
	@SuppressWarnings("unused")
	private static final Logger log = LogManager.getLogger(AnnualData.class.getName());
	
	static final String BASIC_QUERY = "Year,StockNum,營收,成本,毛利,營業利益,業外收支,"
			+ "稅前淨利,稅後淨利,綜合損益,母公司業主淨利,母公司業主綜合損益,EPS,流動資產,現金及約當現金,存貨,"
			+ "預付款項,非流動資產,長期投資,固定資產,總資產,流動負債,非流動負債,總負債,保留盈餘,股本,利息費用,營業現金流,"
			+ "投資現金流,融資現金流,自由現金流,淨現金流,年報已計算";
	
	static final String CALC_REF_QUERY = "股東權益,每股淨值,毛利率,營業利益率,稅前淨利率,稅後淨利率,總資產週轉率,"
			+ "權益乘數,業外收支比重,ROA,ROE,存貨周轉率,負債比,流動比,速動比,利息保障倍數,營業現金對流動負債比,"
			+ "營業現金對負債比,營業現金流對淨利比,自由現金流對淨利比";
	
	static final String DIVIDEND_QUERY = "除息日期,除息參考價,現金盈餘,現金公積,除權日期,除權參考價,股票盈餘,股票公積";
	
	public static final int DATA_CALC_REF			= 1 << 0;
	public static final int DATA_DIVIDEND			= 1 << 1;
	
	Integer year;
	Integer stockNum;
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

	boolean 年報已計算;
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
	Date 除息日期;
	float 除息參考價;
	float 現金盈餘;
	float 現金公積;
	Date 除權日期;
	float 除權參考價;
	float 股票盈餘;
	float 股票公積;
	
	AnnualData(ResultSet rs, int flags) throws SQLException {

		year = (Integer) rs.getObject("Year", Integer.class);
		stockNum = (Integer) rs.getObject("StockNum", Integer.class);
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
		年報已計算 = rs.getObject("年報已計算", Boolean.class);

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
		}

		if (0 != (flags & DATA_DIVIDEND)) {
			除息日期 = (Date) rs.getObject("除息日期");
			除息參考價 = rs.getObject("現金股利", Float.class);
			現金盈餘 = rs.getObject("現金股利", Float.class);
			現金公積 = rs.getObject("現金股利", Float.class);
			除權日期 = (Date) rs.getObject("除權日期");
			除權參考價 =  rs.getObject("現金股利", Float.class);
			股票盈餘 = rs.getObject("盈餘配股", Float.class);
			股票公積 = rs.getObject("資本公積", Float.class);
		}
	}
	
	public static AnnualData[] getAllCalcRefData(MyDB db, int stockNum) throws SQLException {
		Statement stm = db.conn.createStatement();
		ResultSet rs = stm.executeQuery(String.format("SELECT "
				+ BASIC_QUERY + "," + CALC_REF_QUERY
				+ " FROM annual WHERE StockNum=%s ORDER BY Year", stockNum));
		int numRow = MyDB.getNumRow(rs);
		if (numRow == 0)
			return null;

		AnnualData[] data = new AnnualData[numRow];
		int iRow = 0;
		while (rs.next()) {
			data[iRow++] = new AnnualData(rs, DATA_CALC_REF);
		}

		stm.close();
		
		int lastIdx = data.length - 1;
		int yearCount = data[lastIdx].year - data[0].year + 1;
		
		AnnualData[] allYData = new AnnualData[yearCount];
		int j = 0;
		int year = data[0].year;
		for (int i = 0; i < yearCount; i++) {
			if (year == data[j].year) {
				allYData[i] = data[j];
				j++;
			} else {
				allYData[i] = null;
			}
			
			year++;
		}
		
		return allYData;
	}
}
