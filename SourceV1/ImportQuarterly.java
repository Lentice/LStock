import java.io.File;
import java.io.IOException;
import java.net.URLEncoder;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;

class QuarterlyData {
	private static final String ALL_DATA = "SELECT * FROM quarterly WHERE StockNum=%s AND 總資產 > 0 ORDER BY YearQuarter";

	Integer yearQuarter;
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
	Boolean 現金流累計需更正;
	Boolean 第四季累計需修正;
	Boolean 季報有缺少;

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

	long 近四季稅後淨利;
	long 近四季營業現金流;
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

	QuarterlyData() {

	}

	QuarterlyData(ResultSet rs) throws SQLException {

		/* 以下數值若不存在 則當成0 以方便計算 */
		yearQuarter = (Integer) rs.getObject("YearQuarter", Integer.class);
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
		現金流累計需更正 = rs.getObject("現金流累計需更正", Boolean.class);
		第四季累計需修正 = rs.getObject("第四季累計需修正", Boolean.class);
		季報有缺少 = rs.getObject("季報有缺少", Boolean.class);

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

		近四季稅後淨利 = rs.getObject("近四季稅後淨利", Long.class);
		近四季營業現金流 = rs.getObject("近四季營業現金流", Long.class);
		近四季營業利益率 = (Float) rs.getObject("近四季營業利益率");
		近四季ROE = (Float) rs.getObject("近四季ROE");
		近四季利息保障倍數 = (Float) rs.getObject("近四季利息保障倍數");
		單季營收年增率 = (Float) rs.getObject("單季營收年增率");
		近4季營收年增率 = (Float) rs.getObject("近4季營收年增率");
		單季毛利年增率 = (Float) rs.getObject("單季毛利年增率");
		近4季毛利年增率 = (Float) rs.getObject("近4季毛利年增率");
		單季營業利益年增率 = (Float) rs.getObject("單季營業利益年增率");
		近4季營業利益年增率 = (Float) rs.getObject("近4季營業利益年增率");
		單季稅後淨利年增率 = (Float) rs.getObject("單季稅後淨利年增率");
		近4季稅後淨利年增率 = (Float) rs.getObject("近4季稅後淨利年增率");
		單季EPS年增率 = (Float) rs.getObject("單季EPS年增率");
		近4季EPS年增率 = (Float) rs.getObject("近4季EPS年增率");
		單季總資產年增率 = (Float) rs.getObject("單季總資產年增率");
		近4季總資產年增率 = (Float) rs.getObject("近4季總資產年增率");
		單季淨值年增率 = (Float) rs.getObject("單季淨值年增率");
		近4季淨值年增率 = (Float) rs.getObject("近4季淨值年增率");
		單季固定資產年增率 = (Float) rs.getObject("單季固定資產年增率");
		近4季固定資產年增率 = (Float) rs.getObject("近4季固定資產年增率");
	}

	public static QuarterlyData[] getAllData(MyDB db, int stockNum) throws SQLException {
		Statement stm = db.conn.createStatement();
		ResultSet rs = stm.executeQuery(String.format(ALL_DATA, stockNum));
		int numRow = getNumRow(rs);
		if (numRow == 0)
			return null;

		QuarterlyData[] allQuarter = new QuarterlyData[numRow];
		int iRow = 0;
		while (rs.next()) {
			allQuarter[iRow] = new QuarterlyData(rs);
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

	static QuarterlyData getData(QuarterlyData[] allQuarter, int year, int quarter) throws SQLException {
		for (QuarterlyData data : allQuarter) {
			if (data.yearQuarter / 100 == year && data.yearQuarter % 100 == quarter) {
				return data;
			}
		}
		return null;
	}

	static QuarterlyData getShiftData(QuarterlyData[] allQuarter, int currentYearQuarter, int shiftCount)
			throws SQLException {
		int year = currentYearQuarter / 100;
		int quarter = currentYearQuarter % 100;

		if (shiftCount >= 0) {
			for (int i = 0; i < shiftCount; i++) {
				quarter += 1;
				if (quarter >= 5) {
					year += 1;
					quarter = 1;
				}
			}
		} else {
			for (int i = shiftCount; i < 0; i++) {
				quarter -= 1;
				if (quarter == 0) {
					year -= 1;
					quarter = 4;
				}
			}
		}

		for (QuarterlyData qdata : allQuarter) {
			if (qdata.yearQuarter / 100 == year && qdata.yearQuarter % 100 == quarter) {
				return qdata;
			}
		}
		return null;
	}
}

class QuarterlyFixAndSupplement implements Runnable {

	static MyStatement cashflowStm;
	static MyStatement quarter4Stm;
	static MyStatement supplementStm;
	static Object lock = new Object();
	static MyDB db;

	Company company;
	QuarterlyData[] data;

	QuarterlyFixAndSupplement(Company company) {
		this.company = company;
	}

	public void run() {

		try {
			QuarterlyData[] data = QuarterlyData.getAllData(db, company.stockNum);
			if (data == null)
				return;

			Log.trace("更正現金流累計 " + company.code);
			cashflowCalculateAndImport(cashflowStm, company.stockNum, data);
			Log.trace("更正第4季累計 " + company.code);
			quarter4CalculateAndImport(quarter4Stm, company.stockNum, data);
			Log.trace("補完剩餘欄位 " + company.code);
			supplementOtherField(supplementStm, company.stockNum, data);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(-1);
		}
	}

	void quarter4CalculateAndImport(MyStatement stm, int StockNum, QuarterlyData[] allQuarter) throws SQLException {

		boolean lostData = false;

		int firstYear = allQuarter[0].yearQuarter / 100;

		for (QuarterlyData qdata : allQuarter) {
			int quarter = qdata.yearQuarter % 100;
			if (quarter != 4 || qdata.第四季累計需修正 == false)
				continue;

			int year = qdata.yearQuarter / 100;
			lostData = false;

			for (int i = 1; i < quarter; i++) {
				QuarterlyData qSub = QuarterlyData.getData(allQuarter, year, i);
				if (qSub == null) {
					if (year != firstYear)
						lostData = true;
					continue;
				}

				qdata.營收 -= (qSub.營收);
				qdata.成本 -= (qSub.成本);
				qdata.毛利 -= (qSub.毛利);
				qdata.營業利益 -= (qSub.營業利益);
				qdata.業外收支 -= (qSub.業外收支);
				qdata.稅前淨利 -= (qSub.稅前淨利);
				qdata.稅後淨利 -= (qSub.稅後淨利);
				qdata.綜合損益 -= (qSub.綜合損益);
				qdata.母公司業主淨利 -= (qSub.母公司業主淨利);
				qdata.母公司業主綜合損益 -= (qSub.母公司業主綜合損益);
				qdata.EPS -= (qSub.EPS);
			}

			qdata.第四季累計需修正 = false;
			qdata.季報有缺少 = lostData;

			quarter4ImportToDB(stm, StockNum, qdata);
		}
	}

	void quarter4ImportToDB(MyStatement stm, int StockNum, QuarterlyData data) throws SQLException {
		synchronized (lock) {
			stm.setObject(data.營收);
			stm.setObject(data.成本);
			stm.setObject(data.毛利);
			stm.setObject(data.營業利益);
			stm.setObject(data.業外收支);
			stm.setObject(data.稅前淨利);
			stm.setObject(data.稅後淨利);
			stm.setObject(data.綜合損益);
			stm.setObject(data.母公司業主淨利);
			stm.setObject(data.母公司業主綜合損益);
			stm.setObject(data.EPS);
			stm.setObject(data.營業現金流);
			stm.setObject(data.投資現金流);
			stm.setObject(data.融資現金流);
			stm.setObject(data.第四季累計需修正);
			stm.setObject(data.季報有缺少);
			stm.setObject(data.yearQuarter);
			stm.setObject(StockNum);
			stm.addBatch();
		}
	}

	void cashflowCalculateAndImport(MyStatement stm, int StockNum, QuarterlyData[] allQuarter) throws Exception {

		for (QuarterlyData qdata : allQuarter) {

			if (qdata.現金流累計需更正 == false)
				continue;

			int year = qdata.yearQuarter / 100;
			int quarter = qdata.yearQuarter % 100;

			for (int i = 1; i < quarter; i++) {
				QuarterlyData qSub = QuarterlyData.getData(allQuarter, year, i);
				if (qSub == null)
					continue;

				if (qSub.現金流累計需更正)
					throw new Exception();

				qdata.營業現金流 -= qSub.營業現金流;
				qdata.投資現金流 -= qSub.投資現金流;
				qdata.融資現金流 -= qSub.融資現金流;
			}

			qdata.現金流累計需更正 = false;

			cashflowImportToDB(stm, StockNum, qdata);
		}
	}

	void cashflowImportToDB(MyStatement stm, int StockNum, QuarterlyData data) throws SQLException {
		synchronized (lock) {
			stm.setObject(data.營業現金流);
			stm.setObject(data.投資現金流);
			stm.setObject(data.融資現金流);
			stm.setObject(data.現金流累計需更正);
			stm.setObject(data.yearQuarter);
			stm.setObject(StockNum);
			stm.addBatch();
		}
	}

	void supplementOtherField(MyStatement stm, int StockNum, QuarterlyData[] allQuarter) throws SQLException {
		QuarterlyData past1Q, past2Q, past3Q;
		QuarterlyData past4Q, past5Q, past6Q, past7Q;

		for (QuarterlyData data : allQuarter) {

			if (data.股東權益 != null)
				continue;

			past1Q = QuarterlyData.getShiftData(allQuarter, data.yearQuarter, -1);
			past2Q = QuarterlyData.getShiftData(allQuarter, data.yearQuarter, -2);
			past3Q = QuarterlyData.getShiftData(allQuarter, data.yearQuarter, -3);
			past4Q = QuarterlyData.getShiftData(allQuarter, data.yearQuarter, -4);
			past5Q = QuarterlyData.getShiftData(allQuarter, data.yearQuarter, -5);
			past6Q = QuarterlyData.getShiftData(allQuarter, data.yearQuarter, -6);
			past7Q = QuarterlyData.getShiftData(allQuarter, data.yearQuarter, -7);

			data.自由現金流 = data.營業現金流 + data.投資現金流;

			data.股東權益 = data.總資產 - data.總負債;
			if (data.股本 != 0)
				data.每股淨值 = (float) data.股東權益 / (data.股本 / 10); // 每股10元

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
				data.權益乘數 = (float) data.總資產 / data.股東權益;
				if (past1Q != null && past1Q.股東權益 != 0)
					data.ROE = (float) data.稅後淨利 / ((data.股東權益 + past1Q.股東權益) / 2);
				else
					data.ROE = (float) data.稅後淨利 / data.股東權益;
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

			if (past3Q != null && past2Q != null && past1Q != null) {
				long 利息費用累計 = data.利息費用 + past1Q.利息費用 + past2Q.利息費用 + past3Q.利息費用;
				long 稅前淨利累計 = data.稅前淨利 + past1Q.稅前淨利 + past2Q.稅前淨利 + past3Q.稅前淨利;
				if (利息費用累計 != 0)
					data.近四季利息保障倍數 = (float) (稅前淨利累計 + 利息費用累計) / 利息費用累計;

				data.近四季稅後淨利 = data.稅後淨利 + past1Q.稅後淨利 + past2Q.稅後淨利 + past3Q.稅後淨利;
				data.近四季營業現金流 = data.營業現金流 + past1Q.營業現金流 + past2Q.營業現金流 + past3Q.營業現金流;

				long 近四季營收 = data.營收 + past1Q.營收 + past2Q.營收 + past3Q.營收;
				long 近四季營業利益 = data.營業利益 + past1Q.營業利益 + past2Q.營業利益 + past3Q.營業利益;
				data.近四季營業利益率 = (float) 近四季營收 / 近四季營業利益;
				// if (past3Q != null && past2Q != null && past1Q != null)
				data.近四季ROE = data.ROE + past1Q.ROE + past2Q.ROE + past3Q.ROE;
			}

			if (past1Q != null) {
				float 平均存貨 = (float) (data.存貨 + past1Q.存貨) / 2;
				if (平均存貨 != 0)
					data.存貨周轉率 = (float) data.成本 / 平均存貨;
			}

			// TODO: 盈再率

			if (past4Q != null && past4Q.第四季累計需修正 == false) {
				if (past4Q.營收 != 0)
					data.單季營收年增率 = (float) data.營收 / past4Q.營收 - 1;
				if (past4Q.毛利 != 0)
					data.單季毛利年增率 = (float) data.毛利 / past4Q.毛利 - 1;
				if (past4Q.營業利益 != 0)
					data.單季營業利益年增率 = (float) data.營業利益 / past4Q.營業利益 - 1;
				if (past4Q.稅後淨利 != 0)
					data.單季稅後淨利年增率 = (float) data.稅後淨利 / past4Q.稅後淨利 - 1;
				if (past4Q.EPS != 0)
					data.單季EPS年增率 = (float) data.EPS / past4Q.EPS - 1;
				if (past4Q.總資產 != 0)
					data.單季總資產年增率 = (float) data.總資產 / past4Q.總資產 - 1;
				if (past4Q.股東權益 != 0)
					data.單季淨值年增率 = (float) data.股東權益 / past4Q.股東權益 - 1;
				if (past4Q.固定資產 != 0)
					data.單季固定資產年增率 = (float) data.固定資產 / past4Q.固定資產 - 1;
			}

			if (past7Q != null && past6Q != null && past5Q != null && past4Q != null && past3Q != null && past2Q != null
					&& past1Q != null && past7Q.第四季累計需修正 == false && past6Q.第四季累計需修正 == false
					&& past5Q.第四季累計需修正 == false && past4Q.第四季累計需修正 == false && past3Q.第四季累計需修正 == false
					&& past2Q.第四季累計需修正 == false && past1Q.第四季累計需修正 == false) {
				long past4quarter;
				long lastYearpast4quarter;
				float fpast4quarter;
				float flastYearpast4quarter;

				past4quarter = data.營收 + past1Q.營收 + past2Q.營收 + past3Q.營收;
				lastYearpast4quarter = past4Q.營收 + past5Q.營收 + past6Q.營收 + past7Q.營收;
				if (lastYearpast4quarter != 0)
					data.近4季營收年增率 = (float) past4quarter / lastYearpast4quarter - 1;

				past4quarter = data.毛利 + past1Q.毛利 + past2Q.毛利 + past3Q.毛利;
				lastYearpast4quarter = past4Q.毛利 + past5Q.毛利 + past6Q.毛利 + past7Q.毛利;
				if (lastYearpast4quarter != 0)
					data.近4季毛利年增率 = (float) past4quarter / lastYearpast4quarter - 1;

				past4quarter = data.營業利益 + past1Q.營業利益 + past2Q.營業利益 + past3Q.營業利益;
				lastYearpast4quarter = past4Q.營業利益 + past5Q.營業利益 + past6Q.營業利益 + past7Q.營業利益;
				if (lastYearpast4quarter != 0)
					data.近4季營業利益年增率 = (float) past4quarter / lastYearpast4quarter - 1;

				past4quarter = data.稅後淨利 + past1Q.稅後淨利 + past2Q.稅後淨利 + past3Q.稅後淨利;
				lastYearpast4quarter = past4Q.稅後淨利 + past5Q.稅後淨利 + past6Q.稅後淨利 + past7Q.稅後淨利;
				if (lastYearpast4quarter != 0)
					data.近4季稅後淨利年增率 = (float) past4quarter / lastYearpast4quarter - 1;

				fpast4quarter = data.EPS + past1Q.EPS + past2Q.EPS + past3Q.EPS;
				flastYearpast4quarter = past4Q.EPS + past5Q.EPS + past6Q.EPS + past7Q.EPS;
				if (flastYearpast4quarter != 0)
					data.近4季EPS年增率 = (float) fpast4quarter / flastYearpast4quarter - 1;

				past4quarter = data.總資產 + past1Q.總資產 + past2Q.總資產 + past3Q.總資產;
				lastYearpast4quarter = past4Q.總資產 + past5Q.總資產 + past6Q.總資產 + past7Q.總資產;
				if (lastYearpast4quarter != 0)
					data.近4季總資產年增率 = (float) past4quarter / lastYearpast4quarter - 1;

				fpast4quarter = data.股東權益 + past1Q.股東權益 + past2Q.股東權益 + past3Q.股東權益;
				flastYearpast4quarter = past4Q.股東權益 + past5Q.股東權益 + past6Q.股東權益 + past7Q.股東權益;
				if (flastYearpast4quarter != 0)
					data.近4季淨值年增率 = (float) fpast4quarter / flastYearpast4quarter - 1;

				fpast4quarter = data.固定資產 + past1Q.固定資產 + past2Q.固定資產 + past3Q.固定資產;
				flastYearpast4quarter = past4Q.固定資產 + past5Q.固定資產 + past6Q.固定資產 + past7Q.固定資產;
				if (flastYearpast4quarter != 0)
					data.近4季固定資產年增率 = (float) fpast4quarter / flastYearpast4quarter - 1;
			}

			supplementImportToDB(stm, StockNum, data);
		}
	}

	void supplementImportToDB(MyStatement stm, int StockNum, QuarterlyData data) throws SQLException {
		synchronized (lock) {
			stm.setObject(data.自由現金流);
			stm.setObject(data.股東權益);
			stm.setObject(data.每股淨值);
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
			// TODO: 盈再率

			stm.setObject(data.近四季稅後淨利);
			stm.setObject(data.近四季營業現金流);
			stm.setObject(data.近四季營業利益率);
			stm.setObject(data.近四季ROE);
			stm.setObject(data.近四季利息保障倍數);
			stm.setObject(data.單季營收年增率);
			stm.setObject(data.近4季營收年增率);
			stm.setObject(data.單季毛利年增率);
			stm.setObject(data.近4季毛利年增率);
			stm.setObject(data.單季營業利益年增率);
			stm.setObject(data.近4季營業利益年增率);
			stm.setObject(data.單季稅後淨利年增率);
			stm.setObject(data.近4季稅後淨利年增率);
			stm.setObject(data.單季EPS年增率);
			stm.setObject(data.近4季EPS年增率);
			stm.setObject(data.單季總資產年增率);
			stm.setObject(data.近4季總資產年增率);
			stm.setObject(data.單季淨值年增率);
			stm.setObject(data.近4季淨值年增率);
			stm.setObject(data.單季固定資產年增率);
			stm.setObject(data.近4季固定資產年增率);

			stm.setObject(data.yearQuarter);
			stm.setObject(StockNum);
			stm.addBatch();
		}
	}

	public static void calculate(MyDB myDB) throws Exception {
		db = myDB;
		Company[] companies = Company.getAllCompanies(db);

		cashflowStm = new MyStatement(db.conn);
		cashflowStm.setUpdateStatement("quarterly", "YearQuarter=? AND StockNum=?", "營業現金流", "投資現金流", "融資現金流",
				"現金流累計需更正");

		quarter4Stm = new MyStatement(db.conn);
		quarter4Stm.setUpdateStatement("quarterly", "YearQuarter=? AND StockNum=?", "營收", "成本", "毛利", "營業利益", "業外收支",
				"稅前淨利", "稅後淨利", "綜合損益", "母公司業主淨利", "母公司業主綜合損益", "EPS", "營業現金流", "投資現金流", "融資現金流", "第四季累計需修正", "季報有缺少");
		quarter4Stm.setBatchSize(250);

		supplementStm = new MyStatement(db.conn);
		supplementStm.setUpdateStatement("quarterly", "YearQuarter=? AND StockNum=?", "自由現金流", "股東權益", "每股淨值", "毛利率",
				"營業利益率", "稅前淨利率", "稅後淨利率", "總資產週轉率", "權益乘數", "業外收支比重", "ROA", "ROE", "存貨周轉率", "負債比", "流動比", "速動比",
				"利息保障倍數", "營業現金對流動負債比", "營業現金對負債比", "營業現金流對淨利比", "自由現金流對淨利比", "近四季稅後淨利", "近四季營業現金流", "近四季營業利益率",
				"近四季ROE", "近四季利息保障倍數", "單季營收年增率", "近4季營收年增率", "單季毛利年增率", "近4季毛利年增率", "單季營業利益年增率", "近4季營業利益年增率",
				"單季稅後淨利年增率", "近4季稅後淨利年增率", "單季EPS年增率", "近4季EPS年增率", "單季總資產年增率", "近4季總資產年增率", "單季淨值年增率", "近4季淨值年增率",
				"單季固定資產年增率", "近4季固定資產年增率");
		supplementStm.setBatchSize(250);

		ExecutorService service = Executors.newFixedThreadPool(10);
		List<Future<?>> futures = new ArrayList<>();

		for (Company company : companies) {
			futures.add(service.submit(new QuarterlyFixAndSupplement(company)));
		}

		// wait for all tasks to complete before continuing
		for (Future<?> f : futures) {
			f.get();
		}
		service.shutdownNow();

		cashflowStm.close();
		quarter4Stm.close();
		supplementStm.close();
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
	String formAction;

	boolean useIFRSs;
	int year;
	int quarter;
	int tableType;

	String[][] data;
	Company company;

	Long 營收;
	Long 成本;

	QuarterlyBasicTable(int year, int quarter, Company company, int tableType) throws Exception {
		if (year < 2001)
			throw new Exception("Year is earlier than 2001");

		this.year = year;
		this.quarter = quarter;
		this.company = company;
		this.tableType = tableType;

		useIFRSs = MyDB.isUseIFRSs(company.stockNum, year);

		getDownloadInfo();
		file = new File(filename);
		if (!file.exists() || !isValidQuarterlyData(filename, 1000)) {
			download();
			file = new File(filename);
		}
	}

	boolean parse() throws Exception {

		if (!file.exists()) {
			Log.warn("檔案不存在: " + file.getPath());
			return false;
		}

		Document doc = Jsoup.parse(file, "UTF-8");
		Elements eTitles = null;
		if (tableType == INCOME_STATEMENT) {
			// 此筆合併營收資料有欄位 卻無數字，應當成無合併營收處理
			if (company.stockNum == 5531 && year == 2005 && quarter == 1)
				return false;

			eTitles = doc.getElementsContainingOwnText("費用");
		} else if (tableType == INCOME_STATEMENT_IDV) {
			eTitles = doc.getElementsContainingOwnText("費用");
		} else if (tableType == BALANCE_SHEET) {
			// 此筆合併營收資料有欄位 卻無數字，應當成無合併資料處理
			if (company.stockNum == 6189 && year == 2004 && quarter == 4)
				return false;
			// 此筆合併營收資料有欄位 卻無數字，應當成無合併資料處理
			if (company.stockNum == 5531 && year == 2005 && quarter == 1)
				return false;
			// 5854 合庫 提供無效的合併報表(有欄位 卻無數字)，所以改採獨立報表
			if (company.stockNum == 5854 && year == 2009 && quarter < 4)
				return false;

			if (company.isFinancial())
				eTitles = doc.getElementsContainingOwnText("權益");
			else
				eTitles = doc.getElementsContainingOwnText("權益");
		} else if (tableType == BALANCE_SHEET_IDV) {
			if (company.isFinancial())
				eTitles = doc.getElementsContainingOwnText("權益");
			else
				eTitles = doc.getElementsContainingOwnText("權益");
		} else if (tableType == CASHFLOW_STATEMENT)
			eTitles = doc.getElementsContainingOwnText("本期稅前淨利");
		else
			return false;

		if (eTitles == null || eTitles.size() == 0) {
			Elements no = doc.select("body > center");
			String str = no.toString();

			// TODO: 處理這些
			if (str.contains("查無需求資料") || str.contains("查無所需資料"))
				return false;
			else if (str.contains("請至採IFRSs前之")) {
				Log.warn("請至採IFRSs前之 " + company.code + " " + year + "_" + quarter);
				return false;
			} else if (str.contains("不繼續公開發行")) {
				Log.warn("不繼續公開發行 " + company.code + " " + year + "_" + quarter);
				return false;
			} else if (str.contains("已下市")) {
				Log.warn("已下市 " + company.code + " " + year + "_" + quarter);
				return false;
			} else if (str.contains("無應編製合併財報")) {
				Log.warn("無應編製合併財報之子公司  " + company.code + " " + year + "_" + quarter);
				return false;
			} else if (str.contains("外國發行人免申報")) {
				Log.warn("外國發行人免申報  " + company.code + " " + year + "_" + quarter);
				return false;
			} else if (str.contains("不存在")) {
				Log.warn("不存在 " + company.code + " " + year + "_" + quarter);
				return false;
			} else if (str.contains("第二上市")) {
				Log.warn("第二上市 " + company.code + " " + year + "_" + quarter);
				return false;
			} else {
				new Exception(folderPath + filename).printStackTrace();
				// file.delete();
				System.exit(-1);
				// return false;
			}
		}

		Elements eTableRows = eTitles.first().parent().parent().children();

		data = new String[eTableRows.size()][2];
		for (int i = 0; i < eTableRows.size(); i++) {
			Elements eColumns = eTableRows.get(i).children();
			if (eColumns.size() < 2)
				continue;

			data[i][0] = HtmlParser.getText(eColumns.get(0));
			data[i][1] = HtmlParser.getText(eColumns.get(1));

			if (!useIFRSs && data[i][1] != null)
				data[i][1] = data[i][1].replace(".00", "");

		}

		return true;
	}
	
	private boolean noQuarterlyData(String filename) throws IOException {
		File file = new File(filename);

		if (!file.exists())
			return false;

		Document doc = Jsoup.parse(file, "UTF-8");
		if (doc.toString().contains("查無需求資料") || doc.toString().contains("查無所需資料")) {
			file.delete();
			return true;
		}
		
		return false;
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
		} else if (doc.toString().contains("資料庫連線時發生下述問題")) {
			file.delete();
			return false;
		}
		
		doc = Jsoup.parse(file, "MS950");
		if (doc.toString().contains("查詢過於頻繁")) {
			file.delete();
			return false;
		}

		return true;
	}

	private void getDownloadInfo() throws Exception {

		switch (tableType) {
		case BALANCE_SHEET:
			folderPath = DataPath.QUARTERLY_BALANCE_SHEET;
			filename = String.format(folderPath + "%s_%04d_%d.html", company.code, year, quarter);
			if (useIFRSs)
				formAction = "/mops/web/ajax_t164sb03";
			else
				formAction = "/mops/web/ajax_t05st33";

			break;
		case INCOME_STATEMENT:
			folderPath = DataPath.QUARTERLY_INCOME_STATEMENT;
			filename = String.format(folderPath + "%s_%04d_%d.html", company.code, year, quarter);
			if (useIFRSs)
				formAction = "/mops/web/ajax_t164sb04";
			else
				formAction = "/mops/web/ajax_t05st34";

			break;
		case CASHFLOW_STATEMENT:
			folderPath = DataPath.QUARTERLY_CASHFLOW_STATEMENT;
			filename = String.format(folderPath + "%s_%04d_%d.html", company.code, year, quarter);
			if (useIFRSs)
				formAction = "/mops/web/ajax_t164sb05";
			else
				formAction = "/mops/web/ajax_t05st39";

			break;
		case BALANCE_SHEET_IDV:
			folderPath = DataPath.QUARTERLY_BALANCE_SHEET;
			filename = String.format(folderPath + "%s_%04d_%d_idv.html", company.code, year, quarter);
			formAction = "/mops/web/ajax_t05st31";
			break;
		case INCOME_STATEMENT_IDV:
			folderPath = DataPath.QUARTERLY_INCOME_STATEMENT;
			filename = String.format(folderPath + "%s_%04d_%d_idv.html", company.code, year, quarter);
			formAction = "/mops/web/ajax_t05st32";
			break;
		case CASHFLOW_STATEMENT_IDV:
			folderPath = DataPath.QUARTERLY_CASHFLOW_STATEMENT;
			filename = String.format(folderPath + "%s_%04d_%d_idv.html", company.code, year, quarter);
			formAction = "/mops/web/ajax_t05st36";
			break;
		default:
			throw new Exception("Type is incorrect");
		}
	}

	void download() throws Exception {
		final int MAX_DOWNLOAD_RETRY = 20;

		if (!company.isValidQuarter(year, quarter)) {
			Log.info("Skip invalid stock " + company.code);
			return;
		}

		if (isValidQuarterlyData(filename, 1000)) {
			Log.info("Skip existing file " + filename);
			return;
		}

		String postData;
		if (useIFRSs && (company.isFinancial() || company.stockNum == 5871 || company.stockNum == 2841))
			postData = "encodeURIComponent=1&id=&key=&TYPEK=sii&step=2&firstin=1&";
		else if (!useIFRSs && company.isFinancial())
			postData = "encodeURIComponent=1&check2858=Y&firstin=1&keyword4=&TYPEK=sii&checkbtn=&firstin=1&encodeURIComponent=1&queryName=co_id&off=1&code1=&isnew=false&TYPEK2=&step=1&";
		else
			postData = "step=1&firstin=1&off=1&keyword4=&code1=&TYPEK2=&checkbtn=&queryName=co_id&TYPEK=all&isnew=false&";

		postData = postData + String.format("co_id=%s&year=%s&season=0%s", URLEncoder.encode(company.code, "UTF-8"),
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
			
			// no data currently
			if (dwResult > 0 && noQuarterlyData(filename))
				break;
			
		} while (dwResult < 0 || !isValidQuarterlyData(filename, 1000));
	}

	public String getData(String... names) {
		if (data == null)
			return null;

		boolean foundTitle = false;
		String tempData = null;
		for (String name : names) {
			for (int i = 0; i < data.length; i++) {
				String title = data[i][0];
				if (title == null || !title.equals(name))
					continue;

				foundTitle = true;
				if (data[i][1] != null && data[i][1].length() > 0) {
					return data[i][1];
				} else
					tempData = data[i][1];
			}
		}
		if (tempData != null)
			return tempData;

		if (foundTitle)
			return "0";
		else
			return null;
	}

	Long parseLong(String... names) {
		String temp = getData(names);
		if (temp == null)
			return null;
		else
			return Long.parseLong(temp);
	}

	Float parseFloat(String... names) {
		String temp = getData(names);
		if (temp == null)
			return null;
		else
			return Float.parseFloat(temp);
	}

	Long 營收() {
		營收 = null;
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

	Long 成本() {
		Long v1, v2;
		成本 = null;
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

	Long 毛利() {
		Long 毛利 = null;
		if (useIFRSs) {
			毛利 = parseLong("營業毛利（毛損）淨額", "營業毛利（毛損）", "營業毛利(毛損)");
			if (毛利 == null) {
				if (營收 != null && 成本 != null)
					毛利 = 營收 - 成本;
			}
		} else {
			毛利 = parseLong("營業毛利(毛損)");
			if (毛利 == null) {
				if (營收 != null && 成本 != null)
					毛利 = 營收 - 成本;
			}
		}

		return 毛利;
	}

	Long 營業利益() {
		Long 營業利益 = null;
		if (useIFRSs) {
			營業利益 = parseLong("營業利益（損失）", "營業利益");
			if (營業利益 == null) {
				if (營收 != null && 成本 != null)
					營業利益 = 營收 - 成本;
			}
		} else {
			營業利益 = parseLong("營業淨利(淨損)", " 營業利益（損失）");
			if (營業利益 == null) {
				if (營收 != null && 成本 != null)
					營業利益 = 營收 - 成本;
			}
		}

		return 營業利益;
	}

	Long 業外收支() {
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

	Long 綜合損益() {

		Long 綜合損益 = null;
		if (useIFRSs) {
			綜合損益 = parseLong("本期綜合損益總額", "本期綜合損益總額（稅後）");
		} else {

			if (year == 2005 && (quarter == 2 || quarter == 4) && company.isFinancial() && company.stockNum != 2820) {
				綜合損益 = parseLong("合併淨損益", "合併總損益", "本期損益（淨損）", "本期損益");
				if (綜合損益 == null)
					綜合損益 = parseLong("繼續營業部門稅後淨利（淨損）", "繼續營業部門淨利(淨損)", "稅後純益"); // 沒有正確的綜合損益，且EPS名稱亂填，導致綜合損益得到float數值。
			} else {
				綜合損益 = parseLong("合併總損益（合併報表用）", "合併總損益", "本期損益", "本期損益（淨損）", "合併淨損益", "本期淨利(淨損)", "本期淨利（淨損）", "稅後純益");
				if (綜合損益 == null)
					綜合損益 = parseLong("繼續營業部門稅後淨利（淨損）", "繼續營業部門淨利(淨損)"); // 沒有正確的綜合損益
			}
		}

		return 綜合損益;
	}

	Float eps() {
		Float eps = null;
		if (useIFRSs) {
			eps = parseFloat("基本每股盈餘");
		} else {
			if (company.stockNum == 2801 && year == 2004 && quarter == 3) {
				eps = parseFloat("繼續營業部門淨利（淨損）");// EPS名稱亂填
			} else if (company.stockNum == 2801 && year == 2005 && quarter == 3) {
				eps = parseFloat("繼續營業部門淨利（淨損）");// EPS名稱亂填
			} else if (company.stockNum == 2834 && year == 2004 && quarter == 4) {
				eps = parseFloat("繼續營業部門淨利（淨損）");// EPS名稱亂填
			} else if (company.stockNum == 2834 && year == 2005 && quarter == 4) {
				eps = parseFloat("繼續營業部門淨利（淨損）");// EPS名稱亂填
			} else if (company.stockNum == 2837 && year == 2005 && quarter == 1) {
				eps = parseFloat("繼續營業部門淨利（淨損）");// EPS名稱亂填
			} else if (company.stockNum == 2812 && year == 2005 && quarter == 3) {
				eps = parseFloat("繼續營業部門淨利（淨損）");// EPS名稱亂填
			} else if (company.stockNum == 2836 && year == 2005 && quarter == 1) {
				eps = parseFloat("繼續營業部門淨利（淨損）");// EPS名稱亂填
			} else if (company.stockNum == 2854 && year == 2005 && quarter == 1) {
				eps = parseFloat("每股盈餘");// EPS名稱亂填
			} else if (company.stockNum == 6166 && year == 2004 && quarter == 2) {
				eps = parseFloat("普通股每股盈餘");
			} else if (company.stockNum == 2348 && year == 2004 && quarter == 1) {
				eps = parseFloat("簡單每股盈餘");// EPS內容錯誤
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
					eps = parseFloat("本期淨利（淨損）"); // EPS名稱亂填
				if (eps == null || Math.abs(eps) > 1000)
					eps = parseFloat("繼續營業部門淨利（淨損）"); // EPS名稱亂填
			} else if (year == 2005 && company.isFinancial()) {
				eps = parseFloat("簡單每股盈餘", "普通股每股盈餘", "基本每股盈餘", "每股盈餘", "完全稀釋每股盈餘", "稀釋每股盈餘");
				if (eps == null || eps == 0 || Math.abs(eps) > 1000)
					eps = parseFloat("本期淨利（淨損）"); // EPS名稱亂填
				if (eps == null || Math.abs(eps) > 1000)
					eps = parseFloat("繼續營業部門淨利（淨損）"); // EPS名稱亂填
			} else {
				eps = parseFloat("簡單每股盈餘", "普通股每股盈餘", "基本每股盈餘", "每股盈餘");
				if (eps == null || Math.abs(eps) > 1000)
					eps = parseFloat("稀釋每股盈餘", "完全稀釋每股盈餘");
			}
		}

		return eps;
	}

	Long 股本() {
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

	Long 總負債() {
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

	Long 長期投資() {
		Long 長期投資 = null;
		if (useIFRSs) {
			Long va;
			長期投資 = Long.valueOf(0);
			if ((va = parseLong("備供出售金融資產－非流動淨額", "備供出售金融資產－非流動")) != null)
				長期投資 += va;
			if ((va = parseLong("持有至到期日金融資產－非流動淨額", "持有至到期日金融資產－非流動")) != null)
				長期投資 += va;
			if ((va = parseLong("以成本衡量之金融資產－非流動淨額", "以成本衡量之金融資產－非流動")) != null)
				長期投資 += va;
			if ((va = parseLong("採用權益法之投資淨額", "採用權益法之投資", "採權益法之長期股權投資")) != null)
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
}

public class ImportQuarterly implements Runnable {
	public static MyDB db;
	static MyStatement queryIFRSs;
	static MyStatement queryNoIFRSs;
	static Object lock = new Object();

	int year;
	int quarter;
	Company company;
	Boolean isUseIFRSs;

	ImportQuarterly(int year, int quarter, Company company) {
		this.year = year;
		this.quarter = quarter;
		this.company = company;
		isUseIFRSs = MyDB.isUseIFRSs(Integer.parseInt(company.code), year);
	}

	public void run() {
		Log.info("Import " + company.code + " " + year + "_" + quarter);

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
		else if (month > 3 || (month == 3 && day > 11)) {
			quarter = 4;
			year -= 1;
		} else {
			quarter = 3; 
			year -= 1;
		}

		quarterSet[0] = year;
		quarterSet[1] = quarter;
		return quarterSet;
	}

	private void importBasicData(MyStatement stm) throws Exception {
		QuarterlyBasicTable income = new QuarterlyBasicTable(year, quarter, company,
				QuarterlyBasicTable.INCOME_STATEMENT);
		QuarterlyBasicTable balance = new QuarterlyBasicTable(year, quarter, company,
				QuarterlyBasicTable.BALANCE_SHEET);
		QuarterlyBasicTable cashflow = new QuarterlyBasicTable(year, quarter, company,
				QuarterlyBasicTable.CASHFLOW_STATEMENT);

		boolean incomeResult = income.parse();
		boolean balanceResult = balance.parse();
		boolean cashflowResult = cashflow.parse();

		if (!incomeResult || !balanceResult || !cashflowResult)
			return;

		synchronized (lock) {
			stm.setObject(Integer.valueOf(year * 100 + quarter)); // YearQuarter
			stm.setObject(Integer.valueOf(company.code)); // StockNum

			stm.setObject(income.營收()); // 營收
			stm.setObject(income.成本()); // 成本
			stm.setObject(income.毛利()); // 毛利
			stm.setObject(income.營業利益()); // 營業利益
			stm.setObject(income.業外收支()); // 業外收支
			stm.setObject(income.parseLong("繼續營業單位稅前淨利（淨損）", "稅前淨利（淨損）", "繼續營業單位稅前損益", "繼續營業單位稅前純益（純損）")); // 稅前淨利
			stm.setObject(income.parseLong("繼續營業單位本期淨利（淨損）", "繼續營業單位本期稅後淨利（淨損）", "繼續營業單位本期純益（純損）")); // 稅後淨利
			stm.setObject(income.綜合損益()); // 綜合損益
			stm.setObject(income.parseLong("母公司業主（淨利／損）", "母公司業主（淨利／淨損）")); // 母公司業主淨利
			stm.setObject(income.parseLong("母公司業主（綜合損益）")); // 母公司業主綜合損益
			stm.setObject(income.eps()); // EPS

			stm.setObject(balance.parseLong("流動資產合計", "流動資產總額", "流動資產總計", "流動資產")); // 流動資產
			stm.setObject(balance.parseLong("現金及約當現金")); // 現金及約當現金
			stm.setObject(balance.parseLong("存貨")); // 存貨
			stm.setObject(balance.parseLong("預付款項")); // 預付款項
			stm.setObject(balance.parseLong("非流動資產合計", "非流動資產總額", "非流動資產總計", "非流動資產")); // 非流動資產
			stm.setObject(balance.長期投資()); // 長期投資
			stm.setObject(balance.parseLong("不動產、廠房及設備", "不動產及設備－淨額", "不動產及設備合計", "不動產、廠房及設備淨額")); // 固定資產
			stm.setObject(balance.parseLong("資產總額", "資產總計", "資產合計")); // 總資產

			stm.setObject(balance.parseLong("流動負債合計", "流動負債總額", "流動負債總計")); // 流動負債
			stm.setObject(balance.parseLong("非流動負債合計", "非流動負債總額", "非流動負債總計")); // 非流動負債
			stm.setObject(balance.總負債()); // 總負債
			stm.setObject(balance.parseLong("保留盈餘合計", "保留盈餘總額", "保留盈餘總計", "保留盈餘")); // 保留盈餘
			stm.setObject(balance.股本()); // 股本

			stm.setObject(cashflow.parseLong("利息費用")); // 利息費用
			stm.setObject(cashflow.parseLong("營業活動之淨現金流入（流出）", "營業活動之淨現金流入(流出)")); // 營業現金流
			stm.setObject(cashflow.parseLong("投資活動之淨現金流入（流出）", "投資活動之淨現金流入(流出)")); // 投資現金流
			stm.setObject(cashflow.parseLong("籌資活動之淨現金流入（流出）", "籌資活動之淨現金流入(流出)")); // 融資現金流
			stm.setObject((quarter == 1) ? Boolean.FALSE : Boolean.TRUE); // 現金流累計需更正
			stm.setObject((quarter == 4) ? Boolean.TRUE : Boolean.FALSE); // 第四季累計需修正
			stm.addBatch();
		}
	}

	private void importBasicDataNoIFRSs(MyStatement stm) throws Exception {
		QuarterlyBasicTable income;
		QuarterlyBasicTable balance;

		income = new QuarterlyBasicTable(year, quarter, company, QuarterlyBasicTable.INCOME_STATEMENT);
		if (!income.parse()) {
			income = new QuarterlyBasicTable(year, quarter, company, QuarterlyBasicTable.INCOME_STATEMENT_IDV);
			if (!income.parse())
				return;
		}

		balance = new QuarterlyBasicTable(year, quarter, company, QuarterlyBasicTable.BALANCE_SHEET);

		if (!balance.parse()) {
			balance = new QuarterlyBasicTable(year, quarter, company, QuarterlyBasicTable.BALANCE_SHEET_IDV);
			if (!balance.parse())
				return;
		}

		synchronized (lock) {
			stm.setObject(Integer.valueOf(year * 100 + quarter)); // YearQuarter
			stm.setObject(Integer.valueOf(company.code)); // StockNum

			stm.setObject(income.營收()); // 營收
			stm.setObject(income.成本()); // 成本
			stm.setObject(income.毛利()); // 毛利
			stm.setObject(income.營業利益()); // 營業利益
			stm.setObject(income.業外收支()); // 業外收支
			stm.setObject(income.parseLong("繼續營業部門稅前淨利(淨損)", "繼續營業部門稅前淨利（淨損）", "繼續營業部門稅前淨益(淨損)", "繼續營業單位稅前淨利(淨損)",
					"繼續營業單位稅前淨利（淨損）", "繼續營業單位稅前純益（純損）", "繼續營業單位稅前淨益(淨損)", "稅前純益(純損)", "稅前淨利（淨損）", "繼續營業部門稅前損益",
					"繼續營業單位稅前損益", "繼續營業單位稅前合併淨利（淨損）")); // 稅前淨利
			stm.setObject(income.parseLong("繼續營業部門稅後淨利（淨損）", "繼續營業部門淨利(淨損)", "繼續營業單位淨利(淨損)", "列計非常損益及會計原則變動累積影響數前損益",
					"列計非常損益及會計原則變動之累積影響數前淨利（淨額）", "繼續營業單位稅後純益（純損）", "合併總損益", "稅後純益", "本期損益（淨損）")); // 稅後淨利
			stm.setObject(income.綜合損益()); // 綜合損益
			stm.setObject(income.eps()); // EPS

			stm.setObject(balance.parseLong("流動資產合計", "流動資產")); // 流動資產
			stm.setObject(balance.parseLong("現金及約當現金")); // 現金及約當現金
			stm.setObject(balance.parseLong("存 貨", "存貨")); // 存貨
			stm.setObject(balance.parseLong("預付款項")); // 預付款項
			stm.setObject(balance.長期投資()); // 長期投資
			stm.setObject(balance.parseLong("固定資產淨額", "固定資產", "固定資產合計")); // 固定資產
			stm.setObject(balance.parseLong("資產總計", "資產", "資產合計")); // 總資產

			stm.setObject(balance.parseLong("流動負債合計", "流動負債")); // 流動負債
			stm.setObject(balance.總負債()); // 總負債
			stm.setObject(balance.parseLong("保留盈餘合計", "保留盈餘")); // 保留盈餘
			stm.setObject(balance.股本()); // 股本

			stm.setObject((quarter == 1) ? Boolean.FALSE : Boolean.TRUE); // 現金流累計需更正
			stm.setObject((quarter == 4) ? Boolean.TRUE : Boolean.FALSE); // 第四季累計需修正
			stm.addBatch();
		}
	}

	public static void supplementBasicData(MyDB myDB, int year, int quarter) throws Exception {
		int[] currentQuarter = getCurrentQuarter();
		int endYear = currentQuarter[0];
		int endQuarter = currentQuarter[1];

		db = myDB;
		Company[] companies = Company.getAllCompanies(db);

		queryIFRSs = new MyStatement(db.conn);
		queryIFRSs.setInsertOnDuplicateStatement("quarterly", "YearQuarter", "StockNum", "營收", "成本", "毛利", "營業利益",
				"業外收支", "稅前淨利", "稅後淨利", "綜合損益", "母公司業主淨利", "母公司業主綜合損益", "EPS", "流動資產", "現金及約當現金", "存貨", "預付款項", "非流動資產",
				"長期投資", "固定資產", "總資產", "流動負債", "非流動負債", "總負債", "保留盈餘", "股本", "利息費用", "營業現金流", "投資現金流", "融資現金流",
				"現金流累計需更正", "第四季累計需修正");
		queryIFRSs.setBatchSize(250);

		queryNoIFRSs = new MyStatement(db.conn);
		queryNoIFRSs.setInsertOnDuplicateStatement("quarterly", "YearQuarter", "StockNum", "營收", "成本", "毛利", "營業利益",
				"業外收支", "稅前淨利", "稅後淨利", "綜合損益", "EPS", "流動資產", "現金及約當現金", "存貨", "預付款項", "長期投資", "固定資產", "總資產", "流動負債",
				"總負債", "保留盈餘", "股本", "現金流累計需更正", "第四季累計需修正");
		queryNoIFRSs.setBatchSize(250);

		ExecutorService service = Executors.newFixedThreadPool(8);
		List<Future<?>> futures = new ArrayList<>();

		while (true) {
			if (year > endYear || (year == endYear && quarter > endQuarter)) {
				Log.info("End");
				break;
			}

			for (Company company : companies) {
				// skip no data stocks
				int stockNum = company.stockNum;

				if (company.category == null || stockNum < 1000 || stockNum > 9999) {
					Log.info(company.code + " skipped: invalid stock");
					continue;
				}

				if (!company.isValidQuarter(year, quarter)) {
					Log.info(company.code + " skipped: 已下市");
					continue;
				}

				if (stockNum == 5880 && year < 2013) {
					Log.info(company.code + " Skipped: 公開資訊站查無資料");
					continue;
				}

				if (stockNum == 2841 && year < 2006) {
					Log.info(company.code + " Skipped: 表格內容殘缺");
					continue;
				}

				if (stockNum == 4141 && year < 2010) {
					Log.info(company.code + " Skipped: 股本不正確");
					continue;
				}

				futures.add(service.submit(new ImportQuarterly(year, quarter, company)));
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

		queryNoIFRSs.close();
		queryIFRSs.close();
	}

	public static void main(String[] args) {
		try {
			MyDB db = new MyDB();
			int yearQuarter = db.getLastQuarterlyRevenue();
			int year = yearQuarter / 100;
			int quarter = yearQuarter % 100;
			supplementBasicData(db, year, quarter);
			QuarterlyFixAndSupplement.calculate(db);
			Log.info("Done");
			db.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
