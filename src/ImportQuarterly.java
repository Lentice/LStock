import java.io.File;
import java.net.URLEncoder;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Calendar;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;

class QuarterlyData {
	private static final String ALL_DATA = "SELECT * FROM quarterly WHERE StockNum=%s AND 總資產 > 0 ORDER BY YearQuarter";

	Integer YearQuarter;
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
	Boolean 季報有缺少;

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
	Float 盈再率;

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
		int colume = 1;

		/* 以下數值若不存在 則當成0 以方便計算 */
		YearQuarter = (Integer) rs.getObject(colume++, Integer.class);
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
		總資產 = rs.getObject(colume++, Long.class);
		流動負債 = rs.getObject(colume++, Long.class);
		非流動負債 = rs.getObject(colume++, Long.class);
		總負債 = rs.getObject(colume++, Long.class);
		保留盈餘 = rs.getObject(colume++, Long.class);
		股本 = rs.getObject(colume++, Long.class);
		營業現金流 = rs.getObject(colume++, Long.class);
		投資現金流 = rs.getObject(colume++, Long.class);
		融資現金流 = rs.getObject(colume++, Long.class);
		自由現金流 = rs.getObject(colume++, Long.class);
		現金流累計需更正 = rs.getObject(colume++, Boolean.class);
		第四季累計需修正 = rs.getObject(colume++, Boolean.class);
		季報有缺少 = rs.getObject(colume++, Boolean.class);

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
		盈再率 = (Float) rs.getObject(colume++);

		單季營收年增率 = (Float) rs.getObject(colume++);
		近4季營收年增率 = (Float) rs.getObject(colume++);
		單季毛利年增率 = (Float) rs.getObject(colume++);
		近4季毛利年增率 = (Float) rs.getObject(colume++);
		單季營業利益年增率 = (Float) rs.getObject(colume++);
		近4季營業利益年增率 = (Float) rs.getObject(colume++);
		單季稅後淨利年增率 = (Float) rs.getObject(colume++);
		近4季稅後淨利年增率 = (Float) rs.getObject(colume++);
		單季EPS年增率 = (Float) rs.getObject(colume++);
		近4季EPS年增率 = (Float) rs.getObject(colume++);
		單季總資產年增率 = (Float) rs.getObject(colume++);
		近4季總資產年增率 = (Float) rs.getObject(colume++);
		單季淨值年增率 = (Float) rs.getObject(colume++);
		近4季淨值年增率 = (Float) rs.getObject(colume++);
		單季固定資產年增率 = (Float) rs.getObject(colume++);
		近4季固定資產年增率 = (Float) rs.getObject(colume++);
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
			if (data.YearQuarter / 100 == year && data.YearQuarter % 100 == quarter) {
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
			if (qdata.YearQuarter / 100 == year && qdata.YearQuarter % 100 == quarter) {
				return qdata;
			}
		}
		return null;
	}
}

class QuarterlyFixAndSupplement {
	public static void run(MyDB db) throws Exception {
		Company[] companies = Company.getAllCompanies(db);

		MyStatement cashflowStm = new MyStatement(db.conn);
		cashflowStm.setUpdateStatement("quarterly", "YearQuarter=? AND StockNum=?", "營業現金流", "投資現金流", "融資現金流",
				"現金流累計需更正");

		MyStatement quarter4Stm = new MyStatement(db.conn);
		quarter4Stm.setUpdateStatement("quarterly", "YearQuarter=? AND StockNum=?", "營收", "成本", "毛利", "研究發展費用", "營業利益",
				"業外收支", "稅前淨利", "稅後淨利", "綜合損益", "母公司業主淨利", "母公司業主綜合損益", "EPS", "營業現金流", "投資現金流", "融資現金流", "第四季累計需修正",
				"季報有缺少");

		MyStatement supplementStm = new MyStatement(db.conn);
		supplementStm.setUpdateStatement("quarterly", "YearQuarter=? AND StockNum=?", "自由現金流", "股東權益", "每股淨值", "長期投資",
				"毛利率", "營業利益率", "稅前淨利率", "稅後淨利率", "總資產週轉率", "權益乘數", "業外收支比重", "ROA", "ROE", "負債比", "流動比", "速動比", "營業現金對流動負債比",
				"營業現金對負債比", "營業現金流對淨利比", "自由現金流對淨利比", "單季營收年增率", "近4季營收年增率", "單季毛利年增率", "近4季毛利年增率", "單季營業利益年增率",
				"近4季營業利益年增率", "單季稅後淨利年增率", "近4季稅後淨利年增率", "單季EPS年增率", "近4季EPS年增率", "單季總資產年增率", "近4季總資產年增率", "單季淨值年增率",
				"近4季淨值年增率", "單季固定資產年增率", "近4季固定資產年增率");

		for (Company company : companies) {

			int stockNum = Integer.parseInt(company.code);

			QuarterlyData[] data = QuarterlyData.getAllData(db, stockNum);
			if (data == null)
				continue;

			Log.trace("更正現金流累計 " + company.code);
			cashflowCalculateAndImport(cashflowStm, stockNum, data);
			Log.trace("更正第4季累計 " + company.code);
			quarter4CalculateAndImport(quarter4Stm, stockNum, data);
			Log.trace("補完剩餘欄位 " + company.code);
			supplementOtherField(supplementStm, stockNum, data);
		}

		cashflowStm.close();
		quarter4Stm.close();
		supplementStm.close();
	}

	static void quarter4CalculateAndImport(MyStatement stm, int StockNum, QuarterlyData[] allQuarter)
			throws SQLException {

		boolean lostData = false;

		int firstYear = allQuarter[0].YearQuarter / 100;

		for (QuarterlyData qdata : allQuarter) {
			int quarter = qdata.YearQuarter % 100;
			if (quarter != 4)
				continue;

			int year = qdata.YearQuarter / 100;
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
				qdata.研究發展費用 -= (qSub.研究發展費用);
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

	static void quarter4ImportToDB(MyStatement stm, int StockNum, QuarterlyData data) throws SQLException {
		stm.setObject(data.營收);
		stm.setObject(data.成本);
		stm.setObject(data.毛利);
		stm.setObject(data.研究發展費用);
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
		stm.setObject(data.YearQuarter);
		stm.setObject(StockNum);
		stm.addBatch();
	}

	static void cashflowCalculateAndImport(MyStatement stm, int StockNum, QuarterlyData[] allQuarter) throws Exception {

		for (QuarterlyData qdata : allQuarter) {

			if (qdata.現金流累計需更正 == false)
				continue;

			int year = qdata.YearQuarter / 100;
			int quarter = qdata.YearQuarter % 100;

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

	static void cashflowImportToDB(MyStatement stm, int StockNum, QuarterlyData data) throws SQLException {
		stm.setObject(data.營業現金流);
		stm.setObject(data.投資現金流);
		stm.setObject(data.融資現金流);
		stm.setObject(data.現金流累計需更正);
		stm.setObject(data.YearQuarter);
		stm.setObject(StockNum);
		stm.addBatch();
	}

	static void supplementOtherField(MyStatement stm, int StockNum, QuarterlyData[] allQuarter) throws SQLException {
		QuarterlyData past1Q, past2Q, past3Q;
		QuarterlyData past4Q, past5Q, past6Q, past7Q;

		for (QuarterlyData qdata : allQuarter) {

			if (qdata.股東權益 != null)
				continue;

			past1Q = QuarterlyData.getShiftData(allQuarter, qdata.YearQuarter, -1);
			past2Q = QuarterlyData.getShiftData(allQuarter, qdata.YearQuarter, -2);
			past3Q = QuarterlyData.getShiftData(allQuarter, qdata.YearQuarter, -3);
			past4Q = QuarterlyData.getShiftData(allQuarter, qdata.YearQuarter, -4);
			past5Q = QuarterlyData.getShiftData(allQuarter, qdata.YearQuarter, -5);
			past6Q = QuarterlyData.getShiftData(allQuarter, qdata.YearQuarter, -6);
			past7Q = QuarterlyData.getShiftData(allQuarter, qdata.YearQuarter, -7);

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
			// TODO: 盈再率

			if (past4Q != null && past4Q.第四季累計需修正 == false) {
				if (past4Q.營收 != 0)
					qdata.單季營收年增率 = (float) qdata.營收 / past4Q.營收 - 1;
				if (past4Q.毛利 != 0)
					qdata.單季毛利年增率 = (float) qdata.毛利 / past4Q.毛利 - 1;
				if (past4Q.營業利益 != 0)
					qdata.單季營業利益年增率 = (float) qdata.營業利益 / past4Q.營業利益 - 1;
				if (past4Q.稅後淨利 != 0)
					qdata.單季稅後淨利年增率 = (float) qdata.稅後淨利 / past4Q.稅後淨利 - 1;
				if (past4Q.EPS != 0)
					qdata.單季EPS年增率 = (float) qdata.EPS / past4Q.EPS - 1;
				if (past4Q.總資產 != 0)
					qdata.單季總資產年增率 = (float) qdata.總資產 / past4Q.總資產 - 1;
				if (past4Q.股東權益 != 0)
					qdata.單季淨值年增率 = (float) qdata.股東權益 / past4Q.股東權益 - 1;
				if (past4Q.固定資產 != 0)
					qdata.單季固定資產年增率 = (float) qdata.固定資產 / past4Q.固定資產 - 1;
			}

			if (past7Q != null && past6Q != null && past5Q != null && past4Q != null && past3Q != null && past2Q != null
					&& past1Q != null && past7Q.第四季累計需修正 == false && past6Q.第四季累計需修正 == false
					&& past5Q.第四季累計需修正 == false && past4Q.第四季累計需修正 == false && past3Q.第四季累計需修正 == false
					&& past2Q.第四季累計需修正 == false && past1Q.第四季累計需修正 == false) {
				long past4quarter;
				long lastYearpast4quarter;
				float fpast4quarter;
				float flastYearpast4quarter;

				past4quarter = qdata.營收 + past1Q.營收 + past2Q.營收 + past3Q.營收;
				lastYearpast4quarter = past4Q.營收 + past5Q.營收 + past6Q.營收 + past7Q.營收;
				if (lastYearpast4quarter != 0)
					qdata.近4季營收年增率 = (float) past4quarter / lastYearpast4quarter - 1;

				past4quarter = qdata.毛利 + past1Q.毛利 + past2Q.毛利 + past3Q.毛利;
				lastYearpast4quarter = past4Q.毛利 + past5Q.毛利 + past6Q.毛利 + past7Q.毛利;
				if (lastYearpast4quarter != 0)
					qdata.近4季毛利年增率 = (float) past4quarter / lastYearpast4quarter - 1;

				past4quarter = qdata.營業利益 + past1Q.營業利益 + past2Q.營業利益 + past3Q.營業利益;
				lastYearpast4quarter = past4Q.營業利益 + past5Q.營業利益 + past6Q.營業利益 + past7Q.營業利益;
				if (lastYearpast4quarter != 0)
					qdata.近4季營業利益年增率 = (float) past4quarter / lastYearpast4quarter - 1;

				past4quarter = qdata.稅後淨利 + past1Q.稅後淨利 + past2Q.稅後淨利 + past3Q.稅後淨利;
				lastYearpast4quarter = past4Q.稅後淨利 + past5Q.稅後淨利 + past6Q.稅後淨利 + past7Q.稅後淨利;
				if (lastYearpast4quarter != 0)
					qdata.近4季稅後淨利年增率 = (float) past4quarter / lastYearpast4quarter - 1;

				fpast4quarter = qdata.EPS + past1Q.EPS + past2Q.EPS + past3Q.EPS;
				flastYearpast4quarter = past4Q.EPS + past5Q.EPS + past6Q.EPS + past7Q.EPS;
				if (flastYearpast4quarter != 0)
					qdata.近4季EPS年增率 = fpast4quarter / flastYearpast4quarter - 1;

				past4quarter = qdata.總資產 + past1Q.總資產 + past2Q.總資產 + past3Q.總資產;
				lastYearpast4quarter = past4Q.總資產 + past5Q.總資產 + past6Q.總資產 + past7Q.總資產;
				if (lastYearpast4quarter != 0)
					qdata.近4季總資產年增率 = (float) past4quarter / lastYearpast4quarter - 1;

				fpast4quarter = qdata.股東權益 + past1Q.股東權益 + past2Q.股東權益 + past3Q.股東權益;
				flastYearpast4quarter = past4Q.股東權益 + past5Q.股東權益 + past6Q.股東權益 + past7Q.股東權益;
				if (flastYearpast4quarter != 0)
					qdata.近4季淨值年增率 = fpast4quarter / flastYearpast4quarter - 1;

				fpast4quarter = qdata.固定資產 + past1Q.固定資產 + past2Q.固定資產 + past3Q.固定資產;
				flastYearpast4quarter = past4Q.固定資產 + past5Q.固定資產 + past6Q.固定資產 + past7Q.固定資產;
				if (flastYearpast4quarter != 0)
					qdata.近4季固定資產年增率 = fpast4quarter / flastYearpast4quarter - 1;
			}

			supplementImportToDB(stm, StockNum, qdata);
		}
	}

	static void supplementImportToDB(MyStatement stm, int StockNum, QuarterlyData data) throws SQLException {
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
		// TODO: 利息保障倍數 =
		// TODO: 盈再率

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

		stm.setObject(data.YearQuarter);
		stm.setObject(StockNum);
		stm.addBatch();
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
	String[][] data;
	Company company;

	QuarterlyBasicTable(int year, int quarter, Company company, int tableType) throws Exception {
		if (year < 2001)
			throw new Exception("Year is earlier than 2001");

		this.year = year;
		this.quarter = quarter;
		this.company = company;
		this.code = company.code;
		this.category = company.category;
		lastUpdateDate = company.lastUpdateInt;
		this.tableType = tableType;

		stockNum = Integer.parseInt(code);
		useIFRSs = MyDB.isUseIFRSs(stockNum, year);

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
			eTitles = doc.getElementsContainingOwnText("每股盈餘");
		} else if (tableType == INCOME_STATEMENT_IDV) {
			eTitles = doc.getElementsContainingOwnText("每股盈餘");
		} else if (tableType == BALANCE_SHEET) {
			// 此筆合併營收資料有欄位 卻無數字，應當成無合併營收處理
			if (stockNum == 6189 && year == 2004 && quarter == 4)
				return false;
			eTitles = doc.getElementsContainingOwnText("流動資產");
		} else if (tableType == BALANCE_SHEET_IDV) {
			eTitles = doc.getElementsContainingOwnText("流動資產");
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
				Log.warn("請至採IFRSs前之 " + code + " " + year + "_" + quarter);
				return false;
			} else if (str.contains("不繼續公開發行")) {
				Log.warn("不繼續公開發行 " + code + " " + year + "_" + quarter);
				return false;
			} else if (str.contains("已下市")) {
				Log.warn("已下市 " + code + " " + year + "_" + quarter);
				return false;
			} else if (str.contains("無應編製合併財報")) {
				Log.warn("無應編製合併財報之子公司  " + code + " " + year + "_" + quarter);
				return false;
			} else if (str.contains("外國發行人免申報")) {
				Log.warn("外國發行人免申報  " + code + " " + year + "_" + quarter);
				return false;
			} else if (str.contains("不存在")) {
				// Log.warn("不存在 " + code + " " + year + "_" + quarter);
				return false;
			} else if (str.contains("第二上市")) {
				// Log.warn("第二上市 " + code + " " + year + "_" + quarter);
				return false;
			} else {
				throw new Exception(folderPath + filename);
				// return false;
			}
		}

		Elements eTableRows = eTitles.first().parent().parent().children();

		data = new String[eTableRows.size()][2];
		for (int i = 0; i < eTableRows.size(); i++) {
			Elements eColumes = eTableRows.get(i).children();
			if (eColumes.size() < 2)
				continue;

			data[i][0] = HtmlParser.getText(eColumes.get(0));
			data[i][1] = HtmlParser.getText(eColumes.get(1));

			if (!useIFRSs && data[i][1] != null)
				data[i][1] = data[i][1].replace(".00", "");

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

		if (!company.isValidQuarter(year, quarter)) {
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

		boolean foundTitle = false;
		String tempData = null;
		for (String name : names) {
			for (int i = 0; i < data.length; i++) {
				String title = data[i][0];
				if (title == null || title.compareTo(name) != 0)
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

	private static void importBasicDataNoIFRSs(MyStatement stm, int year, int quarter, Company company)
			throws Exception {
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

		int idx = 1;
		stm.setInt(idx++, year * 100 + quarter); // YearQuarter
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

		stm.setTinyInt(idx++, (quarter == 1) ? 0 : 1); // 現金流累計需更正
		stm.setTinyInt(idx++, (quarter == 4) ? 1 : 0); // 第四季累計需修正
		stm.addBatch();
	}

	private static void importBasicData(MyStatement stm, int year, int quarter, Company company) throws Exception {
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

		int idx = 1;
		stm.setInt(idx++, year * 100 + quarter); // YearQuarter
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
		stm.setTinyInt(idx++, (quarter == 1) ? 0 : 1); // 現金流累計需更正
		stm.setTinyInt(idx++, (quarter == 4) ? 1 : 0); // 第四季累計需修正
		stm.addBatch();
	}

	public static void supplementBasicData(int year, int quarter) throws Exception {
		int[] currentQuarter = getCurrentQuarter();
		int endYear = currentQuarter[0];
		int endQuarter = currentQuarter[1];

		Company[] company = Company.getAllCompanies(db);

		MyStatement queryIFRSs = new MyStatement(db.conn);
		queryIFRSs.setInsertOnDuplicateStatement("quarterly", "YearQuarter", "StockNum", "營收", "成本", "毛利", "研究發展費用",
				"營業利益", "業外收支", "稅前淨利", "稅後淨利", "綜合損益", "母公司業主淨利", "母公司業主綜合損益", "EPS", "流動資產", "存貨", "預付款項", "非流動資產",
				"備供出售金融資產", "持有至到期日金融資產", "以成本衡量之金融資產", "採用權益法之投資淨額", "固定資產", "總資產", "流動負債", "非流動負債", "總負債", "保留盈餘",
				"股本", "營業現金流", "投資現金流", "融資現金流", "現金流累計需更正", "第四季累計需修正");

		MyStatement queryNoIFRSs = new MyStatement(db.conn);
		queryNoIFRSs.setInsertOnDuplicateStatement("quarterly", "YearQuarter", "StockNum", "營收", "成本", "毛利", "研究發展費用",
				"營業利益", "業外收支", "稅前淨利", "稅後淨利", "綜合損益", "EPS", "流動資產", "存貨", "預付款項", "長期投資", "固定資產", "總資產", "流動負債",
				"總負債", "保留盈餘", "股本", "現金流累計需更正", "第四季累計需修正");

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
					Log.info(code + " skipped: invalid stock");
					continue;
				}

				if (!company[i].isValidQuarter(year, quarter)) {
					Log.info(code + " skipped: 已下市");
					continue;
				}

				if (category.compareTo("金融保險業") == 0 || stockNum == 2807) {
					Log.info(code + " skipped: 金融保險業");
					continue;
				}

				if (stockNum == 2905 || stockNum == 2514 || stockNum == 1409 || stockNum == 1718) {
					Log.info(code + " Skipped: 表格格式與人不同");
					continue;
				}

				Log.info("Import " + code + " " + year + "_" + quarter);
				Boolean isUseIFRSs = MyDB.isUseIFRSs(Integer.parseInt(code), year);
				if (isUseIFRSs) {
					importBasicData(queryIFRSs, year, quarter, company[i]);
				} else {
					importBasicDataNoIFRSs(queryNoIFRSs, year, quarter, company[i]);
				}
			}

			// Next quarter
			if (++quarter > 4) {
				quarter = 1;
				year++;
			}
		}
		queryIFRSs.close();
	}

	public static void main(String[] args) {

		try {
			db = new MyDB();
			int yearQuarter = db.getLastQuarterlyRevenue();
			int year = yearQuarter / 100;
			int quarter = yearQuarter % 100;
			supplementBasicData(year, quarter);
			QuarterlyFixAndSupplement.run(db);
			Log.info("Done");
			db.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
