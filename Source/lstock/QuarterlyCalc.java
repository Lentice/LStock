package lstock;

import java.sql.SQLException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import lstock.ui.RebuildUI;

public class QuarterlyCalc implements Runnable {
	private static final Logger log = LogManager.getLogger(QuarterlyCalc.class.getName());
	
	static MyStatement stQuarter;
	
	private static final Object uiLock = new Object();
	private static int totalImportCount;
	private static int oldPercentage;
	private static int importedCount;

	MyDB db;
	Company company;
	QuarterlyData[] qData;
	
	QuarterlyCalc(MyDB db, Company company) {
		this.db = db;
		this.company = company;
	}
	
	void calc(QuarterlyData[] data, int idxStart) {
		
		
		for (int i = idxStart; i < data.length; i++) {
			if (data[i] == null || data[i].營收 == null)
				continue;
			
			data[i].股東權益 = data[i].總資產 - data[i].總負債;			
			if (data[i].股本 != 0)
				data[i].每股淨值 = (float) data[i].股東權益 / (data[i].股本 / 10); // 每股10元
			
			if (data[i].營收 != 0) {
				data[i].毛利率 = (float) data[i].毛利 / data[i].營收;
				data[i].營業利益率 = (float) data[i].營業利益 / data[i].營收;
				data[i].稅前淨利率 = (float) data[i].稅前淨利 / data[i].營收;
				data[i].稅後淨利率 = (float) data[i].稅後淨利 / data[i].營收;
			}
			
			if (data[i].稅前淨利 != 0)
				data[i].業外收支比重 = (float) data[i].業外收支 / data[i].稅前淨利;
			
			if (data[i].總資產 != 0) {
				data[i].ROA = (float) data[i].稅後淨利 / data[i].總資產;
				data[i].負債比 = (float) data[i].總負債 / data[i].總資產;
				data[i].總資產週轉率 = (float) data[i].營收 / data[i].總資產;
			}
			
			if (data[i].股東權益 != 0) {
				data[i].權益乘數 = (float) data[i].總資產 / data[i].股東權益;
				if (i > 0 && data[i-1] != null) {
					if (data[i].股東權益 + data[i-1].股東權益 != 0)  
						data[i].ROE = (float) data[i].稅後淨利 / ((data[i].股東權益 + data[i-1].股東權益) / 2);
				} else {
					data[i].ROE = (float) data[i].稅後淨利 / data[i].股東權益;
				}
			}
			
			if (data[i].流動負債 != 0) {
				data[i].流動比 = (float) data[i].流動資產 / data[i].流動負債;
				data[i].速動比 = (float) (data[i].流動資產 - data[i].存貨 - data[i].預付款項) / data[i].流動負債;
				data[i].營業現金對流動負債比 = (float) data[i].營業現金流 / data[i].流動負債;
			}
			
			if (data[i].總負債 != 0)
				data[i].營業現金對負債比 = (float) data[i].營業現金流 / data[i].總負債;
			
			if (data[i].稅後淨利 != 0) {
				data[i].營業現金流對淨利比 = (float) data[i].營業現金流 / data[i].稅後淨利;
				data[i].自由現金流對淨利比 = (float) data[i].自由現金流 / data[i].稅後淨利;
			}
			
			if (data[i].利息費用 != 0)
				data[i].利息保障倍數 = (float) (data[i].稅前淨利 + data[i].利息費用) / data[i].利息費用;
			
			if (i > 0 && data[i-1] != null) {
				float 平均存貨 = (float) (data[i].存貨 + data[i-1].存貨) / 2;
				if (平均存貨 != 0)
					data[i].存貨周轉率 = (float) data[i].成本 / 平均存貨;
			}
			
			if (i > 4 && data[i-4] != null) {
				if (data[i-4].營收 != 0)
					data[i].單季營收年增率 = (float) data[i].營收 / data[i-4].營收 - 1;
				if (data[i-4].毛利 != 0)
					data[i].單季毛利年增率 = (float) data[i].毛利 / data[i-4].毛利 - 1;
				if (data[i-4].營業利益 != 0)
					data[i].單季營業利益年增率 = (float) data[i].營業利益 / data[i-4].營業利益 - 1;
				if (data[i-4].稅後淨利 != 0)
					data[i].單季稅後淨利年增率 = (float) data[i].稅後淨利 / data[i-4].稅後淨利 - 1;
				if (data[i-4].EPS != 0)
					data[i].單季EPS年增率 = (float) data[i].EPS / data[i-4].EPS - 1;
				if (data[i-4].總資產 != 0)
					data[i].單季總資產年增率 = (float) data[i].總資產 / data[i-4].總資產 - 1;
				if (data[i-4].股東權益 != 0)
					data[i].單季淨值年增率 = (float) data[i].股東權益 / data[i-4].股東權益 - 1;
				if (data[i-4].固定資產 != 0)
					data[i].單季固定資產年增率 = (float) data[i].固定資產 / data[i-4].固定資產 - 1;
			}
			
			if (i > 3 && data[i-3] != null && data[i-2] != null && data[i-1] != null) {
				long 利息費用累計 = data[i].利息費用 + data[i-1].利息費用 + data[i-2].利息費用 + data[i-3].利息費用;
				long 稅前淨利累計 = data[i].稅前淨利 + data[i-1].稅前淨利 + data[i-2].稅前淨利 + data[i-3].稅前淨利;
				if (利息費用累計 != 0)
					data[i].近四季利息保障倍數 = (float) (稅前淨利累計 + 利息費用累計) / 利息費用累計;
				
				data[i].近四季稅後淨利 = data[i].稅後淨利 + data[i-1].稅後淨利 + data[i-2].稅後淨利 + data[i-3].稅後淨利;
				data[i].近四季營業現金流 = data[i].營業現金流 + data[i-1].營業現金流 + data[i-2].營業現金流 + data[i-3].營業現金流;
				
				long 近四季營收 = data[i].營收 + data[i-1].營收 + data[i-2].營收 + data[i-3].營收;
				long 近四季營業利益 = data[i].營業利益 + data[i-1].營業利益 + data[i-2].營業利益 + data[i-3].營業利益;
				if (近四季營業利益 != 0)
				data[i].近四季營業利益率 = (float) 近四季營收 / 近四季營業利益;
				
				data[i].近四季ROE = data[i].ROE + data[i-1].ROE + data[i-2].ROE + data[i-3].ROE;
			
				if (i > 7 && data[i-4] != null && data[i-5] != null && data[i-6] != null && data[i-7] != null) {
					long past4quarter;
					long lastYearPast4quarter;
					float fPast4quarter;
					float fLastYearPast4quarter;
	
					past4quarter = data[i].營收 + data[i-1].營收 + data[i-2].營收 + data[i-3].營收;
					lastYearPast4quarter = data[i-4].營收 + data[i-5].營收 + data[i-6].營收 + data[i-7].營收;
					if (lastYearPast4quarter != 0)
						data[i].近4季營收年增率 = (float) past4quarter / lastYearPast4quarter - 1;
					
					past4quarter = data[i].毛利 + data[i-1].毛利 + data[i-2].毛利 + data[i-3].毛利;
					lastYearPast4quarter = data[i-4].毛利 + data[i-5].毛利 + data[i-6].毛利 + data[i-7].毛利;
					if (lastYearPast4quarter != 0)
						data[i].近4季毛利年增率 = (float) past4quarter / lastYearPast4quarter - 1;
					
					past4quarter = data[i].營業利益 + data[i-1].營業利益 + data[i-2].營業利益 + data[i-3].營業利益;
					lastYearPast4quarter = data[i-4].營業利益 + data[i-5].營業利益 + data[i-6].營業利益 + data[i-7].營業利益;
					if (lastYearPast4quarter != 0)
						data[i].近4季營業利益年增率 = (float) past4quarter / lastYearPast4quarter - 1;
					
					past4quarter = data[i].稅後淨利 + data[i-1].稅後淨利 + data[i-2].稅後淨利 + data[i-3].稅後淨利;
					lastYearPast4quarter = data[i-4].稅後淨利 + data[i-5].稅後淨利 + data[i-6].稅後淨利 + data[i-7].稅後淨利;
					if (lastYearPast4quarter != 0)
						data[i].近4季稅後淨利年增率 = (float) past4quarter / lastYearPast4quarter - 1;
					
					fPast4quarter = data[i].EPS + data[i-1].EPS + data[i-2].EPS + data[i-3].EPS;
					fLastYearPast4quarter = data[i-4].EPS + data[i-5].EPS + data[i-6].EPS + data[i-7].EPS;
					if (fLastYearPast4quarter != 0)
						data[i].近4季EPS年增率 = (float) fPast4quarter / fLastYearPast4quarter - 1;
	
					past4quarter = data[i].總資產 + data[i-1].總資產 + data[i-2].總資產 + data[i-3].總資產;
					lastYearPast4quarter = data[i-4].總資產 + data[i-5].總資產 + data[i-6].總資產 + data[i-7].總資產;
					if (lastYearPast4quarter != 0)
						data[i].近4季總資產年增率 = (float) past4quarter / lastYearPast4quarter - 1;
	
					fPast4quarter = data[i].股東權益 + data[i-1].股東權益 + data[i-2].股東權益 + data[i-3].股東權益;
					fLastYearPast4quarter = data[i-4].股東權益 + data[i-5].股東權益 + data[i-6].股東權益 + data[i-7].股東權益;
					if (fLastYearPast4quarter != 0)
						data[i].近4季淨值年增率 = (float) fPast4quarter / fLastYearPast4quarter - 1;
					
					fPast4quarter = data[i].固定資產 + data[i-1].固定資產 + data[i-2].固定資產 + data[i-3].固定資產;
					fLastYearPast4quarter = data[i-4].固定資產 + data[i-5].固定資產 + data[i-6].固定資產 + data[i-7].固定資產;
					if (fLastYearPast4quarter != 0)
						data[i].近4季固定資產年增率 = (float) fPast4quarter / fLastYearPast4quarter - 1;
				}
			}
		}
	}
	
	void importToDB(QuarterlyData[] qData, int idxStart) throws SQLException {
		synchronized (stQuarter.AcquireLock()) {
			for (int i = idxStart; i < qData.length; i++) {
				if (qData[i] == null)
					continue;
				
				qData[i].季報已計算 = true;
				stQuarter.setObject(qData[i].季報已計算);
				stQuarter.setObject(qData[i].股東權益);
				stQuarter.setObject(qData[i].每股淨值);
				stQuarter.setObject(qData[i].毛利率);
				stQuarter.setObject(qData[i].營業利益率);
				stQuarter.setObject(qData[i].稅前淨利率);
				stQuarter.setObject(qData[i].稅後淨利率);
				stQuarter.setObject(qData[i].總資產週轉率);
				stQuarter.setObject(qData[i].權益乘數);
				stQuarter.setObject(qData[i].業外收支比重);
				stQuarter.setObject(qData[i].ROA);
				stQuarter.setObject(qData[i].ROE);
				stQuarter.setObject(qData[i].存貨周轉率);
				stQuarter.setObject(qData[i].負債比);
				stQuarter.setObject(qData[i].流動比);
				stQuarter.setObject(qData[i].速動比);
	
				stQuarter.setObject(qData[i].利息保障倍數);
				stQuarter.setObject(qData[i].營業現金對流動負債比);
				stQuarter.setObject(qData[i].營業現金對負債比);
				stQuarter.setObject(qData[i].營業現金流對淨利比);
				stQuarter.setObject(qData[i].自由現金流對淨利比);
	
				stQuarter.setObject(qData[i].近四季稅後淨利);
				stQuarter.setObject(qData[i].近四季營業現金流);
				stQuarter.setObject(qData[i].近四季營業利益率);
				stQuarter.setObject(qData[i].近四季ROE);
				stQuarter.setObject(qData[i].近四季利息保障倍數);
				stQuarter.setObject(qData[i].單季營收年增率);
				stQuarter.setObject(qData[i].近4季營收年增率);
				stQuarter.setObject(qData[i].單季毛利年增率);
				stQuarter.setObject(qData[i].近4季毛利年增率);
				stQuarter.setObject(qData[i].單季營業利益年增率);
				stQuarter.setObject(qData[i].近4季營業利益年增率);
				stQuarter.setObject(qData[i].單季稅後淨利年增率);
				stQuarter.setObject(qData[i].近4季稅後淨利年增率);
				stQuarter.setObject(qData[i].單季EPS年增率);
				stQuarter.setObject(qData[i].近4季EPS年增率);
				stQuarter.setObject(qData[i].單季總資產年增率);
				stQuarter.setObject(qData[i].近4季總資產年增率);
				stQuarter.setObject(qData[i].單季淨值年增率);
				stQuarter.setObject(qData[i].近4季淨值年增率);
				stQuarter.setObject(qData[i].單季固定資產年增率);
				stQuarter.setObject(qData[i].近4季固定資產年增率);
	
				stQuarter.setObject(qData[i].yearQuarter);
				stQuarter.setObject(company.stockNum);
				stQuarter.addBatch();
			}
		}
	}
	
	public void run() {

		final String uiMessage = String.format("季報運算: %d", company.stockNum);
		try {
			RebuildUI.addProcess(uiMessage);
			qData = QuarterlyData.getAllCalcRefData(db, company.stockNum);
			
			if (qData == null)
				return;
			
			int idxStart = 0;
			
			for (int i = 0; i < qData.length; i++) {
				if (qData[i] != null && qData[i].季報已計算 == false) {
					idxStart = i;
					break;
				}
			}
			
			calc(qData, idxStart);
			importToDB(qData, idxStart);
			
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
	
	public static void calculateAllCompanies(MyDB db) throws Exception {
		Company[] companies = Company.getAllCompaniesWith(
				"(SELECT MIN(季報已計算) FROM quarterly Where quarterly.StockNum = StockNum) = 0");
		
		if (companies == null || companies.length == 0) {
			RebuildUI.updateProgressBar(100);
			return;
		}
		totalImportCount = companies.length;
		RebuildUI.updateProgressBar(0);
		oldPercentage = 0;
		importedCount = 0;
	
		stQuarter = new MyStatement(db.conn);
		stQuarter.setStatementUpdate("quarterly", "YearQuarter=? AND StockNum=?", 
				"季報已計算", "股東權益", "每股淨值", "毛利率", "營業利益率", "稅前淨利率", "稅後淨利率", 
				"總資產週轉率", "權益乘數", "業外收支比重", "ROA", "ROE", "存貨周轉率", "負債比", 
				"流動比", "速動比", "利息保障倍數", "營業現金對流動負債比", "營業現金對負債比", "營業現金流對淨利比", 
				"自由現金流對淨利比", "單季營收年增率", "單季毛利年增率", "單季營業利益年增率", "單季稅後淨利年增率", 
				"單季EPS年增率", "單季總資產年增率", "單季淨值年增率", "單季固定資產年增率", "近四季稅後淨利", 
				"近四季營業現金流", "近四季營業利益率", "近四季ROE", "近四季利息保障倍數", "近4季營收年增率", 
				"近4季毛利年增率", "近4季營業利益年增率", "近4季稅後淨利年增率", "近4季EPS年增率", "近4季總資產年增率", 
				"近4季淨值年增率", "近4季固定資產年增率");
		stQuarter.setBatchSize(250);
		
		MyThreadPool threadPool = new MyThreadPool();

		for (Company company : companies) {
			threadPool.add(new QuarterlyCalc(db, company));
		}

		threadPool.waitFinish();

		stQuarter.close();
	}
	
	public static void main(String[] args) throws Exception {
		MyDB db = new MyDB();
		log.info("Start!!");
		calculateAllCompanies(db);
		db.close();
		log.info("Done!!");
	}
}


