package lstock;

import java.sql.SQLException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import lstock.ui.RebuildUI;

public class AnnualCalc implements Runnable {
	private static final Logger log = LogManager.getLogger(AnnualCalc.class.getName());
	
	static MyStatement stAnnual;
	
	private static final Object uiLock = new Object();
	private static int totalImportCount;
	private static int oldPercentage;
	private static int importedCount;

	MyDB db;
	Company company;
	AnnualData[] data;
	
	AnnualCalc(MyDB db, Company company){
		this.db = db;
		this.company = company;
	}
	
	void calc(AnnualData[] data, int idxStart) {
		
		for (int i = idxStart; i < data.length; i++) {
			if (data[i] == null || data[i].營收 == null)
				continue;
			
			data[i].股東權益 = data[i].總資產 - data[i].總負債;
			if (data[i].股本 != 0)
				data[i].每股淨值 = (float) data[i].股東權益 / (data[i].股本 / 10); // 每股10元
			
			data[i].毛利率 = (float) data[i].毛利 / data[i].營收;
			data[i].營業利益率 = (float) data[i].營業利益 / data[i].營收;
			data[i].稅前淨利率 = (float) data[i].稅前淨利 / data[i].營收;
			data[i].稅後淨利率 = (float) data[i].稅後淨利 / data[i].營收;
			
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
		}
	}
	
	void importToDB(AnnualData[] data, int idxStart) throws SQLException {
		synchronized (stAnnual.AcquireLock()) {
			for (int i = idxStart; i < data.length; i++) {
				if (data[i] == null)
					continue;
				
				data[i].年報已計算 = true;
				stAnnual.setObject(data[i].年報已計算);
				stAnnual.setObject(data[i].股東權益);
				stAnnual.setObject(data[i].每股淨值);
				stAnnual.setObject(data[i].毛利率);
				stAnnual.setObject(data[i].營業利益率);
				stAnnual.setObject(data[i].稅前淨利率);
				stAnnual.setObject(data[i].稅後淨利率);
				stAnnual.setObject(data[i].總資產週轉率);
				stAnnual.setObject(data[i].權益乘數);
				stAnnual.setObject(data[i].業外收支比重);
				stAnnual.setObject(data[i].ROA);
				stAnnual.setObject(data[i].ROE);
				stAnnual.setObject(data[i].存貨周轉率);
				stAnnual.setObject(data[i].負債比);
				stAnnual.setObject(data[i].流動比);
				stAnnual.setObject(data[i].速動比);
	
				stAnnual.setObject(data[i].利息保障倍數);
				stAnnual.setObject(data[i].營業現金對流動負債比);
				stAnnual.setObject(data[i].營業現金對負債比);
				stAnnual.setObject(data[i].營業現金流對淨利比);
				stAnnual.setObject(data[i].自由現金流對淨利比);
	
				stAnnual.setObject(data[i].year);
				stAnnual.setObject(company.stockNum);
				stAnnual.addBatch();
			}
		}
	}
	
	@Override
	public void run() {
		final String uiMessage = String.format("年報運算: %d", company.stockNum);
		try {
			RebuildUI.addProcess(uiMessage);
			data = AnnualData.getAllCalcRefData(db, company.stockNum);
			
			if (data == null)
				return;
			
			int idxStart = 0;
			
			for (int i = 0; i < data.length; i++) {
				if (data[i] != null && data[i].年報已計算 == false) {
					idxStart = i;
					break;
				}
			}
			
			calc(data, idxStart);
			importToDB(data, idxStart);
			
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
				"StockNum IN (SELECT DISTINCT StockNum FROM annual Where 年報已計算 = 0)");
		
		if (companies == null || companies.length == 0) {
			RebuildUI.updateProgressBar(100);
			return;
		}
		totalImportCount = companies.length;
		RebuildUI.updateProgressBar(0);
		oldPercentage = 0;
		importedCount = 0;
	
		stAnnual = new MyStatement(db.conn);
		stAnnual.setStatementUpdate("annual", "Year=? AND StockNum=?", 
				"年報已計算", "股東權益", "每股淨值", "毛利率", "營業利益率", "稅前淨利率", "稅後淨利率", 
				"總資產週轉率", "權益乘數", "業外收支比重", "ROA", "ROE", "存貨周轉率", "負債比", 
				"流動比", "速動比", "利息保障倍數", "營業現金對流動負債比", "營業現金對負債比", "營業現金流對淨利比", 
				"自由現金流對淨利比");
		stAnnual.setBatchSize(500);
		
		MyThreadPool threadPool = new MyThreadPool();

		for (Company company : companies) {
			threadPool.add(new AnnualCalc(db, company));
		}

		threadPool.waitFinish();

		stAnnual.close();
	}
	
	public static void main(String[] args) throws Exception {
		MyDB db = new MyDB();
		log.info("Start!!");
		calculateAllCompanies(db);
		db.close();
		log.info("Done!!");
	}
}
