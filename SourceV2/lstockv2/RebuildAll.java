package lstockv2;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class RebuildAll {
	private static final Logger log = LogManager.getLogger(lstockv2.RebuildAll.class.getName());
	
	
	public static void main(String[] args) throws Exception {
		MyDB db = new MyDB();
		
		log.info("Import Daily Trade");
		ImportDailyTrade.supplementDB(db);
		
		log.info("Import Daily TaiEx Price");
		ImportMonthlyTaiExPrice.supplementDB(db);
		
		log.info("Import Monthly Revenue");
		ImportMonthlyRevenue.supplementDB(db);
		
		log.info("Import Quarterly Tables");
		ImportQuarterly.supplementDB(db);
		ImportQuarterly.supplementOldCashflow(db);
		
		log.info("Import Annual Dividend");
		ImportAnnualDividend.supplementDB(db);
		
		db.close();
		log.info("Done !!");
	}
}
