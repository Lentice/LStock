package lstockv2;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Scanner;

public class Test {
	public static void checkValidQuarterlyTable(File[] files) throws Exception {
	    for (File file : files) {
	        if (file.isDirectory()) {
	            System.out.println("Directory: " + file.getName());
	            checkValidQuarterlyTable(file.listFiles()); // Calls same method again.
	        } else {
	            System.out.println("File: " + file.getName());
	            
	            /*
	             * 外國發行人免申報個別財務報表資訊，請至合併財務報表查詢
					該 1311 上市公司已下市！
					此公司代號不存在或公司未申報基本資料！
					
					<<< 重新查詢 >>>
					查詢過於頻繁，請於稍後再查詢!
					
					<<<< 不正確 >>>>
					第二上市(櫃)公司免申報本項財務報表資訊，請至簡明合併財務報表及財務報告電子書查詢
					第二上市（櫃）公司免申報本項財務報表資訊，請至簡明合併財務報表及財務報告電子書查詢。
					第二上市（櫃）公司請循「投資專區/臺灣存託憑證專區/臺灣存託憑證代號總表」，<br>點選「存託憑證代號」項下代號，再點選”財務報告書”及”簡明財務報表”查詢</font></h4>
					
					<< 例外 >>
					現金流量表 9918_2010_3
	             * 
	             */
	            
	            if (!file.isFile() || file.length() > 5 * 1024)
	    			continue;
	            
	            final Scanner scanner = new Scanner(file);
	            while (scanner.hasNextLine()) {
	               final String lineFromFile = scanner.nextLine();
	               if(lineFromFile.contains("查無需求資料")
	            		   || lineFromFile.contains("查無所需資料")
	            		   || lineFromFile.contains("不繼續公開發行") 
	            		   || lineFromFile.contains("請至合併財務報表查詢")
	            		   || lineFromFile.contains("公司已下市")
	            		   || lineFromFile.contains("無應編製合併財報之子公司")
	            		   || lineFromFile.contains("此公司代號不存")
	            		   || lineFromFile.contains("查詢過於頻繁")
	            		   || lineFromFile.contains("第二上市（櫃）")
	            		   || lineFromFile.contains("第二上市(櫃)")) { 
	                   // a match!
	            	   scanner.close();
	            	   
	            	   if(file.delete()){
	            		   System.out.println(file.getName() + " is deleted!");
            		   }else{
            			   System.out.println("Delete operation is failed.");
            		   }
	                   
	                   break;
	               }
	            }
	            
	        }
	    }
	}
	
	@SuppressWarnings("unused")
	private static void checkQuarterly備供出售金融資產(File[] files) throws IOException {
		for (File file : files) {
			if (file.isDirectory()) {
				System.out.println("Directory: " + file.getName());
				checkQuarterly備供出售金融資產(file.listFiles()); // Calls same method
														// again.
			} else {
//				System.out.println(file.getPath() + file.getName() + "  " + file.length());
				
				final Scanner scanner = new Scanner(file);
	            while (scanner.hasNextLine()) {
	               final String lineFromFile = scanner.nextLine();
	               if(lineFromFile.contains("備供出售金融資產")) { 
	            	   if (lineFromFile.contains("備供出售金融資產－非流動淨額") || lineFromFile.contains("備供出售金融資產－非流動"))
	            		   continue;
	            	   
	            	   if (lineFromFile.contains("備供出售金融資產－流動淨額") || lineFromFile.contains("備供出售金融資產－流動"))
	            		   continue;
	            	   
	            	   if (lineFromFile.contains("備供出售金融資產-非流動淨額") || lineFromFile.contains("備供出售金融資產-非流動"))
	            		   continue;
	            	   
	            	   if (lineFromFile.contains("備供出售金融資產淨額-非流動淨額") || lineFromFile.contains("備供出售金融資產淨額-非流動"))
	            		   continue;
	            	   
	            	   if (lineFromFile.contains("備供出售金融資產-流動淨額") || lineFromFile.contains("備供出售金融資產-流動"))
	            		   continue;
	            	   
	            	   if (lineFromFile.contains("備供出售金融資產-流動淨額") || lineFromFile.contains("備供出售金融資產-流動"))
	            		   continue;
	            	   
	            	   if (lineFromFile.contains("備供出售金融資產未實現損益") || lineFromFile.contains("備供出售金融資產未實利益（損失"))
	            		   continue;
	            	   
	            	   System.out.println(lineFromFile);
	                   break;
	               }
	            }
	            
	            scanner.close();
			}
		}
	}
	
	@SuppressWarnings("unused")
	private static void checkQuarterly投資性不動產淨額(File[] files) throws IOException {
		for (File file : files) {
			if (file.isDirectory()) {
				System.out.println("Directory: " + file.getName());
				checkQuarterly投資性不動產淨額(file.listFiles()); // Calls same method
														// again.
			} else {
//				System.out.println(file.getPath() + file.getName() + "  " + file.length());
				
				final Scanner scanner = new Scanner(file);
	            while (scanner.hasNextLine()) {
	               final String lineFromFile = scanner.nextLine();
	               if(lineFromFile.contains("投資性不動產淨額")) { 
	            	   if (lineFromFile.contains("投資性不動產淨額</td>"))
	            		   break;
	            	   
	            	   System.out.println(lineFromFile);
	                   break;
	               }
	            }
	            scanner.close();
			}
		}
	}
	
	private static void checkQuarterly負債總額(File[] files) throws IOException {
		for (File file : files) {
			if (file.isDirectory()) {
				System.out.println("Directory: " + file.getName());
				checkQuarterly負債總額(file.listFiles()); // Calls same method
														// again.
			} else {
//				System.out.println(file.getPath() + file.getName() + "  " + file.length());
				
				final Scanner scanner = new Scanner(file);
	            while (scanner.hasNextLine()) {
	               final String lineFromFile = scanner.nextLine();
	               if(lineFromFile.contains("負債總額") || lineFromFile.contains("負債總計") || lineFromFile.contains("負債合計")) { 
	            	   if (lineFromFile.contains("負債總額</td>") || lineFromFile.contains("負債總計</td>") || lineFromFile.contains("負債合計</td>"))
	            		   break;
	            	   
	            	   System.out.println(lineFromFile);
	                   break;
	               }
	            }
	            scanner.close();
			}
		}
	}
	
	private static void checkQuarterly現金流字串() throws IOException {
		File[] files = new File("D:\\Dropbox\\Stock\\Code\\LStock\\Data\\季_現金流量表\\").listFiles();
		HashMap<String, Integer> hash = new HashMap<>();
		for (File file : files) {
			if (file.isDirectory()) {
				System.out.println("Directory: " + file.getName());
				checkQuarterly負債總額(file.listFiles()); // Calls same method
														// again.
			} else {
//				System.out.println(file.getPath() + file.getName() + "  " + file.length());
				
				final Scanner scanner = new Scanner(file);
	            while (scanner.hasNextLine()) {
	               final String lineFromFile = scanner.nextLine();
	               if(lineFromFile.contains("營業活動")) {
	            	   
	            	   String[] splitStr = lineFromFile.split("\\s+");
	            	   for(String data: splitStr) {
	            		   if(data.contains("營業活動")) {
	            			   if (!hash.containsKey(HtmlUtil.trim(data))) {
	            				   hash.put(HtmlUtil.trim(data), new Integer(0));
	            				   System.out.println(data);

	        	            	   break;
	            			   }
	            		   }
	            	   }
	                   break;
	               }
	            }
	            scanner.close();
			}
		}
	}
	
	private static void checkQuarterly損益() throws IOException {
		File[] files = new File("D:\\Dropbox\\Stock\\Code\\LStock\\Data\\季_綜合損益表\\").listFiles();

		HashMap<String, Integer> hash = new HashMap<>();
		for (File file : files) {
			if (file.isDirectory()) {
				System.out.println("Directory: " + file.getName());
				checkQuarterly負債總額(file.listFiles()); // Calls same method
														// again.
			} else {
//				System.out.println(file.getPath() + file.getName() + "  " + file.length());
				
				final Scanner scanner = new Scanner(file);
	            while (scanner.hasNextLine()) {
	               final String lineFromFile = scanner.nextLine();
	               if(lineFromFile.contains("繼續營業")) {
	            	   
	            	   String[] splitStr = lineFromFile.split("\\s+");
	            	   for(String data: splitStr) {
	            		   if(data.contains("繼續營業")) {
	            			   if (!hash.containsKey(HtmlUtil.trim(data))) {
	            				   hash.put(HtmlUtil.trim(data), new Integer(0));
	            				   System.out.println(data);

	        	            	   break;
	            			   }
	            		   }
	            	   }
	                   break;
	               }
	            }
	            scanner.close();
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		System.out.println("Start");
		
	    //checkValidQuarterlyTable(files);
		//checkQuarterly負債總額(files);
//		checkQuarterly現金流字串();
		checkQuarterly損益();
		
//		String test = "營業活動之淨現金流入（流出）(流出)";
//		test = test.replaceAll("（|）|\\(|\\)", "");
//		System.out.println(test);
		System.out.println("Done!!");
	}

	

}
