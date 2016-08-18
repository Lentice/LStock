package lstockv2;
import java.io.File;
import java.util.Scanner;

public class Test {
		
	public static void showFiles(File[] files) throws Exception {
	    for (File file : files) {
	        if (file.isDirectory()) {
	            System.out.println("Directory: " + file.getName());
	            showFiles(file.listFiles()); // Calls same method again.
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
	
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		System.out.println("Directory: ");
		File[] files = new File("D:\\My Documents\\桌面\\Test").listFiles();
	    showFiles(files);
	}

}
