import java.sql.*;  
import java.io.File;
import java.io.FilenameFilter;
import java.io.*; 
import java.nio.file.Files; 
import java.nio.file.*;  

class JavaCsv{  
public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, SQLException{  
    
Class.forName("com.mysql.cj.jdbc.Driver");  //com.mysql.jdbc.Driver
Connection con=DriverManager.getConnection("jdbc:mysql://localhost:3306/java","root","");    
Statement stmt=con.createStatement(); 
//stmt.executeUpdate("CREATE TABLE tv (EVT_DATE VARCHAR(255), IP_ADDRESS VARCHAR(255), ACTION_STATUS VARCHAR(255), HEXADECIMAL_MAC_ADDRESS VARCHAR(255), SERVICE VARCHAR(255), CHANNEL VARCHAR(255), MINS_VIEW_CHANNEL VARCHAR(255), MINS_BOX_LAST_ACTION VARCHAR(255), PROGRAM_NAME VARCHAR(255), DELIVERY_FILE_NAME VARCHAR(255), TVBOXID VARCHAR(255), MB VARCHAR(255), GENDER VARCHAR(255), AREA VARCHAR(255), AGE VARCHAR(255))");
while(true){  
            File folder = new File("C:\\dataset"); //Implementing FilenameFilter to retrieve only txt files
            FilenameFilter txtFileFilter;
            txtFileFilter = new FilenameFilter() 
            {
                @Override
                public boolean accept(File dir, String name)
                {
                    return name.endsWith(".csv");
                }
            };
            File[] files = folder.listFiles(txtFileFilter); 
      
        for (File file : files){
        stmt.executeUpdate("LOAD DATA INFILE'/dataset/" + file.getName() + "'" + "INTO TABLE tv FIELDS TERMINATED BY ',' IGNORE 1 LINES;");
        Files.move(Paths.get("C:\\dataset\\"+file.getName()), Paths.get("C:\\dataset\\backup\\"+file.getName()), StandardCopyOption.REPLACE_EXISTING);
        }
    Thread.sleep(30);
    }
}  
}  
