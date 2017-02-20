
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

// H2 In-Memory Database Example shows about storing the database contents into memory. 

public class H2MemoryDatabaseExample {

    private static final String DB_DRIVER = "org.h2.Driver";
    private static final String DB_CONNECTION = "jdbc:h2:~/data/test;AUTO_SERVER=TRUE";
    private static final String DB_USER = "";
    private static final String DB_PASSWORD = "";

    public static void main(String[] args) throws Exception {
        try {
            executeQueries();

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void executeQueries() throws SQLException {
        Connection connection = getDBConnection();
        Statement stmt = connection.createStatement();
        
        try {
            connection.setAutoCommit(false);
    		stmt.execute("CREATE TABLE PERSON(ID_ varchar(255), name varchar(255), primary key (ID_))");
    	
    		stmt.execute("INSERT INTO PERSON(ID_, name) VALUES('1', 'Anju')");
    		stmt.execute("INSERT INTO PERSON(ID_, name) VALUES('2', 'Sonia')");
    		stmt.execute("INSERT INTO PERSON(ID_, name) VALUES('3', 'Asha')");

     		java.sql.ResultSet rs = stmt.executeQuery("select * from PERSON");

    		while (rs.next()) {
    			System.out.println("ID_ " + rs.getInt("ID_") + " Name " + rs.getString("name"));
    		}
            connection.commit();
        } catch (SQLException e) {
            System.out.println("Exception Message " + e.getLocalizedMessage());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            connection.close();
        }
    }

    private static Connection getDBConnection() {
        Connection dbConnection = null;
        try {
            Class.forName(DB_DRIVER);
        } catch (ClassNotFoundException e) {
            System.out.println(e.getMessage());
        }
        try {
            dbConnection = DriverManager.getConnection(DB_CONNECTION, DB_USER, DB_PASSWORD);
            return dbConnection;
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        return dbConnection;
    }
}
