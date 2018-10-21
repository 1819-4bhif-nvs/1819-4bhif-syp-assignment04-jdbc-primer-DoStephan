package at.htl.hotel;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import javax.swing.plaf.nimbus.State;
import java.sql.*;

import static junit.framework.TestCase.fail;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class HotelTest {

    public static final String DRIVER_STRING ="org.apache.derby.jdbc.ClientDriver";
    public static final String CONNECTION_STRING ="jdbc:derby://localhost:1527/db";
    public static final String USER ="app";
    public static final String PASSWORD ="app";
    private static Connection conn;

    @BeforeClass
    public static void initJdbc(){
        try {
            Class.forName(DRIVER_STRING);
            conn = DriverManager.getConnection(CONNECTION_STRING, USER, PASSWORD);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            System.out.println("Verbindung zur DB nicht möglich:\n"
                    + e.getMessage()+"\n");
            System.exit(1);
        }
    }

    @AfterClass
    public static void teardownJdbc(){

        try{
            conn.createStatement().execute("DROP TABLE tourist");
            System.out.println("Tabelle tourist gelöscht");
        } catch (SQLException e) {
            System.out.println("Problem beim Löschen von tourist"+ e.getMessage());
        }
        try{
            conn.createStatement().execute("DROP TABLE hotel");
            System.out.println("Tabelle HOTEL gelöscht");
        } catch (SQLException e) {
            System.out.println("Problem beim Löschen von HOTEL"+ e.getMessage());
        }


        try {
            if(conn != null || !conn.isClosed()){
                conn.close();
                System.out.println("Goodbye");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    @Test
    public void T01createTables(){
        try {
            Statement stmt = conn.createStatement();

            String sql = "CREATE TABLE hotel (" +
                    "id INT CONSTRAINT hotel_pk PRIMARY KEY," +
                    "name VARCHAR(255) NOT NULL, " +
                    "stars INT NOT NULL," +
                    "location VARCHAR(255)," +
                    "price_per_night INT NOT NULL)";

            stmt.execute(sql);

            sql = "CREATE TABLE tourist (" +
                    "id INT CONSTRAINT tourist_pk PRIMARY KEY," +
                    "name VARCHAR(255) NOT NULL, " +
                    "hotel_id INT NOT NULL," +
                    "CONSTRAINT hotel_id_fk FOREIGN KEY(hotel_id) REFERENCES hotel(id))";
            stmt.execute(sql);
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }
    @Test
    public void T03insertTourist(){
        int count = 0;
        try {

            Statement stmt = conn.createStatement();
            String sql = "INSERT INTO tourist (id, name, hotel_id) " +
                    "VALUES (1, 'Hans', 2)";
            count += stmt.executeUpdate(sql);
            sql = "INSERT INTO tourist (id, name, hotel_id) " +
                    "VALUES (2, 'Karl', 1)";
            count += stmt.executeUpdate(sql);
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }
        assertThat(count, is(2));

    }
    @Test
    public void T02insertHotel(){
        int count = 0;
        try {

            Statement stmt = conn.createStatement();

            String sql = "INSERT INTO hotel (id, name, stars , location, price_per_night) " +
                    "VALUES (1, 'Neutor', 4, 'Innsbruck', 250)";
            count += stmt.executeUpdate(sql);
            sql = "INSERT INTO hotel (id, name, stars , location, price_per_night) " +
                    "VALUES (2, 'porta maggiore', 3, 'Rom', 200)";
            count += stmt.executeUpdate(sql);
            sql = "INSERT INTO hotel (id, name, stars , location, price_per_night) " +
                    "VALUES (3, 'Gerstner', 4, 'Wien', 150)";
            count += stmt.executeUpdate(sql);
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        assertThat(count,is(3));
    }

    @Test
    public void T04testHotel(){

        try {
            PreparedStatement pstmt = conn.prepareStatement
                    ("SELECT id, name, stars, location, price_per_night FROM hotel");
            ResultSet rs =  pstmt.executeQuery();

            rs.next();
            assertThat(rs.getString("NAME"),is("Neutor"));
            assertThat(rs.getString("STARS"), is("4"));
            assertThat(rs.getString("LOCATION"),is("Innsbruck"));
            assertThat(rs.getString("PRICE_PER_NIGHT"),is("250"));

            rs.next();
            assertThat(rs.getString("NAME"),is("porta maggiore"));
            assertThat(rs.getString("STARS"), is("3"));
            assertThat(rs.getString("LOCATION"),is("Rom"));
            assertThat(rs.getString("PRICE_PER_NIGHT"),is("200"));

            rs.next();
            assertThat(rs.getString("NAME"),is("Gerstner"));
            assertThat(rs.getString("STARS"), is("4"));
            assertThat(rs.getString("LOCATION"),is("Wien"));
            assertThat(rs.getString("PRICE_PER_NIGHT"),is("150"));

        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }
    @Test
    public void T05testTourist(){

        try {
            PreparedStatement pstmt = conn.prepareStatement
                    ("SELECT id, name, hotel_id FROM tourist ");
            ResultSet rs =  pstmt.executeQuery();

            rs.next();
            assertThat(rs.getString("NAME"),is("Hans"));
            assertThat(rs.getString("hotel_id"), is("2"));

            rs.next();
            assertThat(rs.getString("NAME"),is("Karl"));
            assertThat(rs.getString("hotel_id"),is("1"));

        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }

    }
    @Test
    public void T06testMetaDataHotel(){
        try {
            DatabaseMetaData databaseMetaData = conn.getMetaData();
            String catalog = null;
            String schemaPattern = null;
            String tableNamePattern = "HOTEL";
            String columnNamePattern = null;

            ResultSet rs = databaseMetaData.getColumns(catalog, schemaPattern, tableNamePattern, columnNamePattern);

            rs.next();
            String columnName = rs.getString(4);
            int columnType = rs.getInt(5);
            assertThat(columnName, is("ID"));
            assertThat(columnType, is(Types.INTEGER));

            rs.next();
            columnName = rs.getString(4);
            columnType = rs.getInt(5);
            assertThat(columnName, is("NAME"));
            assertThat(columnType, is(Types.VARCHAR));

            rs.next();
            columnName = rs.getString(4);
            columnType = rs.getInt(5);
            assertThat(columnName, is("STARS"));
            assertThat(columnType, is(Types.INTEGER));

            rs.next();
            columnName = rs.getString(4);
            columnType = rs.getInt(5);
            assertThat(columnName, is("LOCATION"));
            assertThat(columnType, is(Types.VARCHAR));

            rs.next();
            columnName = rs.getString(4);
            columnType = rs.getInt(5);
            assertThat(columnName, is("PRICE_PER_NIGHT"));
            assertThat(columnType, is(Types.INTEGER));

            String schema = null;
            String tableName = "HOTEL";

            rs = databaseMetaData.getPrimaryKeys(
                    catalog, schema, tableName);

            rs.next();
            columnName = rs.getString(4);
            assertThat(columnName, is("ID"));

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    @Test
    public void T07testMetaDataTourist(){
        try {
            DatabaseMetaData databaseMetaData = conn.getMetaData();
            String catalog = null;
            String schemaPattern = null;
            String tableNamePattern = "TOURIST";
            String columnNamePattern = null;

            ResultSet rs = databaseMetaData.getColumns(catalog, schemaPattern, tableNamePattern, columnNamePattern);

            rs.next();
            String columnName = rs.getString(4);
            int columnType = rs.getInt(5);
            assertThat(columnName, is("ID"));
            assertThat(columnType, is(Types.INTEGER));

            rs.next();
            columnName = rs.getString(4);
            columnType = rs.getInt(5);
            assertThat(columnName, is("NAME"));
            assertThat(columnType, is(Types.VARCHAR));

            rs.next();
            columnName = rs.getString(4);
            columnType = rs.getInt(5);
            assertThat(columnName, is("HOTEL_ID"));
            assertThat(columnType, is(Types.INTEGER));

            String schema = null;
            String tableName = "TOURIST";

            rs = databaseMetaData.getPrimaryKeys(
                    catalog, schema, tableName);

            rs.next();
            columnName = rs.getString(4);
            assertThat(columnName, is("ID"));

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
