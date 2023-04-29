import java.io.IOException;
import java.sql.*;
/**
 * Sample of JDBC for PostgreSQL ACID is implemented
 */

public class Transaction {
    public static void main(String args[]) throws SQLException, IOException,
            ClassNotFoundException {

        // Load the PostgreSQL driver
        Class.forName("org.postgresql.Driver");

        // Connect to the default database with credentials
        Connection conn = DriverManager.getConnection("jdbc:postgresql://localhost:5432/postgres", "postgres", "Chochi#2000");

        // For atomicity
        conn.setAutoCommit(false);

        // For isolation
        conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);

        Statement stmt1 = null;
        try {
            // Create statement object
            stmt1 = conn.createStatement();

            String sql = "DELETE FROM depot WHERE dep_id ='d1';" +
                    "DELETE FROM stock WHERE dep_id ='d1';";
            stmt1.executeUpdate(sql);


            // Maybe a table student1 exist, maybe not
            // create table student(id integer, name varchar(10), primary key(Id))
            // Either the 2 following inserts are executed, or none of them are. This is
            // atomicity.
            //stmt1.executeUpdate("insert into student values (1, 'stud1')");
            //stmt1.executeUpdate("insert into student values (2, 'stud2')");
        } catch (SQLException e) {
            System.out.println("An exception was thrown"  + e.getMessage());
            // For atomicity
            conn.rollback();
            stmt1.close();
            conn.close();
            return;
        }
        conn.commit();
        stmt1.close();
        conn.close();
    }
}