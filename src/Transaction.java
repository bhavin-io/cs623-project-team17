import java.io.IOException;
import java.sql.*;
import java.util.Scanner;

/*************************************************************************
 *
 *  Pace University
 *  Spring 2023
 *  DBMS PROJECT
 *  Course: CS 623
 *
 * Group Members: Bhavin Himatkumar Goswami(CRN: 23704), Ananthula Sai Vyshnav(CRN: 23704), Lokeshwar Anchuri(CRN: 23704)
 *
 * Tasks Performed:
 * 2. The depot d1 is deleted from Depot and Stock.
 * 4. The depot d1 changes its name to dd1 in Depot and Stock.
 * 6. We add a depot (d100, Chicago, 100) in Depot and (p1, d100, 100) in Stock.
 *************************************************************************/

public class Transaction {
    public static void main(String args[]) throws SQLException, IOException,
            ClassNotFoundException {
        //initializing selected option
        int selected_option = 0;
        // Load the PostgreSQL driver
        Class.forName("org.postgresql.Driver");

        // Connect to the default database with credentials
        Connection conn = DriverManager.getConnection("jdbc:postgresql://localhost:5432/cs623", "postgres", "Bhavin@root");

        // For atomicity
        conn.setAutoCommit(false);

        // For isolation
        conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
        Statement stmt_obj = null;

        //DROP TABLE IF EXISTS
        String dropIfExist = "DROP TABLE IF EXISTS Product, Depot, Stock;";
        //Create table if not exist
        String createIfNot = "CREATE TABLE IF NOT EXISTS Product(prodid CHAR(10), pname VARCHAR(30), price DECIMAL);" +
                "CREATE TABLE IF NOT EXISTS Depot(depid CHAR(10), addr VARCHAR(40), volume INTEGER);" +
                "CREATE TABLE IF NOT EXISTS Stock(prodid CHAR(10), depid CHAR(10), quantity INTEGER);";

        //Alter table set constraints like pk,fk,ck and cascading
        String alterCasc = "ALTER TABLE Product ADD CONSTRAINT pk_product PRIMARY KEY(prodid);" +
                "ALTER TABLE Depot ADD CONSTRAINT pk_depot PRIMARY KEY(depid);" +
                "ALTER TABLE Stock ADD CONSTRAINT pk_stock PRIMARY KEY(prodid,depid);" +
                "ALTER TABLE Stock ADD CONSTRAINT fk_stock_product_id FOREIGN KEY(prodid) REFERENCES Product(prodid);" +
                "ALTER TABLE Stock ADD CONSTRAINT fk_stock_depot_id FOREIGN KEY(depid) REFERENCES Depot(depid)ON DELETE CASCADE ON UPDATE CASCADE;;";
        //Insert Data in Product, Depot & Stock
        String insertData = "INSERT INTO Product(prodid, pname, price) VALUES" +
                "('p1','tape',2.5)," +
                "('p2','tv',250)," +
                "('p3','vcr',80);" +
                "INSERT INTO Depot(depid, addr, volume) VALUES" +
                "('d1','New York',9000)," +
                "('d2','Syracuse',6000)," +
                "('d4','New York',2000);" +
                "INSERT INTO Stock(prodid, depid, quantity) VALUES" +
                "('p1','d1',1000)," +
                "('p1','d2',-100)," +
                "('p1','d4',1200)," +
                "('p3','d1',3000)," +
                "('p3','d4',2000)," +
                "('p2','d4',1500)," +
                "('p2','d1',-400)," +
                "('p2','d2',2000);";

        //initializing DB with Tables, Data and setting constraints
        try {
            // Create statement object
            stmt_obj = conn.createStatement();

            //EXECUTING Drop table if exist
            //EXECUTING Create table if not exist
            //EXECUTING Insert data in table
            //EXECUTING Alter table and adding cascading
            stmt_obj.executeUpdate(dropIfExist);
            stmt_obj.executeUpdate(createIfNot);
            stmt_obj.executeUpdate(insertData);
            stmt_obj.executeUpdate(alterCasc);
            System.out.println("\n###Tables Product, Depot & Stock Created, We are all set to perform tasks!!###");
        } catch (SQLException e) {
            System.out.println("\n###Tables already exists!###");
        }
        conn.commit();//committing changes if the table is created
        stmt_obj.close();

        //Menu Driven Loop
        while (true) {
            stmt_obj = null;// Resetting statement object to null
            //Scanner for menu input
            Scanner input_option = new Scanner(System.in);
            System.out.println("\nSelect from the following menu:(Enter the task no: to be performed)\n");
            System.out.println("1. The depot 'd2' changes its name to 'dd1' in Depot and Stock\n" +
                    "2. The depot 'd1' is deleted from 'Depot' and 'Stock'\n" +
                    "3. Add a 'depot' (d100, Chicago, 100) in 'Depot' and (p1, d100, 100) in 'Stock'\n" +
                    "4. Retrive all the tables\n"+"5.Exit\n");
            selected_option = input_option.nextInt();
            if (selected_option != 4) {
                System.out.println("##################################################################################");
                System.out.println("Performing Query...........");
            }
            //Switch case on user input
            switch (selected_option) {
                case 1:
                    //Update case Task 2
                    System.out.println("Updating 'd2' to 'dd1' in Depot and Stock!");
                    try {
                        stmt_obj = conn.createStatement();
                        //We have intentionally set depid to 'd2' since we have a task 4 which is to delete d1 from depot and stock
                        // We can re-insert d1 to the table again for achieving task 4 by running the below code snippets to insert d1 again to the db
                        // stmt_obj.executeUpdate("INSERT INTO Depot(depid, addr, volume) VALUES ('d1','New York',9000);");
                        // stmt_obj.executeUpdate("INSERT INTO Stock(prodid, depid, quantity) VALUES" +"('p1','d1',1000)," +"('p3','d1',3000)," +"('p2','d1',-400);");

                        stmt_obj.executeUpdate("UPDATE Depot SET depid = 'dd1' WHERE depid = 'd2';");
                        conn.commit();
                        stmt_obj.close();
                    } catch (SQLException e) {
                        System.out.println("An exception was thrown" + e);
                        System.out.println("Rolling back..............!");
                        // For atomicity
                        conn.rollback();
                        stmt_obj.close();
                    }


                    break;
                case 2:
                    //Delete case Task 4
                    System.out.println("Deleting 'd1' from Depot and Stock!");
                    try {
                        stmt_obj = conn.createStatement();
                        //We need to reinsert d1 in order to Delete d1 from table if we perform Update query on d1 first!
                        // Execute Insert CODE SNIPPETS HERE
                        stmt_obj.executeUpdate("DELETE FROM depot WHERE depid ='d1';");
                        conn.commit();
                        stmt_obj.close();
                    } catch (SQLException e) {
                        System.out.println("An exception was thrown" + e.getMessage());
                        System.out.println("Rolling back..............!");
                        // For atomicity
                        conn.rollback();
                        stmt_obj.close();
                    }

                    break;

                case 3:
                    //Insert case Task 6
                    System.out.println("Adding a depot (d100, Chicago, 100) in Depot and (p1, d100, 100) in Stock.");
                    try {
                        stmt_obj = conn.createStatement();
                        stmt_obj.executeUpdate("INSERT INTO Depot (depid, addr, volume) VALUES ('d100', 'Chicago', 100);");
                        stmt_obj.executeUpdate("INSERT INTO Stock (prodid, depid, quantity) VALUES ('p1', 'd100', 100);");
                        conn.commit();
                        stmt_obj.close();
                    } catch (SQLException e) {
                        System.out.println("An exception was thrown" + e.getMessage());
                        System.out.println("Rolling back..............!");
                        // For atomicity
                        conn.rollback();
                        stmt_obj.close();
                    }

                    break;
                case 4:
                    //Retrieving table data from postgres
                    System.out.println("Retrieve all the tables from Database");
                    try {
                        stmt_obj = conn.createStatement();
                        ResultSet rs_product = stmt_obj.executeQuery("SELECT * FROM Product;");
                        System.out.println("###############################");
                        System.out.println("Table Product");
                        while (rs_product.next()) {
                            String product_id = rs_product.getString("prodid").trim();
                            String product_name = rs_product.getString("pname".trim());
                            float product_price = rs_product.getInt("price");
                            System.out.println("prodid: " + product_id + ", pname: " + product_name + ", price: " + product_price);
                        }

                        rs_product.close();

                        ResultSet rs_depot = stmt_obj.executeQuery("SELECT * FROM Depot;");
                        System.out.println("###############################");
                        System.out.println("Table Depot");

                        while (rs_depot.next()) {
                            String depot_id = rs_depot.getString("depid").trim();
                            String depot_addr = rs_depot.getString("addr").trim();
                            int depot_volume = rs_depot.getInt("volume");
                            System.out.println("depid: " + depot_id + ", addr: " + depot_addr + ", volume: " + depot_volume);
                        }

                        rs_depot.close();

                        ResultSet rs_stock = stmt_obj.executeQuery("SELECT * FROM Stock;");
                        System.out.println("###############################");
                        System.out.println("Table Stock");

                        while (rs_stock.next()) {
                            String stock_product_id = rs_stock.getString("prodid").trim();
                            String stock_depid_id = rs_stock.getString("depid").trim();
                            int stock_quantity = rs_stock.getInt("quantity");
                            System.out.println("prodid: " + stock_product_id + ", depid: " + stock_depid_id + ", quantity: " + stock_quantity);
                        }

                        rs_stock.close();

                        conn.commit();
                        stmt_obj.close();
                    } catch (SQLException e) {
                        System.out.println("An exception was thrown" + e.getMessage());
                        // For atomicity
                        conn.rollback();
                        stmt_obj.close();
                    }

                    break;
                case 5:
                    //Exit
                    System.out.println("Program Terminated! Good Bye!");
                    stmt_obj.close();
                    conn.close();
                    break;
                default:
                    System.out.println("Invalid choice");
                    stmt_obj.close();
                    conn.close();
            }
        }

    }
}