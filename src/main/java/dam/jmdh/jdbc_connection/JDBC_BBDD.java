package dam.jmdh.jdbc_connection;

import java.sql.*;

public class JDBC_BBDD {


    public static void main(String[] args) {

        // Datos de conexión. Adecuad a vuestra bbdd, usuario y password.

        String basedatos = "sakila";
        String host = "localhost";
        String port = "3306";
        //String parAdic = "?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC";
        String urlConnection = "jdbc:mysql://" + host + ":" + port + "/" + basedatos; // + parAdic;
        String user = "root";
        String pwd = "pass";

        //Class.forName("com.mysql.jdbc.Driver");    // No necesario desde SE 6.0
        //Class.forName("com.mysql.cj.jdbc.Driver"); // para MySQL 8.0, no necesario

        // Creamos conexión en bloque de recurso.
        try (Connection c = DriverManager.getConnection(urlConnection, user, pwd)) {
            System.out.println("Conexión realizada.");
            Statement statement = c.createStatement();
            ResultSet rs = statement.executeQuery("SELECT NOW();");
            rs.next();
            Timestamp fecha =rs.getTimestamp(1);
            System.out.println(fecha.toString());


            // *** Consulta con executeQuery
            System.out.println("Consulta con executeQuery");

            consulta(c);

            // *** Consulta con execute
            System.out.println("Consulta con execute");

            consulta2(c);

            // *** Consulta con ResulsetScrollable
            System.out.println("Consulta consultaResulsetScrillable");
            consultaResulsetScrollable(c);

            /* Solución Actividad 1:
            Haz un programa que muestre los mismos datos que la consulta anterior pero en orden
            inverso. La consulta de SQL debe ser la misma, sin ningún cambio.
             */
            System.out.println("En orden inverso");
            consultaAnteriorInversa(c);

            /* Solución Actividad 2:
            ¿Cómo se podría averiguar el número de filas obtenidas por una consulta utilizando los métodos de
            ResultSet, pero sin recorrer sus contenidos para contarlas? Escribe un programa que 1o haga.
             */
            System.out.println("Numero de filas: " + cuentaFilas(c));

        } catch (SQLException e) {
            System.out.println("SQL mensaje: " + e.getMessage());
            System.out.println("SQL Estado: " + e.getSQLState());
            System.out.println("SQL código específico: " + e.getErrorCode());
        } catch (Exception e) {
            e.printStackTrace(System.err);
        }
    }


    private static void consulta(Connection connection) throws SQLException {
        Statement s=connection.createStatement();
        String sql = "select * from actor";
        ResultSet rs= s.executeQuery(sql);
        while (rs.next()) {
            System.out.println(rs.getString("first_name"));
        }

    }

    private static void consulta2(Connection connection) throws SQLException {
        Statement s = connection.createStatement();
        String sql = "select * from actor;";

        if( s.execute(sql) ) {  // Si la ejecución genera un Resulset devuelve 0
            ResultSet rs = s.getResultSet();
            while (rs.next()) {
                System.out.println(rs.getString("first_name"));
            }
        }
        System.out.println();
    }


    private static void consultaResulsetScrollable(Connection connection) throws SQLException {
        // resultSetType - a result set type; one of ResultSet.TYPE_FORWARD_ONLY, ResultSet.TYPE_SCROLL_INSENSITIVE, or ResultSet.TYPE_SCROLL_SENSITIVE
        Statement statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        ResultSet rs = statement.executeQuery("select first_name from actor where first_name like 'A%';");
        while (rs.next()) {
            System.out.println(rs.getString(1));
        }

        if (rs.absolute(4)) {

            System.out.println("Cuarto actor: " + rs.getString(1));
        } else {
            System.out.println("Posición fuera de rango");
        }

    }

    private static void consultaAnteriorInversa(Connection c) throws SQLException {
        Statement statement = c.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        ResultSet rs = statement.executeQuery("select first_name from actor where first_name like 'A%';");
        rs.afterLast(); // Mueve cursor detrás del último registro
        while (rs.previous()){  // Recorre resulset hacia atrás
            System.out.println(rs.getString(1));
        }

    }

    private static int cuentaFilas (Connection c) throws SQLException {
        Statement statement = c.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        ResultSet rs = statement.executeQuery("select first_name from actor where first_name like 'A%';");
        return rs.last() ? rs.getRow() : 0;  // Números de filas



    }
}