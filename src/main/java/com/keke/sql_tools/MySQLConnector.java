package com.keke.sql_tools;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import java.io.File;
import java.sql.*;

/**
 * Created by KEKE on 2019/1/4
 */
public class MySQLConnector {

    private String db_url;
    private String user;
    private String pass;
    private Connection conn;

    public MySQLConnector(){

        SAXReader reader = new SAXReader();
        File config = new File(".conf");
        try {
            Document document = reader.read(config);
            Element mysqlInfo = document.getRootElement().element("mysql");
            db_url = "jdbc:mysql://"+mysqlInfo.attributeValue("url")+"/"+mysqlInfo.attributeValue("database");
            user = mysqlInfo.attributeValue("user");
            pass = mysqlInfo.attributeValue("password");

            // 注册jdbc驱动
            Class.forName("com.mysql.jdbc.Driver");

            // 打开连接
            conn = DriverManager.getConnection(db_url, user, pass);

        } catch (DocumentException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }



    public Connection getConn(){
        return conn;
    }

    public void destroy(){
        try {
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {

        System.out.println(System.getProperty("user.dir"));

        MySQLConnector connector = new MySQLConnector();
        Statement stmt = connector.getConn().createStatement();
        ResultSet rs = stmt.executeQuery("SELECT * FROM test");
        while (rs.next()){
            System.out.println(rs.getInt("id")+" "+rs.getString("content"));
        }
        rs.close();
        stmt.close();
        connector.destroy();
    }
}
