package com.keke.data_processing;

import com.keke.sql_tools.MySQLConnector;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import java.io.File;
import java.io.FileNotFoundException;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by KEKE on 2019/1/4
 */
public class CorpusBuilder {

    /**
     * 从 .conf 文件中的文件路径中获取txt原始文件
     * @param listFileNames
     */
    private static void getFileNames(ArrayList<String> listFileNames){

        SAXReader reader = new SAXReader();
        File config = new File(".conf");
        try {
            Document document = reader.read(config);
            Element orgCorpusInfo = document.getRootElement().element("origin-corpus");
            File path = new File(orgCorpusInfo.attributeValue("path"));
            File[] files = path.listFiles();
            for (int i=0;i<files.length;i++){
                if (!files[i].isDirectory()){
                    listFileNames.add(files[i].getAbsolutePath());
                }
            }
        } catch (DocumentException e) {
            e.printStackTrace();
        }
    }

    /**
     * 将 .conf 中路径中的原始txt文件中的信息存储到服务器Mysql数据库中的Corpus表
     */
    public static void upload2CorpusTable(){

        ArrayList<String> fileNames = new ArrayList<String>();
        getFileNames(fileNames);
        MySQLConnector connector = new MySQLConnector();
//        for (String txtName: fileNames){
        for(int i=0;i<fileNames.size();i++){
            String txtName = fileNames.get(i);

            try {
                File txtFile = new File(txtName);
                Scanner in = new Scanner(txtFile);
                StringBuilder stringBuilder = new StringBuilder();
                while (in.hasNextLine()){
                    stringBuilder.append(in.nextLine()+" ");
                }
                // 去掉文件开始的 \uFEFF 字符
                stringBuilder.deleteCharAt(0);
                if (stringBuilder.charAt(0)=='[')
                    stringBuilder.deleteCharAt(0);
                // 去掉文件中出现的 &nbsp 字符
                String originContent = stringBuilder.toString().replaceAll("\u00A0", " ")
                        .replaceAll("\\\\s+", " ").trim();
                String pattern = "^([^-=].*[^-=])-{20,}([^-=].*[^-=])={20,}(.*)$";
                Pattern r = Pattern.compile(pattern);
                Matcher m = r.matcher(originContent);
                if (m.find()){
                    String title = m.group(1).trim();
                    String origin = m.group(2).trim();
                    String[] annotations = m.group(3).trim().split("-{20,}");
                    String annotation;
                    String keywords;
                    if (annotations.length==2)
                        annotation = annotations[0].trim() + " " + annotations[1].trim();
                    else
                        annotation = annotations[0];
                    r = Pattern.compile(".*(KEYWORDS|keywords|Keywords):?(.*)$");
                    m =r.matcher(origin);
                    if (m.find())
                        keywords = m.group(2).trim();
                    else
                        keywords = "";
//                    MySQLConnector connector = new MySQLConnector();
                    new corpusTableRecord(title,keywords,origin,annotation).upload(connector);
                    System.out.println("File "+(i+1)+" upload success");
                }else
                    System.out.println("No match");

            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }
//        System.out.println("upload success");
        connector.destroy();
    }

    private static class corpusTableRecord{

        private String title;
        private String keywords;
        private String org_content;
        private String anno_content;
        private String update_time;

        public corpusTableRecord(String title, String keywords, String org_content, String anno_content){

            // 将原文中的单引号替换成 \' 防止sql语句语法错误
            this.title = title.replaceAll("'","\\\\'");
            this.keywords = keywords.replaceAll("'","\\\\'");;
            this.org_content = org_content.replaceAll("'","\\\\'");;
            this.anno_content = anno_content.replaceAll("'","\\\\'");;
            Date date = new Date();
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            this.update_time = sdf.format(date);

        }

        public void upload(MySQLConnector connector){

            try {
                Statement stmt = connector.getConn().createStatement();
                String values = "('"+title+"','"+keywords+"','"+org_content+"','"+anno_content+"','"+update_time+"');";
                String sql = "INSERT INTO corpus(title,keywords,org_content,anno_content,update_time) " +
                        "VALUES "+values;
                stmt.execute(sql);
                stmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
    public static void main(String[] args){

        upload2CorpusTable();
//        new corpusTableRecord("","","","");
    }
}
