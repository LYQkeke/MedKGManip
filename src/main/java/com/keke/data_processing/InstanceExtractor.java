package com.keke.data_processing;

import com.keke.sql_tools.MySQLConnector;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Stack;
import java.util.regex.Pattern;

/**
 * Created by KEKE on 2019/1/7
 */
public class InstanceExtractor {

    public static void extractByDate(String date) throws BadDateException {
        String pattern = "^\\d{4}-\\d{2}-\\d{2}$";
        boolean isMatch = Pattern.matches(pattern, date);
        if (!isMatch){
            throw new BadDateException("Bad date format ! ! !");
        }
        MySQLConnector connector = new MySQLConnector();
        try {
            Statement stmt = connector.getConn().createStatement();
            String sql = "SELECT id,anno_content FROM corpus WHERE update_time='" +date+"'";
            ResultSet rs = stmt.executeQuery(sql);
            // 处理每一条查询结果--Corpus记录
            while (rs.next()){
                int id = rs.getInt("id");
                String anno_content = rs.getString("anno_content");
                resolveAnno(id, anno_content, connector);
            }
            rs.close();
            stmt.close();

        } catch (SQLException e) {
            e.printStackTrace();
        }
        connector.destroy();
    }

    public static void extractByCorpusId(int id){
        MySQLConnector connector = new MySQLConnector();
        try {
            Statement stmt = connector.getConn().createStatement();
            String sql = "select id,anno_content from corpus where id = " + id;
            ResultSet rs = stmt.executeQuery(sql);
            while (rs.next()){
                String anno_content = rs.getString("anno_content");
                System.out.println(anno_content);
                resolveAnno(id, anno_content, connector);
            }
            rs.close();
            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            connector.destroy();
        }
    }


    private static void resolveAnno(int corpusId, String anno_content, MySQLConnector connector){

        StringBuilder content = new StringBuilder();
        StringBuilder type = new StringBuilder();
        StringBuilder type_description = new StringBuilder();
        boolean nestFlag = false;
        Stack<Character> stack = new Stack<Character>();
        char[] chs = anno_content.toCharArray();
        for (int i=anno_content.indexOf('[');i<chs.length;i++){
            if (!stack.isEmpty()||(chs[i]=='['&&chs[i+1]=='#')){
                stack.push(chs[i]);
                if (chs[i]==']'&&chs[i-1]==')'&&chs[i-3]=='('){
                    stack.pop();
                    while (stack.peek()!='@'){
                        type_description.insert(0,stack.pop());
                    }
                    stack.pop();
                    while (stack.peek()!='#'){
                        type.insert(0,stack.pop());
                    }
                    stack.pop();
                    while (stack.peek()!='#'){
                        content.insert(0,stack.pop());
                    }
                    stack.pop();
                    stack.pop();

                    if (!stack.isEmpty()){
                        System.out.println("NestAnnotation !!!");
                        nestFlag = true;
                        char[] chars = content.toString().replaceAll("  "," ").trim().toCharArray();
                        for (char c:chars){
                            stack.push(c);
                        }
                    }
                    System.out.println(content.toString().replaceAll("  ", " ").trim());
                    new InstanceInfo(corpusId,
                            Integer.parseInt(type.toString()),
                            content.toString().replaceAll("  "," ").trim(),
                            type_description.toString().replaceAll("  "," "),
                            nestFlag).upload2InstanceTable(connector);
                    content = new StringBuilder();
                    type = new StringBuilder();
                    type_description = new StringBuilder();
                    nestFlag = false;
                }
            }
        }

//        /**
//         * 未处理嵌套标注的情况
//         */
//        Pattern nestReg = Pattern.compile("\\[([^\\[\\]]*\\[[^\\[\\]]*?\\][^\\[\\]]*)+?\\]");
//        Matcher nestMather = nestReg.matcher(anno_content);
//        while (nestMather.find()){
//            Pattern reg = Pattern.compile("\\[[^\\[\\]]*?\\]");
//            Matcher matcher = reg.matcher(anno_content);
//            while (matcher.find()){
//                String annoItem = matcher.group();
//                Pattern r = Pattern.compile("^\\[#(.*)#(.*)@(.*)\\]$");
//                Matcher m = r.matcher(annoItem);
//                if (m.find()){
//                    new InstanceInfo(corpusId, Integer.parseInt(m.group(2)), m.group(1), m.group(3)).upload2InstanceTable(connector);
//                }
//
//            }
//        }
//
//        /**
//         * 处理未嵌套的标注
//         */
//        Pattern reg = Pattern.compile("\\[[^\\[\\]]*?\\]");
////        Pattern reg = Pattern.compile("\\[[^\\[\\]]*?\\]");
//        Matcher matcher = reg.matcher(anno_content);
//        while (matcher.find()){
//            System.out.println(matcher.group());
//            Pattern r = Pattern.compile("^\\[#(.*)#(.*)@(.*)\\]$");
//            Matcher m = r.matcher(matcher.group());
//            if (m.find()){
////                System.out.println(m.group(1));
////                System.out.println(m.group(2));
////                System.out.println(m.group(3));
//                new InstanceInfo(corpusId, Integer.parseInt(m.group(2)), m.group(1), m.group(3)).upload2InstanceTable(connector);
//            }
//            System.out.println();
//        }
    }


    private static class BadDateException extends Exception{
        public BadDateException(String description){
            super(description);
        }
    }


    private static class InstanceInfo{

        private int corpusId;
        private String content;
        private int length;
        private int type;
        private String type_description;
        private boolean nest_flag;

        public InstanceInfo(int corpusId, int type, String content, String type_description, boolean nest_flag){

            this.corpusId = corpusId;
            this.content = content.trim().replaceAll("'","\\\\'");
            this.type = type;
            this.type_description = type_description.trim();
            this.nest_flag = nest_flag;

            this.length = this.content.split(" ").length;
//            System.out.println();
        }

        public void upload2InstanceTable(MySQLConnector connector){

            try {
                Statement stmt = connector.getConn().createStatement();
                String values = "("+corpusId+",'"+content+"',"+length+","+type+",'"+type_description+"',"+nest_flag+")";
                String sql = "INSERT INTO instance(corpus_id,content,len,type,type_description,nest_flag) VALUES "+values;
                stmt.executeUpdate(sql);
                System.out.println("an instance record upload success");
                System.out.println();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
    public static void main(String[] args){
//        extractByCorpusId(64);
        try {
//            extractByDate("2019-01-08");
            extractByDate("2019-01-14");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
