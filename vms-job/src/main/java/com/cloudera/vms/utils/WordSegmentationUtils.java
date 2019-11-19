//package com.cloudera.vms.utils;
//
////import org.wltea.analyzer.IKSegmentation;
////import org.wltea.analyzer.Lexeme;
//
//import org.wltea.analyzer.core.Lexeme;
//
//import java.io.IOException;
//import java.io.StringReader;
//
//public class WordSegmentationUtils {
//
//    public static void main(String[] args) throws Exception {
//        String text = "下载解压之后主要使用和依赖以下的文件";
//        System.out.println(getTerms(text));
////		Analyzer analyzer = new IKAnalyzer(true);
////		StringReader reader = new StringReader(text);
////		TokenStream ts = analyzer.tokenStream("", reader);
////        CharTermAttribute term=ts.getAttribute(CharTermAttribute.class);
////        while(ts.incrementToken()){
////            System.out.print(term.toString()+"|");
////        }
////        analyzer.close();
////        reader.close();
//
//    }
//
//    public static String getTerms(String text) throws IOException {
//        StringBuilder terms = new StringBuilder();
//        // 创建分词对象
//        StringReader reader = new StringReader(text);
//        IKSegmentation ikSegmentation = new IKSegmentation(reader, true);// 当为true时，分词器进行最大词长切分
//        Lexeme lexeme = null;
//        try {
//            while ((lexeme = ikSegmentation.next()) != null) {
//                String lexemeText = lexeme.getLexemeText();
////                System.out.println(lexemeText);
//                if (terms.length() != 0) {
//                    terms.append(" ");
//                }
//                terms.append(lexemeText);
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        } finally {
//            reader.close();
//        }
//        return terms.toString();
//    }
//
////    public static String getTerms(String text) throws IOException {
////        StringBuilder terms = new StringBuilder();
////        StringReader reader = new StringReader(text);
////        IKSegmentation ikSegmentation = new IKSegmentation(reader);
////        while (true) {
////            Lexeme lexeme = ikSegmentation.next();
////            if (lexeme != null) {
////                String lexemeText = lexeme.getLexemeText();
////                if (terms.length() != 0) {
////                    terms.append(" ");
////                }
////                terms.append(lexemeText);
////            } else {
////                break;
////            }
////        }
////        return terms.toString();
////    }
//
////    public static String getTerms(String text) throws IOException {
////        StringBuilder terms = new StringBuilder();
//////		Analyzer analyzer = new IKAnalyzer(true);
//////		Analyzer analyzer = new IKAnalyzer(new Configuration(new Environment(Settings.EMPTY), Settings.EMPTY));
////        Analyzer analyzer = new IKAnalyzer();
////        StringReader reader = new StringReader(text);
////        TokenStream ts = analyzer.tokenStream("", reader);
////        CharTermAttribute term = ts.getAttribute(CharTermAttribute.class);
////        while (ts.incrementToken()) {
////            terms.append(term.toString()).append(" ");
////        }
////        analyzer.close();
////        reader.close();
////        return terms.toString();
////    }
//
//}
