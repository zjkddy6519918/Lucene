package com.kemp.lucene;

import com.sun.istack.internal.Nullable;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.QueryBuilder;
import org.wltea.analyzer.lucene.IKAnalyzer;

import java.io.File;
import java.util.*;

/**
 * Lucene索引简单工具类
 * @author kemp
 */
public class LuceneIndexUtil {

    /** 索引文件目录，默认为程序目录 **/
    private static String filePath = "";

    /**
     * 定义索引文件目录
     * @param filePath
     */
    public static void defineIndexPath(String filePath) {
        if (!filePath.endsWith(File.separator)) {
            filePath = filePath + File.separator;
        }
        LuceneIndexUtil.filePath = filePath;
    }

    /********************************************************** 查询 ***********************************************/

    /**
     * 根据查询条件查询
     * @param indexName
     * @param query
     * @return
     * @throws Exception
     */
    public static List<Map<String, String>> query(String indexName, Query query) throws Exception {
        return query(indexName, query, Integer.MAX_VALUE);
    }

    /**
     * 根据查询条件查询
     * @param indexName
     * @param query
     * @param limitNum 查询数量限制
     * @return
     * @throws Exception
     */
    public static List<Map<String, String>> query(String indexName, Query query, int limitNum) throws Exception {
        // 返回结果
        List<Map<String, String>> resultList = new ArrayList<>(1);
        try (IndexReader indexReader = getIndexReader(indexName)) {
            IndexSearcher indexSearcher = new IndexSearcher(indexReader);
            TopDocs topDocs = indexSearcher.search(query, limitNum);
            for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
                Document doc = indexSearcher.doc(scoreDoc.doc);
                List<IndexableField> fields = doc.getFields();
                Map<String, String> docMap = new HashMap<>(fields.size());
                for (IndexableField field : fields) {
                    docMap.put(field.name(), field.stringValue());
                }
                resultList.add(docMap);
            }
            return resultList;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /********************************************************** 新增 ***********************************************/

    /**
     * 添加单个文档
     * @param indexName 索引名
     * @param fieldList 待添加域集合
     * @param analyzer 分析器，若为空则使用默认分析器
     * @throws Exception
     * @return 更新的文档数
     */
    public static long addDocument(String indexName, List<Field> fieldList, @Nullable Analyzer analyzer) throws Exception {
        if (indexName == null || "".equals(indexName) ||
                fieldList == null || fieldList.size() == 0) {
            return 0;
        }
        String filePath = LuceneIndexUtil.filePath;

        // 创建一个和IndexWriter对象
        try (IndexWriter indexWriter = getIndexWriter(indexName, analyzer)) {
            // 创建一个Document对象
            Document document = new Document();
            // 向document对象中添加域
            for (Field field : fieldList) {
                document.add(field);
            }
            // 把文档写入索引库
            long result = indexWriter.addDocument(document);
            return result;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 批量添加文档
     * @param indexName 索引名
     * @param fieldsList 待添加域集合
     * @param analyzer 分析器，若为空则使用默认分析器
     * @return
     * @throws Exception
     */
    public static long addDocumentList(String indexName, List<List<Field>> fieldsList, @Nullable Analyzer analyzer) throws Exception {
        if (indexName == null || "".equals(indexName) ||
                fieldsList == null || fieldsList.size() == 0) {
            return 0;
        }
        String filePath = LuceneIndexUtil.filePath;
        if (!filePath.endsWith(File.separator)) {
            filePath = filePath + File.separator;
        }
        int documentSize = fieldsList.size();
        // 待插入文档集合
        List<Document> preInsertDocumentList = new ArrayList<>(documentSize);
        // 创建一个和IndexWriter对象
        try (IndexWriter indexWriter = getIndexWriter(indexName, analyzer)) {
            for (List<Field> fieldList : fieldsList) {
                // 创建一个Document对象
                Document document = new Document();
                // 向document对象中添加域
                for (Field field : fieldList) {
                    document.add(field);
                }
                // 把文档加入待插入文档集合
                preInsertDocumentList.add(document);
            }
            // 插入文档
            long result = indexWriter.addDocuments(preInsertDocumentList);
            return result;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /********************************************************** 删除 ***********************************************/

    /**
     * 删除整个索引
     * @param index 索引名
     * @return
     */
    public static long deleteAllIndex(String index) throws Exception {
        // 创建一个和IndexWriter对象
        try (IndexWriter indexWriter = getIndexWriter(index, null)) {
            // 删除索引
            long result = indexWriter.deleteAll();
            return result;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 根据文档id删除文档
     * @param index 索引名
     * @param query 查询条件
     * @return
     */
    public static long deleteIndexByQuery(String index, Query query) {
        // 创建一个和IndexWriter对象
        try (IndexWriter indexWriter = getIndexWriter(index, null)) {
            // 删除指定id的文档
            long result = indexWriter.deleteDocuments(query);
            return result;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /********************************************************** 修改 ***********************************************/
    /**
     * 修改文档 (先删除后添加)
     * @param index 索引名
     * @param term 对应字段值的待删除文档
     * @param document 新增文档
     * @return
     * @throws Exception
     */
    public static long updateIndex(String index, Term term, Document document) throws Exception {
        // 创建一个和IndexWriter对象
        try (IndexWriter indexWriter = getIndexWriter(index, null)) {
            long result = indexWriter.updateDocument(term, document);
            return result;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 修改文档 (先删除后添加)
     * @param index 索引名
     * @param term 对应字段值的待删除文档
     * @param documents 新增文档集合
     * @return
     * @throws Exception
     */
    public static long updateIndex(String index, Term term, Collection<Document> documents) throws Exception {
        // 创建一个和IndexWriter对象
        try (IndexWriter indexWriter = getIndexWriter(index, null)) {
            long result = indexWriter.updateDocuments(term, documents);
            return result;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 获取IndexWriter
     * @param indexName
     * @param analyzer
     * @return
     */
    private static IndexWriter getIndexWriter(String indexName, Analyzer analyzer) throws Exception {
        IndexWriter indexWriter = new IndexWriter(
                FSDirectory.open(new File(filePath + indexName).toPath()),
                new IndexWriterConfig(analyzer == null ? new StandardAnalyzer() : analyzer));
        return indexWriter;
    }

    /**
     * 获取IndexWriter
     * @param indexName
     * @return
     */
    private static IndexReader getIndexReader(String indexName) throws Exception {
        Directory directory = FSDirectory.open(new File(filePath + indexName).toPath());
        // 创建一个IndexReader对象
        IndexReader indexReader = DirectoryReader.open(directory);
        return indexReader;
    }

    public static void main(String[] args) throws Exception {
        List<Field> fieldList = new ArrayList<>(3);
        fieldList.add(new TextField("name", "Kemp 测试", Field.Store.YES));
        fieldList.add(new LongPoint("age", 26));
        fieldList.add(new StoredField("age", 26));
//        deleteAllIndex("demo");
        addDocument("demo", fieldList, null);
        addDocument("demo", fieldList, new IKAnalyzer());

        List<Map<String, String>> documentList = query("demo", new QueryBuilder(new IKAnalyzer()).createPhraseQuery("name", "Kemp"));
//        deleteIndexByQuery("demo", new QueryBuilder(new IKAnalyzer()).createPhraseQuery("name", "Kemp"));
//        List<Document> documentList1 = query("demo", new QueryBuilder(new IKAnalyzer()).createPhraseQuery("name", "Kemp"));

        System.out.println(documentList);
//        deleteIndexByQuery("demo", new TermQuery(new Term("age", "26")));

    }
}
