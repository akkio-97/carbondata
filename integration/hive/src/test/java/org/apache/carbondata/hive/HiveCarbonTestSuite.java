package org.apache.carbondata.hive;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.schema.SchemaReader;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

public class HiveCarbonTestSuite extends HiveTestUtils {

  private static Statement statement;

  @BeforeClass
  public static void setup() throws Exception {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_OFFHEAP_SORT_DEFAULT, "false");
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_UNSAFE_SORT, "false");
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_WRITTEN_BY_APPNAME, "hive");
    statement = getConnection().createStatement();
    statement.execute("drop table if exists hive_carbon_table1");
    statement.execute("drop table if exists hive_carbon_table2");
    statement.execute("drop table if exists hive_carbon_table3");
    statement.execute("drop table if exists hive_carbon_table4");
    statement.execute("drop table if exists hive_carbon_table5");
    statement.execute("drop table if exists hive_table");
    statement.execute("CREATE external TABLE hive_table( shortField SMALLINT, intField INT, bigintField BIGINT , doubleField DOUBLE, stringField STRING, timestampField TIMESTAMP, decimalField DECIMAL(18,2), dateField DATE, charField CHAR(5), floatField FLOAT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' location '/home/root1/projects/carbondata/integration/hive/src/main/resources/csv/' TBLPROPERTIES ('external.table.purge'='false')");

  }

  @Test
  public void createCarbonTableUsingHive() throws Exception {
    statement.execute("CREATE TABLE hive_carbon_table1( shortField SMALLINT , intField INT, bigintField BIGINT , doubleField DOUBLE, stringField STRING, timestampField TIMESTAMP, decimalField DECIMAL, dateField DATE, charField CHAR(5), floatField FLOAT) stored by 'org.apache.carbondata.hive.CarbonStorageHandler' "
        + "TBLPROPERTIES('sort_columns'='floatField', "
        + "'SORT_SCOPE'='global_sort', "
        + "'TABLE_BLOCKSIZE'='600',"
        + "'TABLE_BLOCKLET_SIZE'='100',"
        + "'TABLE_PAGE_SIZE_INMB'='50',"
        + "'LOCAL_DICTIONARY_ENABLE'='true',"
        + "'COLUMN_META_CACHE'='bigintField',"
        + "'CACHE_LEVEL'='blocklet')");
    String location = getFieldValue(statement.executeQuery("describe formatted hive_carbon_table1"), "location");
    String schemaPath = location  + "/Metadata/schema";
    assert(FileFactory.isFileExist(schemaPath));
  }

  @Test
  public void checkVariousTableProperties() throws Exception {
    statement.execute("CREATE TABLE hive_carbon_table2( shortField SMALLINT , intField INT, bigintField BIGINT , doubleField DOUBLE, stringField STRING, timestampField TIMESTAMP, decimalField DECIMAL, dateField DATE, charField CHAR(5), floatField FLOAT) stored by 'org.apache.carbondata.hive.CarbonStorageHandler' "
        + "TBLPROPERTIES('sort_columns'='floatField', "
        + "'SORT_SCOPE'='global_sort', "
        + "'TABLE_BLOCKSIZE'='600',"
        + "'TABLE_BLOCKLET_SIZE'='100',"
        + "'TABLE_PAGE_SIZE_INMB'='50',"
        + "'LOCAL_DICTIONARY_ENABLE'='true',"
        + "'COLUMN_META_CACHE'='bigintField',"
        + "'CACHE_LEVEL'='blocklet')");
    String location = getFieldValue(statement.executeQuery("describe formatted hive_carbon_table2"), "location");
    String schemaFilePath = CarbonTablePath.getSchemaFilePath(location);
    Map<String, String> tableProperties =
        SchemaReader.readCarbonTableFromSchema(schemaFilePath, FileFactory.getConfiguration())
            .getTableInfo().getFactTable().getTableProperties();
    assert(tableProperties.get("column_meta_cache").equalsIgnoreCase("bigintfield"));
    assert(tableProperties.get("cache_level").equalsIgnoreCase("blocklet"));
    assert(tableProperties.get("local_dictionary_enable").equalsIgnoreCase("true"));
    assert(tableProperties.get("TABLE_PAGE_SIZE_INMB".toLowerCase()).equalsIgnoreCase("50"));
    assert(tableProperties.get("TABLE_BLOCKLET_SIZE".toLowerCase()).equalsIgnoreCase("100"));
    assert(tableProperties.get("TABLE_BLOCKSIZE".toLowerCase()).equalsIgnoreCase("600"));
    assert(tableProperties.get("sort_scope").equalsIgnoreCase("global_sort"));
    assert(tableProperties.get("sort_columns").equalsIgnoreCase("floatField"));
  }

  @Test
  public void verifyLoadIntoCarbonTableUsingHive() throws Exception {
    statement.execute("CREATE TABLE hive_carbon_table3( shortField SMALLINT , intField INT, bigintField BIGINT , doubleField DOUBLE, stringField STRING, timestampField TIMESTAMP, decimalField DECIMAL, dateField DATE, charField CHAR(5), floatField FLOAT) stored by 'org.apache.carbondata.hive.CarbonStorageHandler'");
    statement.execute("insert into hive_carbon_table3 select * from hive_table");
    String location = getFieldValue(statement.executeQuery("describe formatted hive_carbon_table3"), "location");
    List<String> folderStructure = new ArrayList<>();
    for (CarbonFile carbonFile : FileFactory.getCarbonFile(location).listFiles(true)) {
      folderStructure.add(carbonFile.getAbsolutePath());
    }
    assert(folderStructure.stream().anyMatch(x -> x.contains("/segments/0_")));
    assert(folderStructure.stream().anyMatch(x -> x.contains("/Fact/Part0/Segment_0/")));
    assert(folderStructure.stream().anyMatch(x -> x.contains(".carbondata")));
    assert(folderStructure.stream().anyMatch(x -> x.contains(".carbonindex")));
  }

  @Test
  public void verifyDataAfterLoad() throws Exception {
    statement.execute("drop table if exists hive_carbon_table4");
    statement.execute("CREATE TABLE hive_carbon_table4(shortField SMALLINT , intField INT, bigintField BIGINT , doubleField DOUBLE, stringField STRING, timestampField TIMESTAMP, decimalField DECIMAL(18,2), dateField DATE, charField CHAR(5), floatField FLOAT) stored by 'org.apache.carbondata.hive.CarbonStorageHandler'");
    statement.execute("insert into hive_carbon_table4 select * from hive_table");
    checkAnswer(statement.executeQuery("select * from hive_carbon_table4"),
            getConnection().createStatement().executeQuery("select * from hive_table"));
  }

  @Test public void verifyDataAfterLoadUsingSortColumns() throws Exception {
    statement.execute("drop table if exists hive_carbon_table5");
    statement.execute(
        "CREATE TABLE hive_carbon_table5(shortField SMALLINT , intField INT, bigintField BIGINT , doubleField DOUBLE, stringField STRING, timestampField TIMESTAMP, decimalField DECIMAL(18,2), dateField DATE, charField CHAR(5), floatField FLOAT) stored by 'org.apache.carbondata.hive.CarbonStorageHandler' TBLPROPERTIES('sort_columns'='stringField', 'sort_scope'= 'local_sort')");
    statement.execute("insert into hive_carbon_table5 select * from hive_table");
    ResultSet resultSet = getConnection().createStatement()
        .executeQuery("select * from hive_carbon_table5 order by stringfield");
    ResultSet hiveResults = getConnection().createStatement()
        .executeQuery("select * from hive_table order by stringfield");
    checkAnswer(resultSet, hiveResults);
  }

  @Ignore
  public void testCreateAndLoadUsingComplexColumns() throws Exception {
    statement.execute("drop table if exists hive_table_complex");
    statement.execute("drop table if exists hive_carbon_table6");
    statement.execute("CREATE TABLE hive_table_complex(arrayField  ARRAY<STRING>, mapField MAP<String, int>, structField struct<col1: int, col2: string>)");
    statement.execute(
        "insert into hive_table_complex values(array('k', 'a'), map('k', 1, 'v', 2), struct('col1', 'cc1', 'col2', 'cc2'))");
    statement.execute(
        "CREATE TABLE hive_carbon_table6(arrayField  ARRAY<STRING>, mapField MAP<String, int>, structField struct<col1: int, col2: string>) stored by 'org.apache.carbondata.hive.CarbonStorageHandler'");
    statement.execute(
        "insert into hive_carbon_table6 values(array('k', 'a'), map('k', 1, 'v', 2), struct('col1', 'cc1', 'col2', 'cc2'))");
    ResultSet hiveResult = getConnection().createStatement().executeQuery("select * from hive_table_complex");
    ResultSet carbonResult = getConnection().createStatement().executeQuery("select * from hive_carbon_table6");
    checkAnswer(carbonResult, hiveResult);
  }

}
