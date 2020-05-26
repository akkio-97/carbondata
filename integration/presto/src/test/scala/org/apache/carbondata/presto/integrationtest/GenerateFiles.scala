package org.apache.carbondata.presto.integrationtest

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, File, InputStream}
import java.util

import scala.collection.JavaConverters._

import org.apache.avro
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.{GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{DecoderFactory, Encoder}
import org.apache.commons.io.FileUtils

import org.apache.carbondata.core.cache.dictionary.DictionaryByteArrayWrapper
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.block.TableBlockInfo
import org.apache.carbondata.core.datastore.chunk.impl.DimensionRawColumnChunk
import org.apache.carbondata.core.datastore.chunk.reader.CarbonDataReaderFactory
import org.apache.carbondata.core.datastore.chunk.reader.dimension.v3.DimensionChunkReaderV3
import org.apache.carbondata.core.datastore.compression.CompressorFactory
import org.apache.carbondata.core.datastore.filesystem.{CarbonFile, CarbonFileFilter}
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.datastore.page.encoding.DefaultEncodingFactory
import org.apache.carbondata.core.metadata.ColumnarFormatVersion
import org.apache.carbondata.core.util.{CarbonMetadataUtil, DataFileFooterConverterV3}
import org.apache.carbondata.sdk.file.CarbonWriter

class GenerateFiles {
  var writerPath = new File(this.getClass.getResource("/").getPath
                            + "../../target/SparkCarbonFileFormat/WriterOutput/")
    .getCanonicalPath
  //getCanonicalPath gives path with \, but the code expects /.
  writerPath = writerPath.replace("\\", "/")

  def buildAvroTestDataSingleFileArrayType(): Any = {
    FileUtils.deleteDirectory(new File(writerPath))
    buildAvroTestDataArrayType(3, null)
  }

  def buildAvroTestDataArrayType(rows: Int, options: util.Map[String, String]): Any = {
    FileUtils.deleteDirectory(new File(writerPath))

    val mySchema = """ {
                     |      "name": "address",
                     |      "type": "record",
                     |      "fields": [
                     |      {
                     |      "name": "name",
                     |      "type": "string"
                     |      },
                     |      {
                     |      "name": "age",
                     |      "type": "int"
                     |      },
                     |      {
                     |      "name": "address",
                     |      "type": {
                     |      "type": "array",
                     |      "items": {
                     |      "name": "street",
                     |      "type": "string"
                     |      }
                     |      }
                     |      }
                     |      ]
                     |  }
                   """.stripMargin

    val json: String = """ {"name": "bob","age": 10,"address": ["abc", "defg"]} """

    WriteFilesWithAvroWriter(rows, mySchema, json)
  }

  def WriteFilesWithAvroWriter(rows: Int,
      mySchema: String,
      json: String) = {
    // conversion to GenericData.Record
    val nn = new avro.Schema.Parser().parse(mySchema)
    val record = testUtil.jsonToAvro(json, mySchema)
    try {
      val writer = CarbonWriter.builder
        .outputPath(writerPath)
        .uniqueIdentifier(System.currentTimeMillis()).withAvroInput(nn).writtenBy("TestNonTransactionalCarbonTable").build()
      var i = 0
      while (i < rows) {
        writer.write(record)
        i = i + 1
      }
      writer.close()
    } catch {
      case e: Exception =>
        e.printStackTrace()
      //        Assert.fail(e.getMessage)
    }
  }


  object testUtil{

    def jsonToAvro(json: String, avroSchema: String): GenericRecord = {
      var input: InputStream = null
      var writer: DataFileWriter[GenericRecord] = null
      var encoder: Encoder = null
      var output: ByteArrayOutputStream = null
      try {
        val schema = new org.apache.avro.Schema.Parser().parse(avroSchema)
        val reader = new GenericDatumReader[GenericRecord](schema)
        input = new ByteArrayInputStream(json.getBytes())
        output = new ByteArrayOutputStream()
        val din = new DataInputStream(input)
        writer = new DataFileWriter[GenericRecord](new GenericDatumWriter[GenericRecord]())
        writer.create(schema, output)
        val decoder = DecoderFactory.get().jsonDecoder(schema, din)
        var datum: GenericRecord = reader.read(null, decoder)
        return datum
      } finally {
        input.close()
        writer.close()
      }
    }

    /**
     * this method returns true if local dictionary is created for all the blocklets or not
     *
     * @return
     */
    def getDimRawChunk(blockindex: Int,storePath :String): util.ArrayList[DimensionRawColumnChunk] = {
      val dataFiles = FileFactory.getCarbonFile(storePath)
        .listFiles(new CarbonFileFilter() {
          override def accept(file: CarbonFile): Boolean = {
            if (file.getName
              .endsWith(CarbonCommonConstants.FACT_FILE_EXT)) {
              true
            } else {
              false
            }
          }
        })
      val dimensionRawColumnChunks = read(dataFiles(0).getAbsolutePath,
        blockindex)
      dimensionRawColumnChunks
    }

    def read(filePath: String, blockIndex: Int) = {
      val carbonDataFiles = new File(filePath)
      val dimensionRawColumnChunks = new
          util.ArrayList[DimensionRawColumnChunk]
      val offset = carbonDataFiles.length
      val converter = new DataFileFooterConverterV3
      val fileReader = FileFactory.getFileHolder(FileFactory.getFileType(filePath))
      val actualOffset = fileReader.readLong(carbonDataFiles.getAbsolutePath, offset - 8)
      val blockInfo = new TableBlockInfo(carbonDataFiles.getAbsolutePath,
        actualOffset,
        "0",
        new Array[String](0),
        carbonDataFiles.length,
        ColumnarFormatVersion.V3,
        null)
      val dataFileFooter = converter.readDataFileFooter(blockInfo)
      val blockletList = dataFileFooter.getBlockletList.asScala
      for (blockletInfo <- blockletList) {
        val dimensionColumnChunkReader =
          CarbonDataReaderFactory
            .getInstance
            .getDimensionColumnChunkReader(ColumnarFormatVersion.V3,
              blockletInfo,
              carbonDataFiles.getAbsolutePath,
              false).asInstanceOf[DimensionChunkReaderV3]
        dimensionRawColumnChunks
          .add(dimensionColumnChunkReader.readRawDimensionChunk(fileReader, blockIndex))
      }
      dimensionRawColumnChunks
    }

    def validateDictionary(rawColumnPage: DimensionRawColumnChunk,
        data: Array[String]): Boolean = {
      val local_dictionary = rawColumnPage.getDataChunkV3.local_dictionary
      if (null != local_dictionary) {
        val compressorName = CarbonMetadataUtil.getCompressorNameFromChunkMeta(
          rawColumnPage.getDataChunkV3.getData_chunk_list.get(0).getChunk_meta)
        val encodings = local_dictionary.getDictionary_meta.encoders
        val encoderMetas = local_dictionary.getDictionary_meta.getEncoder_meta
        val encodingFactory = DefaultEncodingFactory.getInstance
        val decoder = encodingFactory.createDecoder(encodings, encoderMetas, compressorName)
        val dictionaryPage = decoder
          .decode(local_dictionary.getDictionary_data, 0, local_dictionary.getDictionary_data.length)
        val dictionaryMap = new util.HashMap[DictionaryByteArrayWrapper, Integer]
        val usedDictionaryValues = util.BitSet
          .valueOf(CompressorFactory.getInstance.getCompressor(compressorName)
            .unCompressByte(local_dictionary.getDictionary_values))
        var index = 0
        var i = usedDictionaryValues.nextSetBit(0)
        while ( { i >= 0 }) {
          dictionaryMap
            .put(new DictionaryByteArrayWrapper(dictionaryPage.getBytes({ index += 1; index - 1 })),
              i)
          i = usedDictionaryValues.nextSetBit(i + 1)
        }
        for (i <- data.indices) {
          if (null == dictionaryMap.get(new DictionaryByteArrayWrapper(data(i).getBytes))) {
            return false
          }
        }
        return true
      }
      false
    }

    def checkForLocalDictionary(dimensionRawColumnChunks: util
    .List[DimensionRawColumnChunk]): Boolean = {
      var isLocalDictionaryGenerated = false
      import scala.collection.JavaConversions._
      for (dimensionRawColumnChunk <- dimensionRawColumnChunks) {
        if (dimensionRawColumnChunk.getDataChunkV3
          .isSetLocal_dictionary) {
          isLocalDictionaryGenerated = true
        }
      }
      isLocalDictionaryGenerated
    }

  }



  def randomFunc() = {
    val json1: String = """ {"stringCol": "bob","intCol": 14,"doubleCol": 10.5,"realCol": 12.7, "boolCol": true,"arrayStringCol1":["Street1"],"arrayStringCol2": ["India", "Egypt"],"arrayIntCol": [1,2,3],"arrayBigIntCol":[70000,600000000],"arrayRealCol":[1.111,2.2],"arrayDoubleCol":[1.1,2.2,3.3], "arrayBooleanCol": [true, false, true]} """
    val json2: String = """ {"stringCol": "Alex","intCol": 15,"doubleCol": 11.5,"realCol": 13.7, "boolCol": true, "arrayStringCol1": ["Street1", "Street2"],"arrayStringCol2": ["Japan", "China", "India"],"arrayIntCol": [1,2,3,4],"arrayBigIntCol":[70000,600000000,8000],"arrayRealCol":[1.1,2.2,3.3],"arrayDoubleCol":[1.1,2.2,4.45,3.3], "arrayBooleanCol": [true, true, true]} """
    val json3: String = """ {"stringCol": "Rio","intCol": 16,"doubleCol": 12.5,"realCol": 14.7, "boolCol": true, "arrayStringCol1": ["Street1", "Street2","Street3"],"arrayStringCol2": ["China", "Brazil", "Paris", "France"],"arrayIntCol": [1,2,3,4,5],"arrayBigIntCol":[70000,600000000,8000,9111111111],"arrayRealCol":[1.1,2.2,3.3,4.45],"arrayDoubleCol":[1.1,2.2,4.45,5.5,3.3], "arrayBooleanCol": [true, false, true]} """
    val json4: String = """ {"stringCol": "bob","intCol": 14,"doubleCol": 10.5,"realCol": 12.7, "boolCol": true, "arrayStringCol1":["Street1"],"arrayStringCol2": ["India", "Egypt"],"arrayIntCol": [1,2,3],"arrayBigIntCol":[70000,600000000],"arrayRealCol":[1.1,2.2],"arrayDoubleCol":[1.1,2.2,3.3], "arrayBooleanCol": [true, false, true]} """
    val json5: String = """ {"stringCol": "Alex","intCol": 15,"doubleCol": 11.5,"realCol": 13.7, "boolCol": true, "arrayStringCol1": ["Street1", "Street2"],"arrayStringCol2": ["Japan", "China", "India"],"arrayIntCol": [1,2,3,4],"arrayBigIntCol":[70000,600000000,8000],"arrayRealCol":[1.1,2.2,3.3],"arrayDoubleCol":[4,1,21.222,15.231], "arrayBooleanCol": [false, false, false]} """


    val mySchema = """ {
                     |      "name": "address",
                     |      "type": "record",
                     |      "fields": [
                     |      {
                     |      "name": "stringCol",
                     |      "type": "string"
                     |      },
                     |      {
                     |      "name": "intCol",
                     |      "type": "int"
                     |      },
                     |      {
                     |      "name": "doubleCol",
                     |      "type": "double"
                     |      },
                     |      {
                     |      "name": "realCol",
                     |      "type": "float"
                     |      },
                     |      {
                     |      "name": "boolCol",
                     |      "type": "boolean"
                     |      },
                     |      {
                     |      "name": "arrayStringCol1",
                     |      "type": {
                     |      "type": "array",
                     |      "items": {
                     |      "name": "street",
                     |      "type": "string"
                     |      }
                     |      }
                     |      },
                     |      {
                     |      "name": "arrayStringCol2",
                     |      "type": {
                     |      "type": "array",
                     |      "items": {
                     |      "name": "street",
                     |      "type": "string"
                     |      }
                     |      }
                     |      },
                     |      {
                     |      "name": "arrayIntCol",
                     |      "type": {
                     |      "type": "array",
                     |      "items": {
                     |      "name": "street",
                     |      "type": "int"
                     |      }
                     |      }
                     |      },
                     |      {
                     |      "name": "arrayBigIntCol",
                     |      "type": {
                     |      "type": "array",
                     |      "items": {
                     |      "name": "street",
                     |      "type": "long"
                     |      }
                     |      }
                     |      },
                     |      {
                     |      "name": "arrayRealCol",
                     |      "type": {
                     |      "type": "array",
                     |      "items": {
                     |      "name": "street",
                     |      "type": "float"
                     |      }
                     |      }
                     |      },
                     |      {
                     |      "name": "arrayDoubleCol",
                     |      "type": {
                     |      "type": "array",
                     |      "items": {
                     |      "name": "street",
                     |      "type": "double"
                     |      }
                     |      }
                     |      },
                     |      {
                     |      "name": "arrayBooleanCol",
                     |      "type": {
                     |      "type": "array",
                     |      "items": {
                     |      "name": "street",
                     |      "type": "boolean"
                     |      }
                     |      }
                     |      }
                     |      ]
                     |  }
                   """.stripMargin

    val nn = new avro.Schema.Parser().parse(mySchema)
    val record1 = testUtil.jsonToAvro(json1, mySchema)
    val record2 = testUtil.jsonToAvro(json2, mySchema)
    val record3 = testUtil.jsonToAvro(json3, mySchema)
    val record4 = testUtil.jsonToAvro(json4, mySchema)
    val record5 = testUtil.jsonToAvro(json5, mySchema)
    try {
      val writer = CarbonWriter.builder
        .outputPath(writerPath)
        .uniqueIdentifier(System.currentTimeMillis()).withAvroInput(nn).writtenBy("GenerateFiles").build()
      writer.write(record1)
      writer.write(record2)
      writer.write(record3)
      writer.write(record4)
      writer.write(record5)
      writer.close()
    } catch {
      case e: Exception =>
        e.printStackTrace()
      //        Assert.fail(e.getMessage)
    }
  }
}
