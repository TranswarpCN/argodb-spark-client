package io.transwarp.holodesk.spark.utils

import io.transwarp.holodesk.connector.serde.SerDeSupplementaryHelper
import io.transwarp.holodesk.core.result.{ByteArrayColumnResult, ColumnResult}
import io.transwarp.holodesk.core.serde.{HolodeskType, SerDeHelper}
import org.apache.hadoop.io.Text

import scala.collection.mutable.ArrayBuffer

class HolodeskCoreFastSerde(val colNum: Int, val primitiveType: Array[Int], val secondaryType: Array[Array[Int]]) {

  /* ---------------- Serialize ------------------- */
  type Ser = (Any, Int, Int) => Int

  val sers = new ArrayBuffer[Ser]

  var isInit = false

  def init() {
    isInit = false
  }

  val res = new Array[Array[Byte]](colNum)

  // byte
  sers += ((v: Any, idx: Int, offset: Int) => {
    res(idx) = SerDeHelper.serialize(v, HolodeskType.BYTE)
    SerDeHelper.ByteSize
  })

  // short
  sers += ((v: Any, idx: Int, offset: Int) => {
    res(idx) = SerDeHelper.serialize(v, HolodeskType.SHORT)
    SerDeHelper.ShortSize
  })

  // boolean
  sers += ((v: Any, idx: Int, offset: Int) => {
    res(idx) = SerDeHelper.serialize(v, HolodeskType.BOOLEAN)
    SerDeHelper.BooleanSize
  })

  // int
  sers += ((v: Any, idx: Int, offset: Int) => {
    res(idx) = SerDeHelper.serialize(v, HolodeskType.INT)
    SerDeHelper.IntSize
  })

  // long
  sers += ((v: Any, idx: Int, offset: Int) => {
    res(idx) = SerDeHelper.serialize(v, HolodeskType.LONG)
    SerDeHelper.LongSize
  })

  // float
  sers += ((v: Any, idx: Int, offset: Int) => {
    res(idx) = SerDeHelper.serialize(v, HolodeskType.FLOAT)
    SerDeHelper.FloatSize
  })

  // double
  sers += ((v: Any, idx: Int, offset: Int) => {
    res(idx) = SerDeHelper.serialize(v, HolodeskType.DOUBLE)
    SerDeHelper.DoubleSize
  })

  // CHAR
  sers += ((v: Any, idx: Int, offset: Int) => {
    res(idx) = SerDeSupplementaryHelper.serialize(v.asInstanceOf[Text], HolodeskType.CHAR)
    if (res(idx) == null) {
      0
    } else {
      res(idx).length
    }
  })


  // String
  sers += ((v: Any, idx: Int, offset: Int) => {
    res(idx) = SerDeSupplementaryHelper.serialize(v.asInstanceOf[Text], HolodeskType.STRING)
    if (res(idx) == null) {
      0
    } else {
      res(idx).length
    }
  })

  // Varchar2
  sers += ((v: Any, idx: Int, offset: Int) => {
    res(idx) = SerDeSupplementaryHelper.serialize(v.asInstanceOf[Text], HolodeskType.VARCHAR2)
    if (res(idx) == null) {
      0
    } else {
      res(idx).length
    }
  })

  // Array
  sers += ((v: Any, idx: Int, offset: Int) => {
    val av = v.asInstanceOf[Array[_]]
    if (isInit) {
      res(idx)
    } else {
      var i = 0
      var width = 0
      while (i < av.length) {
        val t = HolodeskCoreFastSerde.getType(av(i))
        width += HolodeskCoreFastSerde.WidthByType(t)
        i += 1
      }
      res(idx) = new Array[Byte](width)
      res(idx)
    }
    val t = secondaryType(idx)
    var i = 0
    var offset = 0
    while (i < av.length) {
      offset += sers(t(i))(av(i), idx, offset)
      i += 1
    }
    res(idx).length
  })

  // Other
  sers += ((v: Any, idx: Int, offset: Int) => {
    val av = v.asInstanceOf[Array[Byte]]
    res(idx) = SerDeHelper.serialize(v, HolodeskType.OTHER)
    if (res(idx) == null) {
      0
    } else {
      res(idx).length
    }
  })

  // PlaceHolder for type VOID
  sers += ((v: Any, idx: Int, offset: Int) => {
    0
  })

  // PlaceHolder for type WRDECIMAL
  sers += ((v: Any, idx: Int, offset: Int) => {
    val av = v.asInstanceOf[Array[Byte]]
    res(idx) = av
    if (res(idx) == null) {
      0
    } else {
      res(idx).length
    }
  })

  // VARCHAR
  sers += ((v: Any, idx: Int, offset: Int) => {
    res(idx) = SerDeSupplementaryHelper.serialize(v.asInstanceOf[Text], HolodeskType.VARCHAR)
    if (res(idx) == null) {
      0
    } else {
      res(idx).length
    }
  })

  def isFixedWidthType(t: Int) = {
    HolodeskCoreFastSerde.WidthByType(t) != -1
  }

  def putValue(v: Any, idx: Int) {
    sers(primitiveType(idx))(v, idx, 0)
  }

  def putValue(v: Any, idx: Int, colIndex: Int) {
    sers(primitiveType(idx))(v, colIndex, 0)
  }

  def getCurrentValueBuffer() = {
    res
  }

  /* -------------- Deserialize --------------*/

  type DeSer = (ColumnResult, Int, Int) => Any

  val desers = new ArrayBuffer[DeSer]

  // Byte
  desers += ((b: ColumnResult, idx: Int, offset: Int) => {
    val b_ = b.asInstanceOf[ByteArrayColumnResult]
    SerDeHelper.deserialize(b_.getData, b_.getOffset + offset, b_.getLength, HolodeskType.BYTE)
  })

  // Short
  desers += ((b: ColumnResult, idx: Int, offset: Int) => {
    val b_ = b.asInstanceOf[ByteArrayColumnResult]
    SerDeHelper.deserialize(b_.getData, b_.getOffset + offset, b_.getLength, HolodeskType.SHORT)
  })

  // boolean
  desers += ((b: ColumnResult, idx: Int, offset: Int) => {
    val b_ = b.asInstanceOf[ByteArrayColumnResult]
    SerDeHelper.deserialize(b_.getData, b_.getOffset + offset, b_.getLength, HolodeskType.BOOLEAN)
  })

  // int
  desers += ((b: ColumnResult, idx: Int, offset: Int) => {
    val b_ = b.asInstanceOf[ByteArrayColumnResult]
    SerDeHelper.deserialize(b_.getData, b_.getOffset + offset, b_.getLength, HolodeskType.INT)
  })

  // long
  desers += ((b: ColumnResult, idx: Int, offset: Int) => {
    val b_ = b.asInstanceOf[ByteArrayColumnResult]
    SerDeHelper.deserialize(b_.getData, b_.getOffset + offset, b_.getLength, HolodeskType.LONG)
  })

  // float
  desers += ((b: ColumnResult, idx: Int, offset: Int) => {
    val b_ = b.asInstanceOf[ByteArrayColumnResult]
    SerDeHelper.deserialize(b_.getData, b_.getOffset + offset, b_.getLength, HolodeskType.FLOAT)
  })

  // double
  desers += ((b: ColumnResult, idx: Int, offset: Int) => {
    val b_ = b.asInstanceOf[ByteArrayColumnResult]
    SerDeHelper.deserialize(b_.getData, b_.getOffset + offset, b_.getLength, HolodeskType.DOUBLE)
  })

  // Char
  desers += ((b: ColumnResult, idx: Int, offset: Int) => {
    val b_ = b.asInstanceOf[ByteArrayColumnResult]
    SerDeSupplementaryHelper.deserialize(b_.getData, b_.getOffset + offset, b_.getLength, HolodeskType.CHAR)
  })

  // String
  desers += ((b: ColumnResult, idx: Int, offset: Int) => {
    val b_ = b.asInstanceOf[ByteArrayColumnResult]
    SerDeSupplementaryHelper.deserialize(b_.getData, b_.getOffset + offset, b_.getLength, HolodeskType.STRING)
  })

  // Varchar2
  desers += ((b: ColumnResult, idx: Int, offset: Int) => {
    val b_ = b.asInstanceOf[ByteArrayColumnResult]
    SerDeSupplementaryHelper.deserialize(b_.getData, b_.getOffset + offset, b_.getLength, HolodeskType.VARCHAR2)
  })

  // Array
  desers += ((b: ColumnResult, idx: Int, offset: Int) => {
    val b_ = b.asInstanceOf[ByteArrayColumnResult]
    val t = secondaryType(idx)
    val av = new Array[Any](t.length)
    var i = 0
    var offset = 0
    while (i < t.length) {
      if (offset >= b_.getLength) {
        av(i) = null
        i += 1
      } else {
        av(i) = desers(t(i))(b, idx, offset)
        offset += HolodeskCoreFastSerde.WidthByType(t(i))
        i += 1
      }
    }
    av
  })

  // Other
  desers += ((b: ColumnResult, idx: Int, offset: Int) => {
    val b_ = b.asInstanceOf[ByteArrayColumnResult]
    SerDeHelper.deserialize(b_.getData, b_.getOffset + offset, b_.getLength, HolodeskType.OTHER)
  })

  // Void(placeholder): This piece of code won't be executed
  desers += ((b: ColumnResult, idx: Int, offset: Int) => {
    throw new Exception("Unsupported holodesk type void for v2 deserialization")
  })

  // Wrdecimal(placeholder): This piece of code won't be executed
  desers += ((b: ColumnResult, idx: Int, offset: Int) => {
    val b_ = b.asInstanceOf[ByteArrayColumnResult]
    SerDeHelper.deserialize(b_.getData, b_.getOffset + offset, b_.getLength, HolodeskType.OTHER)
  })

  // Varchar:
  desers += ((b: ColumnResult, idx: Int, offset: Int) => {
    val b_ = b.asInstanceOf[ByteArrayColumnResult]
    SerDeSupplementaryHelper.deserialize(b_.getData, b_.getOffset + offset, b_.getLength, HolodeskType.VARCHAR)
  })

  //totaltype
  desers += ((b: ColumnResult, idx: Int, offset: Int) => {
    throw new RuntimeException("total type place holder, not implemented")
  })

  desers += ((b: ColumnResult, idx: Int, offset: Int) => {
    val b_ = b.asInstanceOf[ByteArrayColumnResult]
    SerDeHelper.deserialize(b_.getData, b_.getOffset + offset, b_.getLength, HolodeskType.DECIMAL_128)
  })

  def getValue(columnResult: ColumnResult, idx: Int, offset: Int = 0) = {
    val b = columnResult.asInstanceOf[ByteArrayColumnResult]
    if (!b.isNull) {
      desers(primitiveType(idx))(columnResult, idx, offset)
    } else {
      null
    }
  }

}

object HolodeskCoreFastSerde {

  val WidthByType = Array(
    SerDeHelper.ByteSize,
    SerDeHelper.ShortSize,
    SerDeHelper.BooleanSize,
    SerDeHelper.IntSize,
    SerDeHelper.LongSize,
    SerDeHelper.FloatSize,
    SerDeHelper.DoubleSize,
    //holodesk type char
    SerDeHelper.OtherSize,
    //holodesk type string
    SerDeHelper.OtherSize,
    //holodesk type varchar2
    SerDeHelper.OtherSize,
    //holodesk type array
    SerDeHelper.OtherSize,
    //holodesk type other
    SerDeHelper.OtherSize,
    //holodesk type void
    SerDeHelper.OtherSize,
    //holodesk type wrdecimal
    SerDeHelper.OtherSize,
    //holodesk type varchar
    SerDeHelper.OtherSize
  )

  def getType(v: Any): Int = {
    v match {
      case v: Byte => HolodeskType.BYTE
      case v: Short => HolodeskType.SHORT
      case v: Boolean => HolodeskType.BOOLEAN
      case v: Int => HolodeskType.INT
      case v: Long => HolodeskType.LONG
      case v: Float => HolodeskType.FLOAT
      case v: Double => HolodeskType.DOUBLE
      case v: String => HolodeskType.STRING
      case v: Text => HolodeskType.STRING
      // Complex type, support shark only.
      // Internal usage
      case v: Array[_] => if (!v.isInstanceOf[Array[Byte]]) {
        HolodeskType.ARRAY
      } else {
        HolodeskType.OTHER
      }
    }
  }

  // For filter generator usage
  // Don't recommand use this interface for table serde
  private type Ser = (Any) => Array[Byte]
  private val sers = new ArrayBuffer[Ser]
  sers += ((v: Any) => {
    SerDeHelper.serialize(v, HolodeskType.BYTE)
  })
  sers += ((v: Any) => {
    SerDeHelper.serialize(v, HolodeskType.SHORT)
  })
  sers += ((v: Any) => {
    SerDeHelper.serialize(v, HolodeskType.BOOLEAN)
  })
  sers += ((v: Any) => {
    SerDeHelper.serialize(v, HolodeskType.INT)
  })
  sers += ((v: Any) => {
    SerDeHelper.serialize(v, HolodeskType.LONG)
  })
  sers += ((v: Any) => {
    SerDeHelper.serialize(v, HolodeskType.FLOAT)
  })
  sers += ((v: Any) => {
    SerDeHelper.serialize(v, HolodeskType.DOUBLE)
  })
  sers += ((v: Any) => {
    SerDeSupplementaryHelper.serialize(v.asInstanceOf[Text], HolodeskType.CHAR)
  })

  sers += ((v: Any) => {
    SerDeSupplementaryHelper.serialize(v.asInstanceOf[Text], HolodeskType.STRING)
  })
  sers += ((v: Any) => {
    SerDeSupplementaryHelper.serialize(v.asInstanceOf[Text], HolodeskType.VARCHAR2)
  })

  // Array
  sers += ((v: Any) => {
    throw new Exception("Unsupported type")
  })
  // Other
  sers += ((v: Any) => {
    SerDeHelper.serialize(v.asInstanceOf[Array[Byte]], HolodeskType.OTHER)
  })

  // Void
  sers += ((v: Any) => {
    throw new Exception("Unsupported type")
  })

  // Wrdecimal
  sers += ((v: Any) => {
    throw new Exception("Unsupported type")
  })

  // Varchar
  sers += ((v: Any) => {
    SerDeSupplementaryHelper.serialize(v.asInstanceOf[Text], HolodeskType.VARCHAR)
  })


  def serialize(obj: Any, tp: Int) = {
    sers(tp)(obj)
  }

}

