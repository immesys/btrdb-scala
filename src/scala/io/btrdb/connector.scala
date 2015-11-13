package io.btrdb

import java.nio.channels._
import java.net._
import io.jvm.uuid._

case class BTrDBException(msg:String)  extends Exception(msg)

case class StatTuple(t : Long,
                     min : Double, mean : Double, max : Double, count : Long)

case class RawTuple(t : Long,
                    v : Double)

case class ChangedRange(start : Long,
                        end : Long)

@SerialVersionUID(1L)
class BTrDB (server : String, port : Int) extends Serializable
{
  @transient
  private[this] var sock : SocketChannel = null
  @transient
  private[this] var echotag = 1

  def version = "0.0.2"
  def connect()
  {
    if (sock != null && sock.isConnected())
    {
      println("Already connected\n")
      return
    }
    sock = SocketChannel.open()
    sock.connect(new InetSocketAddress(server, port))
  }

  connect()

  def close()
  {
    sock.close()
  }
/*
  def getNext(stream : String, time : Long, backward : Bool)
   : RawTuple =
  {
    //TODO
  }
  */
  def getStatistical (stream : String, resolution : Int, startTime : Long, endTime : Long, version : Long = 0)
   : (String, Long, Iterator[StatTuple]) = getStatisticalU(UUID(stream), resolution, startTime, endTime, version)

  def getStatisticalU (stream : UUID, resolution : Int, startTime : Long, endTime : Long, version : Long = 0)
   : (String, Long, Iterator[StatTuple]) =
  {

    //Write the outgoing request
    var msgEchoTag : Long = 0
    this.synchronized
    {
      msgEchoTag = echotag
      echotag += 1
    }
    var msgb = new org.capnproto.MessageBuilder()
    var req = msgb.initRoot(BTrDBCapnP.Request.factory)
    req.setEchoTag(msgEchoTag)
    var qsv = req.initQueryStatisticalValues()
    qsv.setUuid(stream.byteArray)
    qsv.setVersion(version)
    qsv.setPointWidth(resolution.toByte)
    qsv.setStartTime(startTime)
    qsv.setEndTime(endTime)
    this.synchronized
    {
      org.capnproto.Serialize.write(sock, msgb)
    }
    //Read the response. For now, we assume only one in-flight request
    //So the response echotag should match our echotag
    var qversion : Long = -1
    var status : String = "<UNSET>"
    var it = new Iterator[StatTuple]
    {
      var cursegment : BTrDBCapnP.Response.Reader = null
      var curlist : org.capnproto.StructList.Reader[BTrDBCapnP.StatisticalRecord.Reader] = null
      var index : Int = 0
      var listmax : Int = 0
      var more : Boolean = true

      {
        checkSegment()
      }
      def checkSegment() : Boolean =
      {
        if (index == listmax && !more)
          return false
        if (cursegment == null || index == listmax)
        {
          var msg : org.capnproto.MessageReader = null
          this.synchronized
          {
            msg = org.capnproto.Serialize.read(sock)
          }
          cursegment = msg.getRoot(BTrDBCapnP.Response.factory)
          status = cursegment.getStatusCode().toString()
          if (cursegment.getEchoTag() != msgEchoTag)
          {
            throw new IllegalStateException("We do not support out of order segments")
          }
          more = !cursegment.getFinal()
          var sr = cursegment.getStatisticalRecords()
          qversion = sr.getVersion()
          curlist = sr.getValues()
          listmax = curlist.size()
          index = 0
        }
        return true
      }
      override def next() =
      {
        if (!hasNext())
          throw new NoSuchElementException()
        val r = curlist.get(index)
        index += 1
        StatTuple(r.getTime(), r.getMin(), r.getMean(), r.getMax(), r.getCount())
      }
      override def hasNext() =
      {
        checkSegment()
      }
    }
    (status, qversion, it)
  }

  def getRaw(stream : String, startTime : Long, endTime : Long, version : Long = 0)
   : (String, Long, Iterator[RawTuple]) = getRawU(UUID(stream), startTime, endTime, version)

  def getRawU(stream : UUID, startTime : Long, endTime : Long, version : Long = 0)
   : (String, Long, Iterator[RawTuple]) =
  {
    //Write the outgoing request
    var msgEchoTag : Long = 0
    this.synchronized
    {
      msgEchoTag = echotag
      echotag += 1
    }
    var msgb = new org.capnproto.MessageBuilder()
    var req = msgb.initRoot(BTrDBCapnP.Request.factory)
    req.setEchoTag(msgEchoTag)
    var qsv = req.initQueryStandardValues()
    qsv.setUuid(stream.byteArray)
    qsv.setVersion(version)
    qsv.setStartTime(startTime)
    qsv.setEndTime(endTime)
    this.synchronized
    {
      org.capnproto.Serialize.write(sock, msgb)
    }
    //Read the response. For now, we assume only one in-flight request
    //So the response echotag should match our echotag
    var qversion : Long = -1
    var status : String = "<UNSET>"
    var it = new Iterator[RawTuple]
    {
      var cursegment : BTrDBCapnP.Response.Reader = null
      var curlist : org.capnproto.StructList.Reader[BTrDBCapnP.Record.Reader] = null
      var index : Int = 0
      var listmax : Int = 0
      var more : Boolean = true

      {
        checkSegment()
      }
      def checkSegment() : Boolean =
      {
        if (index == listmax && !more)
          return false
        if (cursegment == null || index == listmax)
        {
          var msg : org.capnproto.MessageReader = null
          this.synchronized
          {
            msg = org.capnproto.Serialize.read(sock)
          }
          cursegment = msg.getRoot(BTrDBCapnP.Response.factory)
          status = cursegment.getStatusCode().toString()
          if (cursegment.getEchoTag() != msgEchoTag)
          {
            throw new IllegalStateException("We do not support out of order segments")
          }
          more = !cursegment.getFinal()
          var sr = cursegment.getRecords()
          qversion = sr.getVersion()
          curlist = sr.getValues()
          listmax = curlist.size()
          index = 0
        }
        return true
      }
      override def next() =
      {
        if (!hasNext())
          throw new NoSuchElementException()
        val r = curlist.get(index)
        index += 1
        RawTuple(r.getTime(), r.getValue())
      }
      override def hasNext() =
      {
        checkSegment()
      }
    }
    (status, qversion, it)
  }

  def getChangedRanges(stream : String, from : Long, to : Long, resolution : Int)
    : (String, Long, Iterator[ChangedRange]) = getChangedRangesU(UUID(stream), from, to, resolution)

  def getChangedRangesU(stream : UUID, from : Long, to : Long, resolution : Int)
    : (String, Long, Iterator[ChangedRange]) =
  {
    //Write the outgoing request
    var msgEchoTag : Long = 0
    this.synchronized
    {
      msgEchoTag = echotag
      echotag += 1
    }
    var msgb = new org.capnproto.MessageBuilder()
    var req = msgb.initRoot(BTrDBCapnP.Request.factory)
    req.setEchoTag(msgEchoTag)
    var qsv = req.initQueryChangedRanges()
    qsv.setUuid(stream.byteArray)
    qsv.setFromGeneration(from)
    qsv.setToGeneration(to)
    qsv.setResolution(resolution.toByte)
    this.synchronized
    {
      org.capnproto.Serialize.write(sock, msgb)
    }
    //Read the response. For now, we assume only one in-flight request
    //So the response echotag should match our echotag
    var version : Long = -1
    var status : String = "<UNSET>"
    var it = new Iterator[ChangedRange]
    {
      var cursegment : BTrDBCapnP.Response.Reader = null
      var curlist : org.capnproto.StructList.Reader[BTrDBCapnP.ChangedRange.Reader] = null
      var index : Int = 0
      var listmax : Int = 0
      var more : Boolean = true

      {
        checkSegment()
      }
      def checkSegment() : Boolean =
      {
        if (index == listmax && !more)
          return false
        if (cursegment == null || index == listmax)
        {
          var msg : org.capnproto.MessageReader = null
          this.synchronized
          {
            msg = org.capnproto.Serialize.read(sock)
          }
          cursegment = msg.getRoot(BTrDBCapnP.Response.factory)
          status = cursegment.getStatusCode().toString()
          if (cursegment.getEchoTag() != msgEchoTag)
          {
            throw new IllegalStateException("We do not support out of order segments")
          }
          more = !cursegment.getFinal()
          var sr = cursegment.getChangedRngList()
          version = sr.getVersion()
          curlist = sr.getValues()
          listmax = curlist.size()
          index = 0
        }
        return true
      }
      override def next() =
      {
        if (!hasNext())
          throw new NoSuchElementException()
        val r = curlist.get(index)
        index += 1
        ChangedRange(r.getStartTime(), r.getEndTime())
      }
      override def hasNext() =
      {
        checkSegment()
      }
    }
    (status, version, it)
  }

  def getVersions(streams : Seq[String])
    : (String, Iterator[Long]) = getVersionsU(streams.map(UUID(_)))

  def getVersionsU(streams : Seq[UUID])
    : (String, Iterator[Long]) =
  {
    //Write the outgoing request
    var msgEchoTag : Long = 0
    this.synchronized
    {
      msgEchoTag = echotag
      echotag += 1
    }
    var msgb = new org.capnproto.MessageBuilder()
    var req = msgb.initRoot(BTrDBCapnP.Request.factory)
    req.setEchoTag(msgEchoTag)
    var qsv = req.initQueryVersion()
    var ul = qsv.initUuids(streams.length)
    for (i <- 0 until streams.length)
    {
      ul.set(i, new org.capnproto.Data.Reader(streams(i).byteArray))
    }
    this.synchronized
    {
      org.capnproto.Serialize.write(sock, msgb)
    }
    //Read the response. For now, we assume only one in-flight request
    //So the response echotag should match our echotag
    var status : String = "<UNSET>"
    var it = new Iterator[Long]
    {
      var cursegment : BTrDBCapnP.Response.Reader = null
      var curlist : org.capnproto.PrimitiveList.Long.Reader = null
      var index : Int = 0
      var listmax : Int = 0
      var more : Boolean = true

      {
        checkSegment()
      }
      def checkSegment() : Boolean =
      {
        if (index == listmax && !more)
          return false
        if (cursegment == null || index == listmax)
        {
          var msg : org.capnproto.MessageReader = null
          this.synchronized
          {
            msg = org.capnproto.Serialize.read(sock)
          }
          cursegment = msg.getRoot(BTrDBCapnP.Response.factory)
          status = cursegment.getStatusCode().toString()
          if (cursegment.getEchoTag() != msgEchoTag)
          {
            throw new IllegalStateException("We do not support out of order segments")
          }
          more = !cursegment.getFinal()
          var sr = cursegment.getVersionList()
          curlist = sr.getVersions()
          listmax = curlist.size()
          index = 0
        }
        return true
      }
      override def next() =
      {
        if (!hasNext())
          throw new NoSuchElementException()
        val r = curlist.get(index)
        index += 1
        r
      }
      override def hasNext() =
      {
        checkSegment()
      }
    }
    (status, it)
  }

  def deleteValues(stream : String, start : Long, end : Long)
    : String = deleteValuesU(UUID(stream), start, end)

  def deleteValuesU(stream : UUID, start : Long, end : Long)
    : String =
  {
    //Write the outgoing request
    var msgEchoTag : Long = 0
    this.synchronized
    {
      msgEchoTag = echotag
      echotag += 1
    }
    var msgb = new org.capnproto.MessageBuilder()
    var req = msgb.initRoot(BTrDBCapnP.Request.factory)
    req.setEchoTag(msgEchoTag)
    var qsv = req.initDeleteValues()
    qsv.setUuid(stream.byteArray)
    qsv.setStartTime(start)
    qsv.setEndTime(end)
    this.synchronized
    {
      org.capnproto.Serialize.write(sock, msgb)
    }
    //Read the response. For now, we assume only one in-flight request
    //So the response echotag should match our echotag
    var msg : org.capnproto.MessageReader = null
    this.synchronized
    {
      msg = org.capnproto.Serialize.read(sock)
    }
    var cursegment = msg.getRoot(BTrDBCapnP.Response.factory)
    if (cursegment.getEchoTag() != msgEchoTag)
    {
      throw new IllegalStateException("We do not support out of order segments")
    }
    var status = cursegment.getStatusCode().toString()
    return status
  }

  def insertValues(stream : String, values : Iterator[RawTuple], sync : Boolean = false)
    : String = insertValuesU(UUID(stream), values, sync)

  def insertValuesU(stream : UUID, values : Iterator[RawTuple], sync : Boolean = false)
    : String =
  {
    //We chunk the values into chunks
    val chunkSize = 16384
    var gstatus = BTrDBCapnP.StatusCode.OK
    values.grouped(16384).foreach (chunk =>
    {
      //Write the outgoing request
      var msgEchoTag : Long = 0
      this.synchronized
      {
        msgEchoTag = echotag
        echotag += 1
      }
      var msgb = new org.capnproto.MessageBuilder()
      var req = msgb.initRoot(BTrDBCapnP.Request.factory)
      req.setEchoTag(msgEchoTag)
      var qsv = req.initInsertValues()
      qsv.setUuid(stream.byteArray)
      qsv.setSync(sync)
      var vals = qsv.initValues(chunk.size)
      for (i <- 0 until chunk.size)
      {
        var v = vals.get(i)
        v.setTime(chunk(i).t)
        v.setValue(chunk(i).v)
      }
      this.synchronized
      {
        org.capnproto.Serialize.write(sock, msgb)
      }
      //Now get the response
      var msg : org.capnproto.MessageReader = null
      this.synchronized
      {
        msg = org.capnproto.Serialize.read(sock)
      }
      var cursegment = msg.getRoot(BTrDBCapnP.Response.factory)
      if (cursegment.getEchoTag() != msgEchoTag)
      {
        throw new IllegalStateException("We do not support out of order segments")
      }
      var status = cursegment.getStatusCode()
      if (status != BTrDBCapnP.StatusCode.OK)
      {
        gstatus = status
      }
    })
    gstatus.toString
  }
}
