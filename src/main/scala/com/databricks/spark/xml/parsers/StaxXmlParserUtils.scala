package com.databricks.spark.xml.parsers

import com.databricks.spark.xml.XmlOptions
import javax.xml.stream.XMLEventReader
import javax.xml.stream.events._

import scala.annotation.tailrec
import scala.collection.JavaConverters.asScalaIteratorConverter

private[xml] object StaxXmlParserUtils {
  /**
   * Skips elements until this meets the given type of a element
   */
  def skipUntil(parser: XMLEventReader, eventType: Int): XMLEvent = {
    var event = parser.peek
    while (parser.hasNext && event.getEventType != eventType) {
      event = parser.nextEvent
    }
    event
  }

  /**
   * Checks if current event points the EndElement.
   */
  @tailrec
  def checkEndElement(parser: XMLEventReader): Boolean = {
    parser.peek match {
      case _: EndElement | _: EndDocument => true
      case _: StartElement => false
      case _ =>
        // When other events are found here rather than `EndElement` or `StartElement`
        // , we need to look further to decide if this is the end because this can be
        // whitespace between `EndElement` and `StartElement`.
        parser.nextEvent
        checkEndElement(parser)
    }
  }

  /**
   * Produces values map from given attributes.
   */
  def convertAttributesToValuesMap(
                                    attributes: Array[Attribute],
                                    options: XmlOptions): Map[String, String] = {
    if (options.excludeAttributeFlag) {
      Map.empty[String, String]
    } else {
      val attrFields = attributes.map(options.attributePrefix + _.getName.getLocalPart)
      val attrValues = attributes.map(_.getValue)
      val nullSafeValues = {
        if (options.treatEmptyValuesAsNulls) {
          attrValues.map(v => if (v.trim.isEmpty) null else v)
        } else {
          attrValues
        }
      }
      attrFields.zip(nullSafeValues).toMap
    }
  }


  /**
   * Convert the current structure of XML document to a XML string.
   */
  def currentStructureAsString(parser: XMLEventReader, field: StartElement): String = {
    def fieldDepthDelta(event: XMLEvent) = event match {
      case startElement: StartElement if startElement.getName == field.getName => +1
      case endElement: EndElement if endElement.getName == field.getName => -1
      case _ => 0
    }

    @tailrec
    def iterate(fieldDepth: Int, buffer: StringBuffer): String = {
      if (fieldDepth == 0) {
        buffer.toString
      } else {
        parser.peek() match {
          case startElement: StartElement =>
            parser.next()
            val elementWithAttributes = startElement.getAttributes.asScala
              .foldLeft(buffer.append(s"<${startElement.getName}")) {
                case (runningBuffer, attribute: Attribute) =>
                  runningBuffer
                    .append(s""" "${attribute.getName.getLocalPart}"="${attribute.getValue}"""")
              }

            val updatedBuffer = parser.peek() match {
              case endElement: EndElement if endElement.getName == startElement.getName =>
                parser.nextEvent()
                elementWithAttributes.append("/>")
              case _ => elementWithAttributes.append(">")
            }

            iterate(fieldDepth + fieldDepthDelta(startElement), updatedBuffer)
          case endElement: EndElement if endElement.getName == field.getName && fieldDepth <= 1 =>
            buffer.toString
          case endElement: EndElement =>
            parser.next()
            iterate(
              fieldDepth = fieldDepth + fieldDepthDelta(endElement),
              buffer = buffer.append(s"</${endElement.getName}>"))
          case characters: Characters =>
            parser.next()
            iterate(
              fieldDepth = fieldDepth + fieldDepthDelta(characters),
              buffer = buffer.append(s"${characters.getData}"))
          case _: EndDocument =>
            parser.next()
            buffer.toString
        }
      }
    }

    iterate(1, new StringBuffer())
  }

  /**
   * Skip the children of the current XML element.
   */
  def skipChildren(parser: XMLEventReader): Unit = {
    var shouldStop = checkEndElement(parser)
    while (!shouldStop) {
      parser.nextEvent match {
        case _: StartElement =>
          val e = parser.peek
          if (e.isCharacters && e.asCharacters.isWhiteSpace) {
            // There can be a `Characters` event between `StartElement`s.
            // So, we need to check further to decide if this is a data or just
            // a whitespace between them.
            parser.next
          }
          if (parser.peek.isStartElement) {
            skipChildren(parser)
          }
        case _: EndElement =>
          shouldStop = checkEndElement(parser)
        case _: XMLEvent => // do nothing
      }
    }
  }
}
