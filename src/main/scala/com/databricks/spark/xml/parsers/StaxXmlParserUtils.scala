/*
 * Copyright 2019 Databricks
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.databricks.spark.xml.parsers

import com.databricks.spark.xml.XmlOptions
import javax.xml.stream.XMLEventReader
import java.io.StringReader
import javax.xml.stream.{ EventFilter, XMLEventReader, XMLInputFactory, XMLStreamConstants }
import javax.xml.stream.events._

import scala.annotation.tailrec
import scala.collection.JavaConverters._

import scala.collection.JavaConverters.asScalaIteratorConverter

private[xml] object StaxXmlParserUtils {

  private val factory: XMLInputFactory = {
    val factory = XMLInputFactory.newInstance()
    factory.setProperty(XMLInputFactory.IS_NAMESPACE_AWARE, false)
    factory.setProperty(XMLInputFactory.IS_COALESCING, true)
    factory
  }

  def filteredReader(xml: String): XMLEventReader = {
    val filter = new EventFilter {
      override def accept(event: XMLEvent): Boolean =
      // Ignore comments and processing instructions
        event.getEventType match {
          case XMLStreamConstants.COMMENT | XMLStreamConstants.PROCESSING_INSTRUCTION => false
          case _ => true
        }
    }
    // It does not have to skip for white space, since `XmlInputFormat`
    // always finds the root tag without a heading space.
    val eventReader = factory.createXMLEventReader(new StringReader(xml))
    factory.createFilteredReader(eventReader, filter)
  }

  def gatherRootAttributes(parser: XMLEventReader): Array[Attribute] = {
    val rootEvent =
      StaxXmlParserUtils.skipUntil(parser, XMLStreamConstants.START_ELEMENT)
    rootEvent.asStartElement.getAttributes.asScala.map(_.asInstanceOf[Attribute]).toArray
  }

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
      case c: Characters if c.isWhiteSpace =>
        parser.nextEvent()
        checkEndElement(parser)
      case _ => false
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
