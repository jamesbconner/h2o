package water.parser;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.openxml4j.exceptions.OpenXML4JException;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.xssf.eventusermodel.XSSFReader;
import org.apache.poi.xssf.model.SharedStringsTable;
import org.apache.poi.xssf.usermodel.XSSFRichTextString;
import org.xml.sax.*;
import org.xml.sax.helpers.DefaultHandler;
import org.xml.sax.helpers.XMLReaderFactory;

import water.parser.ParseDataset.ParseEngine;
import water.parser.ParseDataset.ParseHandler;

import com.google.common.base.Strings;

public class XlsxParser {
  public static class Engine implements ParseEngine {
    @Override
    public void doParse(InputStream is, ParseHandler h) throws Exception {
      XlsxParser p = new XlsxParser(is, h);
      p.process();
    }
  }


  private final ParseHandler       _handler;
  private final XSSFReader         _reader;
  private final SharedStringsTable _sst;

  private String[] _firstRow = null;
  private String[] _rowStrs = new String[0];
  private double[] _rowNums = new double[0];

  public XlsxParser(InputStream is, ParseHandler h) throws IOException, InvalidFormatException,
      OpenXML4JException {
    _reader = new XSSFReader(OPCPackage.open(is));
    _sst = _reader.getSharedStringsTable();
    _handler = h;
  }

  public XMLReader makeSheetParser() throws SAXException {
    XMLReader parser = XMLReaderFactory.createXMLReader();
    parser.setContentHandler(new SheetHandler());
    return parser;
  }

  public void process() throws IOException, SAXException, InvalidFormatException {
    XMLReader parser = makeSheetParser();
    Iterator<InputStream> it = _reader.getSheetsData();
    while( it.hasNext() ) {
      InputStream sheet = it.next();
      try {
        parser.parse(new InputSource(sheet));
      } finally {
        sheet.close();
      }
    }
    _handler.handleFinished(_firstRow);
  }

  private class SheetHandler extends DefaultHandler {
    private String  _lastContents;
    private boolean _nextIsString;
    private int _curCol;
    private String _rowStr;

    public void startElement(String uri, String localName, String name,
        Attributes attributes) throws SAXException {
      // c => cell
      if( name.equals("row") ) {
        _rowStr = Strings.nullToEmpty(attributes.getValue("r"));
      } else if( name.equals("c") ) {
        // Figure out if the value is an index in the SST
        String cellType = attributes.getValue("t");
        if( cellType != null && cellType.equals("s") ) {
          _nextIsString = true;
        } else {
          _nextIsString = false;
        }

        String cell = attributes.getValue("r");
        cell = cell.substring(0, cell.length() - _rowStr.length());
        _curCol = 0;
        for( int i = 0; i < cell.length(); ++i ) {
          _curCol *= 26;
          char c = cell.charAt(i);
          assert 'A' <= c && c <= 'Z';
          _curCol += c - 'A';
        }
      }
      // Clear contents cache
      _lastContents = "";
    }

    public void endElement(String uri, String localName, String name)
        throws SAXException {
      // Process the last contents as required.
      // Do now, as characters() may be called more than once
      if( _nextIsString ) {
        int idx = Integer.parseInt(_lastContents);
        _lastContents = new XSSFRichTextString(_sst.getEntryAt(idx)).toString();
        _nextIsString = false;
      }

      // v => contents of a cell
      // Output after we've seen the string contents
      if( name.equals("v") ) {
        if( _firstRow == null && _curCol >= _rowNums.length ) {
          _rowNums = Arrays.copyOf(_rowNums, _curCol+1);
          _rowStrs = Arrays.copyOf(_rowStrs, _curCol+1);
        }
        if( _curCol < _rowNums.length ) {
          try {
            _rowNums[_curCol] = Double.parseDouble(_lastContents);
          } catch( NumberFormatException e ) {
            _rowNums[_curCol] = Double.NaN;
            _rowStrs[_curCol] = _lastContents;
          }
        }
      } else if( name.equals("row") ) {
        if( _firstRow == null ) _firstRow = _rowStrs.clone();
        _handler.handleRow(_rowNums, _rowStrs);
      }
    }

    public void characters(char[] ch, int start, int length)
        throws SAXException {
      _lastContents += new String(ch, start, length);
    }
  }
}
