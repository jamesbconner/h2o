package water.parser;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.poi.hssf.eventusermodel.*;
import org.apache.poi.hssf.eventusermodel.dummyrecord.LastCellOfRowDummyRecord;
import org.apache.poi.hssf.eventusermodel.dummyrecord.MissingCellDummyRecord;
import org.apache.poi.hssf.record.*;
import org.apache.poi.poifs.filesystem.POIFSFileSystem;
import water.parser.ParseDataset.DParseTask;

public class XlsParser implements HSSFListener {

  private final POIFSFileSystem _fs;
  private final DParseTask _callback;
  private FormatTrackingHSSFListener _formatListener;

  public XlsParser(InputStream is, DParseTask callback) throws IOException {
    _fs = new POIFSFileSystem(is);
    _callback = callback;
  }

  public void parse() throws IOException {
    MissingRecordAwareHSSFListener listener = new MissingRecordAwareHSSFListener(this);
    _formatListener = new FormatTrackingHSSFListener(listener);

    HSSFEventFactory factory = new HSSFEventFactory();
    HSSFRequest request = new HSSFRequest();
    request.addListenerForAllRecords(_formatListener);

    factory.processWorkbookEvents(request, _fs);
//    _handler.handleFinished(_firstRow);

  }
  
  ArrayList<String> _columnNames = new ArrayList();
  boolean _firstRow;

  @Override
  public void processRecord(Record record) {
    int curCol = -1;
    double curNum = Double.NaN;
    String curStr = null;

    switch( record.getSid() ) {
      case BoundSheetRecord.sid:
      case BOFRecord.sid:
        // we just run together multiple sheets
        break;
      case SSTRecord.sid:
        _sstRecord = (SSTRecord) record;
        break;
      case BlankRecord.sid:
        BlankRecord brec = (BlankRecord) record;

        curCol = brec.getColumn();
        curStr = "";
        break;
      case BoolErrRecord.sid:
        BoolErrRecord berec = (BoolErrRecord) record;

        curCol = berec.getColumn();
        curStr = "";
        break;

      case FormulaRecord.sid:
        FormulaRecord frec = (FormulaRecord) record;

        curCol = frec.getColumn();
        curNum = frec.getValue();

        if( Double.isNaN(curNum) ) {
          // Formula result is a string
          // This is stored in the next record
          _outputNextStringRecord = true;
          _nextCol = frec.getColumn();
        }
        break;
      case StringRecord.sid:
        if( _outputNextStringRecord ) {
          // String for formula
          StringRecord srec = (StringRecord) record;
          curStr = srec.getString();
          curCol = _nextCol;
          _outputNextStringRecord = false;
        }
        break;
      case LabelRecord.sid:
        LabelRecord lrec = (LabelRecord) record;

        curCol = lrec.getColumn();
        curStr = lrec.getValue();
        break;
      case LabelSSTRecord.sid:
        LabelSSTRecord lsrec = (LabelSSTRecord) record;
        if( _sstRecord == null ) {
          System.err.println("[ExcelParser] Missing SST record");
        } else {
          curCol = lsrec.getColumn();
          curStr = _sstRecord.getString(lsrec.getSSTIndex()).toString();
        }
        break;
      case NoteRecord.sid:
        System.err.println("[ExcelParser] Warning cell notes are unsupported");
        break;
      case NumberRecord.sid:
        NumberRecord numrec = (NumberRecord) record;
        curCol = numrec.getColumn();
        curNum = numrec.getValue();
        break;
      case RKRecord.sid:
        System.err.println("[ExcelParser] Warning RK records are unsupported");
        break;
      default:
        break;
    }

    // Handle missing column
    if( record instanceof MissingCellDummyRecord ) {
      MissingCellDummyRecord mc = (MissingCellDummyRecord) record;
      curCol = mc.getColumn();
      curNum = Double.NaN;
    }

    // Handle end of row
    if( record instanceof LastCellOfRowDummyRecord ) {
      if (_firstRow) {
        _firstRow = false;
        String[] arr = new String[_columnNames.size()];
        arr = _columnNames.toArray(arr);
        _callback.setColumnNames(arr);
      }
      System.out.println("new line");
//      _callback.newLine();
    }

    if (curCol == -1)
      return;
    
    if (_firstRow) {
      _columnNames.add(curStr == null ? "" : curStr);
    } else {
      if (curStr == null)
        _callback.addCol(curCol, curNum);
      else 
        _callback.addCol(curCol, curStr);
    }
  }
  /*
   * public static class Engine implements ParseDataset.ParseEngine { @Override
   * public void doParse(InputStream is, ParseHandler h) throws IOException {
   * XlsParser p = new XlsParser(is, h); p.process();
   * MissingRecordAwareHSSFListener listener = new
   * MissingRecordAwareHSSFListener(this); _formatListener = new
   * FormatTrackingHSSFListener(listener);
   *
   * HSSFEventFactory factory = new HSSFEventFactory(); HSSFRequest request =
   * new HSSFRequest();
   *
   * request.addListenerForAllRecords(_formatListener);
   *
   * factory.processWorkbookEvents(request, _fs);
   * _handler.handleFinished(_firstRow); } }
   */
//  private String[] _firstRow = null;
  private SSTRecord _sstRecord;
  private int _nextCol;
  private boolean _outputNextStringRecord;

  /*
   * public XlsParser(InputStream is, ParseHandler handler) throws IOException {
   * _fs = new POIFSFileSystem(is); _handler = handler; }
   *
   * public void process() throws IOException { MissingRecordAwareHSSFListener
   * listener = new MissingRecordAwareHSSFListener(this); _formatListener = new
   * FormatTrackingHSSFListener(listener);
   *
   * HSSFEventFactory factory = new HSSFEventFactory(); HSSFRequest request =
   * new HSSFRequest(); request.addListenerForAllRecords(_formatListener);
   *
   * factory.processWorkbookEvents(request, _fs);
   * _handler.handleFinished(_firstRow); }
   */
  
  
  
  public static void main(String[] argv) throws IOException {
    FileInputStream fs = new FileInputStream("/home/peta/iris.xls");
    DParseTask callback = new DParseTask();
    XlsParser p = new XlsParser(fs,callback);
    p.parse();
  }
}

