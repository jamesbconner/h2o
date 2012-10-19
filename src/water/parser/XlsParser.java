package water.parser;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

import org.apache.poi.hssf.eventusermodel.*;
import org.apache.poi.hssf.eventusermodel.dummyrecord.LastCellOfRowDummyRecord;
import org.apache.poi.hssf.eventusermodel.dummyrecord.MissingCellDummyRecord;
import org.apache.poi.hssf.record.*;
import org.apache.poi.poifs.filesystem.POIFSFileSystem;

import water.parser.ParseDataset.ParseHandler;

public class XlsParser implements HSSFListener {
  public static class Engine implements ParseDataset.ParseEngine {
    @Override
    public void doParse(InputStream is, ParseHandler h) throws IOException {
      XlsParser p = new XlsParser(is, h);
      p.process();
    }
  }

  private final POIFSFileSystem _fs;
  private final ParseHandler _handler;

  private String[] _firstRow = null;
  private String[] _rowStrs = new String[0];
  private double[] _rowNums = new double[0];

  private FormatTrackingHSSFListener _formatListener;
  private SSTRecord _sstRecord;

  private int _nextCol;
  private boolean _outputNextStringRecord;

  public XlsParser(InputStream is, ParseHandler handler) throws IOException {
    _fs = new POIFSFileSystem(is);
    _handler = handler;
  }

  public void process() throws IOException {
    MissingRecordAwareHSSFListener listener = new MissingRecordAwareHSSFListener(this);
    _formatListener = new FormatTrackingHSSFListener(listener);

    HSSFEventFactory factory = new HSSFEventFactory();
    HSSFRequest request = new HSSFRequest();
    request.addListenerForAllRecords(_formatListener);

    factory.processWorkbookEvents(request, _fs);
    _handler.handleFinished(_firstRow);
}

  @Override
  public void processRecord(Record record) {
    int curCol = -1;
    double curNum = Double.NaN;
    String curStr = null;

    switch (record.getSid()) {
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
      if(_outputNextStringRecord) {
        // String for formula
        StringRecord srec = (StringRecord)record;
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
      if(_sstRecord == null) {
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
    if(record instanceof MissingCellDummyRecord) {
        MissingCellDummyRecord mc = (MissingCellDummyRecord)record;
        curCol = mc.getColumn();
        curNum = Double.NaN;
    }

    // Handle end of row
    if(record instanceof LastCellOfRowDummyRecord) {
      if( _firstRow == null ) _firstRow = _rowStrs.clone();
      _handler.handleRow(_rowNums, _rowStrs);
      Arrays.fill(_rowNums, Double.NaN);
      Arrays.fill(_rowStrs, null);
    }

    if( curCol == -1 ) return;

    if( _firstRow == null && curCol >= _rowNums.length ) {
      _rowNums = Arrays.copyOf(_rowNums, curCol+1);
      _rowStrs = Arrays.copyOf(_rowStrs, curCol+1);
    }

    if( curCol < _rowNums.length ) {
      _rowNums[curCol] = curNum;
      if( Double.isNaN(curNum) ) _rowStrs[curCol] = curStr;
    }
  }
}
