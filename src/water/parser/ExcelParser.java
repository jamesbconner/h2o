package water.parser;

import java.io.IOException;
import java.io.InputStream;

import org.apache.poi.hssf.eventusermodel.*;
import org.apache.poi.hssf.eventusermodel.dummyrecord.LastCellOfRowDummyRecord;
import org.apache.poi.hssf.eventusermodel.dummyrecord.MissingCellDummyRecord;
import org.apache.poi.hssf.model.HSSFFormulaParser;
import org.apache.poi.hssf.record.*;
import org.apache.poi.poifs.filesystem.POIFSFileSystem;

public class ExcelParser implements HSSFListener {
  private int _prevRow;
  private int _prevCol;
  private int _nextRow;
  private int _nextCol;

  private FormatTrackingHSSFListener _formatListener;
  private POIFSFileSystem _fs;
  private SSTRecord _sstRecord;

  private boolean _outputNextStringRecord;

  public ExcelParser(InputStream is) throws IOException {
    _fs = new POIFSFileSystem(is);
  }

  public void process() throws IOException {
    MissingRecordAwareHSSFListener listener = new MissingRecordAwareHSSFListener(this);
    _formatListener = new FormatTrackingHSSFListener(listener);

    HSSFEventFactory factory = new HSSFEventFactory();
    HSSFRequest request = new HSSFRequest();
    request.addListenerForAllRecords(_formatListener);

    factory.processWorkbookEvents(request, _fs);
}

  @Override
  public void processRecord(Record record) {
    int curRow = -1;
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

      curRow = brec.getRow();
      curCol = brec.getColumn();
      curStr = "";
      break;
    case BoolErrRecord.sid:
      BoolErrRecord berec = (BoolErrRecord) record;

      curRow = berec.getRow();
      curCol = berec.getColumn();
      curStr = "";
      break;

    case FormulaRecord.sid:
      FormulaRecord frec = (FormulaRecord) record;

      curRow = frec.getRow();
      curCol = frec.getColumn();

      if(Double.isNaN( frec.getValue() )) {
        // Formula result is a string
        // This is stored in the next record
        _outputNextStringRecord = true;
        _nextRow = frec.getRow();
        _nextCol = frec.getColumn();
      } else {
        curStr = _formatListener.formatNumberDateCell(frec);
      }
      break;
    case StringRecord.sid:
      if(_outputNextStringRecord) {
        // String for formula
        StringRecord srec = (StringRecord)record;
        curStr = srec.getString();
        curRow = _nextRow;
        curCol = _nextCol;
        _outputNextStringRecord = false;
      }
      break;
    case LabelRecord.sid:
      LabelRecord lrec = (LabelRecord) record;

      curRow = lrec.getRow();
      curCol = lrec.getColumn();
      curStr = lrec.getValue();
      break;
    case LabelSSTRecord.sid:
      LabelSSTRecord lsrec = (LabelSSTRecord) record;

      curRow = lsrec.getRow();
      curCol = lsrec.getColumn();
      if(_sstRecord == null) {
        System.err.println("[ExcelParser] Missing SST record");
      } else {
        curStr = _sstRecord.getString(lsrec.getSSTIndex()).toString();
      }
      break;
    case NoteRecord.sid:
      System.err.println("[ExcelParser] Warning cell notes are unsupported");
      break;
    case NumberRecord.sid:
      NumberRecord numrec = (NumberRecord) record;

      curRow = numrec.getRow();
      curCol = numrec.getColumn();
      curNum = numrec.getValue();
      break;
    case RKRecord.sid:
      System.err.println("[ExcelParser] Warning RK records are unsupported");
      break;
    default:
      break;
    }

    // Handle new row
    if(curRow != -1 && curRow != _prevRow) {
        _prevCol = -1;
    }

    // Handle missing column
    if(record instanceof MissingCellDummyRecord) {
        MissingCellDummyRecord mc = (MissingCellDummyRecord)record;
        curRow = mc.getRow();
        curCol = mc.getColumn();
        curNum = Double.NaN;
    }

    if(curRow > -1) _prevRow = curRow;
    if(curCol > -1) _prevCol = curCol;

    // If we got something to print out, do so
    if(thisStr != null) {
        if(curCol > 0) {
            output.print(',');
        }
        output.print(thisStr);
    }


    // Handle end of row
    if(record instanceof LastCellOfRowDummyRecord) {
        // Print out any missing commas if needed
        if(minColumns > 0) {
            // Columns are 0 based
            if(lastColumnNumber == -1) { lastColumnNumber = 0; }
            for(int i=lastColumnNumber; i<(minColumns); i++) {
                output.print(',');
            }
        }

        // We're onto a new row
        lastColumnNumber = -1;

        // End the row
        output.println();
    }
  }

}
