
package water.api;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import java.io.IOException;
import water.Value;
import water.ValueArray;
import water.parser.CsvParser;

public class Inspect extends Request {
  protected final H2OExistingKey _key = new H2OExistingKey(KEY);
  protected final LongInt _offset = new LongInt(OFFSET,-1l,-1l,Long.MAX_VALUE);
  protected final Int _view = new Int(VIEW, 100, 0, 10000);

  protected void formatAryData(JsonObject obj, ValueArray ary, long rowIdx, int colIdx, String name) {
    if( rowIdx < 0 || rowIdx >= ary._numrows ) return;
    if( colIdx >= ary._cols.length ) return;
    ValueArray.Column c = ary._cols[colIdx];
    if (ary.isNA(rowIdx,colIdx)) {
      obj.addProperty(name,"NA");
    } else if (c._domain != null) {
      obj.addProperty(name,c._domain[(int)ary.data(rowIdx, colIdx)]);
    } else if ((c._size > 0) && (c._scale == 1))  {
      obj.addProperty(name,ary.data (rowIdx, colIdx));
    } else {
      obj.addProperty(name,ary.datad(rowIdx, colIdx));
    }
  }
  protected void formatRowData(JsonObject obj, ValueArray ary, long rowIdx, int colIdx) {
    if (rowIdx < 0)
      rowIdx = ary._numrows + rowIdx;
    formatAryData(obj, ary, rowIdx, colIdx, String.valueOf(rowIdx));
  }

  protected void formatColData(JsonObject obj, ValueArray ary, long rowIdx, int colIdx) {
    if (rowIdx < 0)
      rowIdx = ary._numrows + rowIdx;
    formatAryData(obj, ary, rowIdx, colIdx, ary._cols[colIdx]._name);
  }

  @Override protected Response serve() {
    Value val = _key.value();
    if (val._isArray == 1) {
      ValueArray ary = ValueArray.value(val);
      return serveValueArray(ary);
    } else {
      return serveUnparsedValue(val);
    }
  }

  private Response serveUnparsedValue(Value v) {
    JsonObject result = new JsonObject();
    result.addProperty(VALUE_TYPE, "unparsed");

    byte[] bs = v.getFirstBytes();
    int[] rows_cols = CsvParser.inspect(bs);
    if( rows_cols != null && rows_cols[1] != 0 ) { // Able to parse sanely?
      double bytes_per_row = (double)bs.length/rows_cols[0];
      long rows = (long)(v.length()/bytes_per_row);
      result.addProperty(ROWS,"~"+rows); // approx rows
      result.addProperty(COLS, rows_cols[1]);
    } else {
      result.addProperty(ROWS, "unknown");
      result.addProperty(COLS, "unknown");
    }
    result.addProperty(VALUE_SIZE, v.length());

    Response r = Response.done(result);
    r.addHeader("<div class='alert'>" +
        Parse.link(v._key, "Parse into hex format") +
        "</div>");
    return r;
  }

  private Response serveValueArray(ValueArray ary) {
    PaginatedTable t = new PaginatedTable(argumentsToJson(),_offset.value(), _view.value(), ary._numrows, true);
    JsonObject result = new JsonObject();
    result.addProperty(RequestStatics.VALUE_TYPE, "parsed");
    result.addProperty(RequestStatics.NUM_ROWS, ary._numrows);
    result.addProperty(RequestStatics.NUM_COLS, ary._cols.length);
    result.addProperty(RequestStatics.ROW_SIZE,ary._rowsize);
    result.addProperty(VALUE_SIZE, ary.length());
    // if offset is -1 display the overview
    if (_offset.value() == -1) {
      JsonArray cols = new JsonArray();
      for (int i = 0; i < ary._cols.length; ++i ) {
        ValueArray.Column c = ary._cols[i];
        JsonObject col = new JsonObject();
        col.addProperty(RequestStatics.NAME, c._name);
        col.addProperty(OFFSET, (int)c._off);
        if (c._domain != null) {
          col.addProperty(RequestStatics.TYPE, "enum");
          JsonArray domain = new JsonArray();
          for (String s: c._domain)
            domain.add(new JsonPrimitive(s));
          col.add(RequestStatics.ENUM_DOMAIN,domain);
        } else {
          col.addProperty(RequestStatics.TYPE, (c._size > 0 && c._scale==1.0) ? "int" : "float");
          col.add(RequestStatics.ENUM_DOMAIN,new JsonArray());
        }
        col.addProperty(RequestStatics.SIZE, Math.abs(c._size));
        col.addProperty(RequestStatics.BASE,      c._base);
        col.addProperty(RequestStatics.SCALE, (int)c._scale);
        col.addProperty(RequestStatics.MIN, c._min);
        col.addProperty(RequestStatics.MAX, c._max);
        col.addProperty(RequestStatics.BADAT, ary._numrows - c._n);
        col.addProperty(RequestStatics.MEAN, c._mean);
        col.addProperty(RequestStatics.VARIANCE, c._sigma);
        formatRowData(col,ary,0,i);
        formatRowData(col,ary,1,i);
        formatRowData(col,ary,2,i);
        formatRowData(col,ary,-3,i);
        formatRowData(col,ary,-2,i);
        formatRowData(col,ary,-1,i);
        cols.add(col);
      }
      result.add(RequestStatics.COLS,cols);
      // otherwise display the column values
    } else {
      if (_offset.value() > ary._numrows)
        return Response.error("Value only has "+ary._numrows+" rows");
      JsonArray rows = new JsonArray();
      long endRow = Math.min(_offset.value() + _view.value(), ary._numrows);
      long startRow = Math.min(_offset.value(), ary._numrows - _view.value());
      for (long row = startRow; row < endRow; ++row) {
        JsonObject obj = new JsonObject();
        obj.addProperty(RequestStatics.ROW,row);
        for (int i = 0; i < ary._cols.length; ++i) {
          formatColData(obj,ary,row,i);
        }
        rows.add(obj);
      }
      result.add(RequestStatics.ROW_DATA, rows);
    }

    Response r = Response.done(result);
    r.setBuilder(VALUE_TYPE, new HideBuilder());
    r.setBuilder(RequestStatics.COLS, t);
    r.setBuilder(RequestStatics.ROW_DATA, t);
    return r;
  }

}
