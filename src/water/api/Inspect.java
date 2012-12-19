
package water.api;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import java.io.IOException;
import water.Value;
import water.ValueArray;

/**
 *
 * @author peta
 */
public class Inspect extends Request {


  public static final String JSON_VALUE_TYPE = "type";
  public static final String JSON_VALUE_ROWS = "rows";
  public static final String JSON_VALUE_COLS = "cols";
  public static final String JSON_VALUE_ROWSIZE = "rowsize";
  public static final String JSON_VALUE_COLUMNS = "columns";

  public static final String JSON_ROWS = "row_data";
  public static final String JSON_ROWS_ROW = "row";

  public static final String JSON_VALUE_COLUMN_NAME = "name";
  public static final String JSON_VALUE_COLUMN_OFFSET = "offset";
  public static final String JSON_VALUE_COLUMN_TYPE = "type";
  public static final String JSON_VALUE_COLUMN_ENUM_DOMAIN = "enum_domain";
  public static final String JSON_VALUE_COLUMN_SIZE = "size";
  public static final String JSON_VALUE_COLUMN_BASE = "base";
  public static final String JSON_VALUE_COLUMN_SCALE = "scale";
  public static final String JSON_VALUE_COLUMN_MIN = "min";
  public static final String JSON_VALUE_COLUMN_MAX = "max";
  public static final String JSON_VALUE_COLUMN_BADAT = "badat";
  public static final String JSON_VALUE_COLUMN_MEAN = "mean";
  public static final String JSON_VALUE_COLUMN_VAR = "var";


  protected final H2OExistingKey _key = new H2OExistingKey(JSON_KEY);
  protected final LongInt _offset = new LongInt(JSON_OFFSET,-1l,-1l,Long.MAX_VALUE);
  protected final Int _view = new Int(JSON_VIEW, 100, 0, 10000);


  protected void formatAryData(JsonObject obj, ValueArray ary, long rowIdx, int colIdx, String name) {
    if (rowIdx < 0)
      rowIdx = ary._numrows + rowIdx;
    ValueArray.Column c = ary._cols[colIdx];
    try {
      if (ary.isNA(rowIdx,colIdx)) {
        obj.addProperty(name,"NA");
      } else if (c._domain != null) {
        obj.addProperty(name,c._domain[(int)ary.data(rowIdx, colIdx)]);
      } else if ((c._size > 0) && (c._scale == 1))  {
        obj.addProperty(name,ary.data(rowIdx, colIdx));
      } else {
        obj.addProperty(name,ary.datad(rowIdx, colIdx));
      }
    } catch (IOException e) {
      obj.addProperty(name,"IOE");
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
    PaginatedTable t = null;
    Value val = _key.value();
    JsonObject result = new JsonObject();
    // If it is array, do the array result inspection
    if (val._isArray != 0) {
      ValueArray ary = ValueArray.value(val);
      t = new PaginatedTable(argumentsToJson(),_offset.value(), _view.value(), ary._numrows, true);
      result.addProperty(JSON_VALUE_TYPE, "ary");
      result.addProperty(JSON_VALUE_ROWS, ary._numrows);
      result.addProperty(JSON_VALUE_COLS, ary._cols.length);
      result.addProperty(JSON_VALUE_ROWSIZE,ary._rowsize);
      result.addProperty(JSON_VALUE_SIZE, ary.length());
      // if offset is -1 display the overview
      if (_offset.value() == -1) {
        JsonArray cols = new JsonArray();
        for (int i = 0; i < ary._cols.length; ++i ) {
          ValueArray.Column c = ary._cols[i];
          JsonObject col = new JsonObject();
          col.addProperty(JSON_VALUE_COLUMN_NAME, c._name);
          col.addProperty(JSON_VALUE_COLUMN_OFFSET, (int)c._off);
          if (c._domain != null) {
            col.addProperty(JSON_VALUE_COLUMN_TYPE, "enum");
            JsonArray domain = new JsonArray();
            for (String s: c._domain)
              domain.add(new JsonPrimitive(s));
            col.add(JSON_VALUE_COLUMN_ENUM_DOMAIN,domain);
          } else {
            col.addProperty(JSON_VALUE_COLUMN_TYPE, c._size > 0 ? "int" : "float");
            col.add(JSON_VALUE_COLUMN_ENUM_DOMAIN,new JsonArray());
          }
          col.addProperty(JSON_VALUE_COLUMN_SIZE, (int)c._size);
          col.addProperty(JSON_VALUE_COLUMN_BASE, (int)c._base);
          col.addProperty(JSON_VALUE_COLUMN_SCALE, (int)c._scale);
          col.addProperty(JSON_VALUE_COLUMN_MIN, c._min);
          col.addProperty(JSON_VALUE_COLUMN_MAX, c._max);
          col.addProperty(JSON_VALUE_COLUMN_BADAT, ary._numrows - c._n);
          col.addProperty(JSON_VALUE_COLUMN_MEAN, c._mean);
          col.addProperty(JSON_VALUE_COLUMN_VAR, c._sigma);
          formatRowData(col,ary,0,i);
          formatRowData(col,ary,1,i);
          formatRowData(col,ary,2,i);
          formatRowData(col,ary,-3,i);
          formatRowData(col,ary,-2,i);
          formatRowData(col,ary,-1,i);
          cols.add(col);
        }
        result.add(JSON_VALUE_COLUMNS,cols);
      // otherwise display the column values
      } else {
        if (_offset.value() >= ary._numrows)
          return Response.error("Value only has "+ary._numrows+" rows");
        JsonArray rows = new JsonArray();
        long endRow = _offset.value() + _view.value();
        if (endRow >= ary._numrows)
          endRow = ary._numrows - 1;
        for (long row = _offset.value(); row < endRow; ++row) {
          JsonObject obj = new JsonObject();
          obj.addProperty(JSON_ROWS_ROW,row);
          for (int i = 0; i < ary._cols.length; ++i) {
            formatColData(obj,ary,row,i);
          }
          rows.add(obj);
        }
        result.add(JSON_ROWS, rows);
      }
    // It is not an array, do whatever you want to
    } else {
      result.addProperty(JSON_VALUE_TYPE,"value");

    }
    Response r = Response.done(result);
    if (t != null) {
      r.setBuilder (JSON_VALUE_COLUMNS,t);
      r.setBuilder (JSON_ROWS,t);
    }
    return r;
  }

}
