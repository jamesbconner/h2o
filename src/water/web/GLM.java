package water.web;

import hex.GLSM;
import hex.GLSM.GLSMException;

import java.text.DecimalFormat;
import java.util.Map.Entry;
import java.util.*;

import water.H2O;
import water.ValueArray;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.sun.org.apache.xml.internal.resolver.readers.XCatalogReader;

public class GLM extends H2OPage {

  static String getColName(int colId, ValueArray ary) {
    String colName = ary.col_name(colId);
    if( colName == null ) colName = "Column " + colId;
    return colName;
  }

  static String getColName(int colId, String[] colNames) {
    if( colId == colNames.length ) return "Intercept";
    String colName = colNames[colId];
    if( colName == null ) colName = "col[" + colId + "]";
    return colName;
  }

  static class InvalidInputException extends RuntimeException {
    public InvalidInputException(String msg) {
      super(msg);
    }
  }

  static class InvalidColumnIdException extends InvalidInputException {
    public InvalidColumnIdException(String exp) {
      super("Invalid column identifier '" + exp + "'");
    }
  }

  public int[] parseVariableExpression(String[] colNames, String vexp) {
    if(vexp.trim().isEmpty())return new int[0];
    String[] colExps = vexp.split(",");
    int[] res = new int[colExps.length];
    int idx = 0;
    __OUTER: for( int i = 0; i < colExps.length; ++i ) {
      String colExp = colExps[i].trim();
      if( colExp.contains(":") ) {
        String[] parts = colExp.split(":");
        if( parts.length != 2 ) throw new InvalidColumnIdException(colExp);
        int from = parseVariableExpression(colNames, parts[0])[0];
        int to = parseVariableExpression(colNames, parts[1])[0];
        int[] new_res = new int[res.length + to - from];
        System.arraycopy(res, 0, new_res, 0, idx);
        for( int j = from; j <= to; ++j ) {
          new_res[idx++] = j;
        }
        res = new_res;
        continue __OUTER;
      }
      for( int j = 0; j < colNames.length; ++j )
        if( colNames[j].equalsIgnoreCase(colExp) ) {
          res[idx++] = j;
          continue __OUTER;
        }
      try {
        res[idx++] = Integer.valueOf(colExps[i].trim());
      } catch( NumberFormatException e ) {
        throw new InvalidColumnIdException(colExps[i].trim());
      }
      ;
    }
    return res;
  }

  @Override
  public String[] requiredArguments() {
    return new String[] { "Key", "Y" };
  }

  @Override
  public JsonObject serverJson(Server s, Properties p) throws PageError {
    JsonObject res = new JsonObject();
    try {
      ValueArray ary = ServletUtil.check_array(p, "Key");
      String[] colNames = ary.col_names();
      int[] yarr = parseVariableExpression(colNames, p.getProperty("Y"));
      if( yarr.length != 1 )
        throw new InvalidInputException("Y has to refer to exactly one column!");
      int Y = yarr[0];
      if( 0 > Y || Y >= ary.num_cols() )
        throw new InvalidInputException("invalid Y value, column " + Y
            + " does not exist!");
      int[] X = null;
      if( p.containsKey("X") ) X = parseVariableExpression(colNames,
          p.getProperty("X"));
      else {
        X = new int[ary.num_cols() - 1];
        int idx = 0;
        for( int i = 0; i < ary.num_cols(); ++i ) {
          if( i != Y ) X[idx++] = i;
        }
      }
      int[] xComp = (p.containsKey("-X")) ? parseVariableExpression(colNames,
          p.getProperty("-X")) : new int[0];
      Arrays.sort(xComp);
      int n = X.length;
      for( int i = 0; i < X.length; ++i ) {
        if( X[i] == Y ) {
          X[i] = -1;
          --n;
          continue;
        }
        for( int j = 0; j < xComp.length; ++j ) {
          if( xComp[j] == X[i] ) {
            --n;
            X[i] = -1;
          }
        }
      }
      if( n < 1 ) {
        res.addProperty("error", "Invalid input: no input columns specified");
        return res;
      }
      int[] columns = new int[n + 1];
      int idx = 0;
      for( int i = 0; i < X.length; ++i )
        if( X[i] != -1 ) columns[idx++] = X[i];
      columns[n] = Y;
      for( int x : columns )
        if( 0 > x || x >= ary.num_cols() ) {
          res.addProperty("error", "Invalid input: column " + x
              + " does not exist!");
          return res;
        }
      String method = p.getProperty("family", "gaussian");
      res.addProperty("key", ary._key.toString());
      res.addProperty("keyHref", "/Inspect?Key=" + H2OPage.encode(ary._key));
      res.addProperty("h2o", H2O.SELF.urlEncode());
      double[] coefs = null;
      long t1 = System.currentTimeMillis();
      if( method.equalsIgnoreCase("gaussian") ) res.addProperty("name",
          "Linear regression");
      else if( method.equalsIgnoreCase("binomial") )
        res.addProperty("name", "");
      GLSM g = new GLSM(ary._key, columns, 1, GLSM.Family.valueOf(method
          .toLowerCase()));
      coefs = g.solve();
      double[] validationCoef = g.test();
      long deltaT = System.currentTimeMillis() - t1;
      res.addProperty("time", deltaT);
      res.addProperty("DegreesOfFreedom", g.n() - 1);
      res.addProperty("ResidualDegreesOfFreedom", g.n() - X.length - 1);
      JsonObject coefficients = new JsonObject();

      for( int i = 0; i < coefs.length; ++i ) {
        String colName = (i == (coefs.length - 1)) ? "Intercept" : getColName(
            columns[i], colNames);
        coefficients.addProperty(colName, coefs[i]);
      }
      res.add("coefficients", coefficients);

      if( validationCoef != null ) {
        res.addProperty("Null Deviance", -2 * validationCoef[0]);
        res.addProperty("Residual Deviance", -2 * validationCoef[1]);
        int k = X.length + 1;
        res.addProperty("AIC", 2 * k - 2 * validationCoef[1]);
      }
    } catch( InvalidInputException e1 ) {
      res.addProperty("error", "Invalid input:" + e1.getMessage());
    } catch( GLSMException e2 ) {
      res.addProperty("error", "Unable to run the regression on this data: '"
          + e2.getMessage() + "'");
    }
    return res;
  }

  static DecimalFormat dformat = new DecimalFormat("###.####");

  @Override
  protected String serveImpl(Server server, Properties args) throws PageError {
    // RString responseTemplate = new RString(
    // "<div class='alert alert-success'>%name on data <a href=%keyHref>%key</a> computed in %time[ms]<strong>.</div>"
    // + "<div>Coefficients:%coefficientHTML</div>"
    // + "<p>"
    // +
    // "<div>Degrees of freedom: <span style=\"font-weight: normal\">%DegreesOfFreedom total (i.e. Null);  %ResidualDegreesOfFreedom Residual</span></div>"
    // +
    // "<div>Null Deviance: <span style=\"font-weight: normal\">%NullDeviance</span></div>"
    // +
    // "<div>Residual Deviance: <span style=\"font-weight: normal\">%ResidualDeviance</span></div>"
    // +
    // "<div>AIC: <span style=\"font-weight:normal;margin-left:5px\">%AIC_formated</span></div>");
    RString responseTemplate = new RString(
        "<div class='alert alert-success'>%name on data <a href=%keyHref>%key</a> computed in %time[ms]<strong>.</div>"
            + "<h3>Coefficients</h3>"
            + "<div>%coefficientHTML</div>"
            + "<h5>Model SRC</h5>"
            + "<div><code>%modelSrc</code></div>"
            + "<table class='table table-striped table-bordered table-condensed'>"
            + "<br/>"
            + "<h3>Validation</h3>"
            + "<tr><th>Degrees of freedom:</th><td>%DegreesOfFreedom total (i.e. Null);  %ResidualDegreesOfFreedom Residual</td></tr>"
            + "<tr><th>Null Deviance</th><td>%NullDeviance</td></tr>"
            + "<tr><th>Residual Deviance</th><td>%ResidualDeviance</td></tr>"
            + "<tr><th>AIC</th><td>%AIC_formated</td></tr>" + "</table>");

    JsonObject json = serverJson(server, args);
    if( json.has("error") )
      return H2OPage.error(json.get("error").getAsString());
    responseTemplate.replace(json);
    StringBuilder bldr = new StringBuilder();
    StringBuilder codeBldr = new StringBuilder();

    JsonObject x = (JsonObject) json.get("coefficients");
    if( x.entrySet().size() < 10 ) {
      for( Entry<String, JsonElement> e : x.entrySet() ) {
        double val = e.getValue().getAsDouble();
        bldr.append("<span style=\"margin:5px;font-weight:normal;\">"
            + e.getKey() + " = " + dformat.format(val) + "</span>");
        if( codeBldr.length() > 0 )
          codeBldr.append((val >= 0) ? " + " : " - ");
        if( e.getKey().equals("Intercept") ) codeBldr.append(dformat
            .format(Math.abs(val)));
        else codeBldr.append(dformat.format(Math.abs(val)) + "*x[" + e.getKey()
            + "]");
      }
      responseTemplate.replace("coefficientHTML", bldr.toString());
    } else {
      StringBuilder headerbldr = new StringBuilder();
      headerbldr
          .append("<table class='table table-striped table-bordered table-condensed'><thead><tr>");
      bldr.append("<tbody><tr>");
      for( Entry<String, JsonElement> e : x.entrySet() ) {
        double val = e.getValue().getAsDouble();
        headerbldr.append("<th>" + e.getKey() + "</th>");
        bldr.append("<td>" + dformat.format(val) + "</td>");
        if( codeBldr.length() > 0 )
          codeBldr.append((val >= 0) ? " + " : " - ");
        if( e.getKey().equals("Intercept") ) codeBldr.append(dformat
            .format(Math.abs(val)));
        else codeBldr.append(dformat.format(Math.abs(val)) + "*x[" + e.getKey()
            + "]");
      }
      headerbldr.append("</tr></thead>");
      bldr.append("</tr></tbody></table>");
      responseTemplate.replace("coefficientHTML",
          headerbldr.toString() + bldr.toString());

    }
    String method = args.getProperty("family", "gaussian");
    if( method.equalsIgnoreCase("gaussian") ) {
      RString m = new RString("y = %equation");
      m.replace("equation", codeBldr.toString());
      responseTemplate.replace("modelSrc", m.toString());
    } else if( method.equalsIgnoreCase("binomial") ) {
      RString m = new RString("y = 1/(1 + Math.exp(-(%equation))");
      m.replace("equation", codeBldr.toString());
      responseTemplate.replace("modelSrc", m.toString());
    }
    if( json.has("Null Deviance") )
      responseTemplate.replace("NullDeviance",
          dformat.format(json.get("Null Deviance").getAsDouble()));
    if( json.has("Residual Deviance") )
      responseTemplate.replace("ResidualDeviance",
          dformat.format(json.get("Residual Deviance").getAsDouble()));
    if( json.has("AIC") )
      responseTemplate.replace("AIC_formated",
          dformat.format(json.get("AIC").getAsDouble()));
    return responseTemplate.toString();
  }
}
