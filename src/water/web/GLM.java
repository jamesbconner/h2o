package water.web;
import hex.GLinearRegression.GLRException;
import hex.rf.Confusion;
import hex.rf.DRF;
import hex.rf.Tree.StatType;

import java.util.Arrays;
import java.util.Properties;

import sun.security.x509.DeltaCRLIndicatorExtension;

import com.google.gson.JsonObject;

import water.*;
import water.web.Page.PageError;

public class GLM extends H2OPage {

  static String getColName(int colId, ValueArray ary){
    String colName = ary.col_name(colId);
    if(colName == null) colName = "Column " + colId;
    return colName;
  }

  static String getColName(int colId, String [] colNames){
    if(colId == colNames.length) return "Intercept";
    String colName = colNames[colId];
    if(colName == null)colName = "col["+colId+"]";
    return colName;
  }
  static class InvalidInputException extends RuntimeException{
    public InvalidInputException(String msg){
      super(msg);
    }
  }
  static class InvalidColumnIdException extends InvalidInputException{
    public InvalidColumnIdException(String exp){
      super("Invalid column identifier '" + exp + "'");
    }
  }
  public int [] parseVariableExpression(String [] colNames, String vexp){
    String [] colExps = vexp.split(",");
    int [] res = new int[colExps.length];
 __OUTER:
    for(int i = 0; i < res.length; ++i){
      String colExp = colExps[i].trim();
      for(int j = 0; j < colNames.length; ++j)
        if(colNames[j].equals(colExp)){
          res[i] = j;
          continue __OUTER;
        }
      try {res[i] = Integer.valueOf(colExps[i].trim());}catch(NumberFormatException e){throw new InvalidColumnIdException(colExps[i].trim());};
    }
    return res;
  }

  @Override public String[] requiredArguments() {
    return new String[] { "Key", "X", "Y"};
  }

  @Override
  public JsonObject serverJson(Server s, Properties p) throws PageError {
    RString responseTemplate = new RString("<div class='alert alert-success'>%name on data <a href=%keyHref>%key</a> computed in %time[ms]<strong>.</div><div class=\"container\">Result Coeficients:");
    ValueArray ary = ServletUtil.check_array(p,"Key");
    String [] colNames = ary.col_names();
    int [] yarr = parseVariableExpression(colNames, p.getProperty("Y"));
    if(yarr.length != 1)throw new InvalidInputException("Y has to refer to exactly one column!");
    int Y = yarr[0];
    if(0 > Y || Y >= ary.num_cols())throw new InvalidInputException("invalid Y value, column " + Y + " does not exist!");
    int [] X = parseVariableExpression(colNames, p.getProperty("X"));
    for(int x:X)if(0 > x || x >= ary.num_cols())throw new InvalidInputException("invalid X vector, column " + x + " does not exist!");
    String method = p.getProperty("family","gaussian");
    JsonObject res = new JsonObject();
    responseTemplate.replace("key", ary._key);
    res.addProperty("h2o",H2O.SELF.urlEncode());
    double [] coefs = null;
    long t1 = System.currentTimeMillis();
    try {
        responseTemplate.replace("name","Linear regression");
        coefs = hex.GLSM.web_main(ary._key, X, Y, method);
      long deltaT = System.currentTimeMillis()-t1;
      responseTemplate.replace("time", deltaT);
      res.addProperty("time", deltaT);
      StringBuilder bldr = new StringBuilder(responseTemplate.toString());
      for(int i = 0; i < coefs.length; ++i){
        String colName = (i == X.length)?"Intercept":getColName(X[i], colNames);
        bldr.append("<div>" + colName + " = " + coefs[i] + "</div>");
        res.addProperty(colName,coefs[i]);
      }
      bldr.append("</div>");
      res.addProperty("response_html", bldr.toString());
    } catch(DRF.IllegalDataException e) {
      res.addProperty("error", H2OPage.error("Incorrect input data: " + e.getMessage()));
    }
    return res;
  }

  @Override protected String serveImpl(Server server, Properties args) throws PageError {
    try{
      return serverJson(server, args).get("response_html").getAsString();
    } catch(InvalidInputException e1){
      return H2OPage.error("Invalid input:" + e1.getMessage());
    } catch(GLRException e2){
      return H2OPage.error("Unable to run the regression on this data: '" + e2.getMessage() + "'");
    }
  }
}
