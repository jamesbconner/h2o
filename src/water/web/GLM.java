package water.web;

import hex.*;
import hex.GLSM.DataPreprocessing;
import hex.GLSM.*;

import java.text.DecimalFormat;
import java.util.*;
import java.util.Map.Entry;

import water.H2O;
import water.ValueArray;

import com.google.gson.*;

public class GLM extends H2OPage {

  static String getColName(int colId, String[] colNames) {
    return colId == colNames.length ? "Intercept" : colName(colId,colNames);
  }

  @Override
  public String[] requiredArguments() {
    return new String[] { "Key", "Y" };
  }

  static JsonObject getCoefficients(int [] columnIds, String [] colNames, double [] beta){
    JsonObject coefficients = new JsonObject();
    for( int i = 0; i < beta.length; ++i ) {
      String colName = (i == (beta.length - 1)) ? "Intercept" : getColName(columnIds[i], colNames);
      coefficients.addProperty(colName, beta[i]);
    }
    return coefficients;
  }

  @Override
  public JsonObject serverJson(Server s, Properties p, String sessionID) throws PageError {
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
          res.addProperty("error", "Invalid input: column " + x + " does not exist!");
          return res;
        }
      double threshold;
      try{threshold = Double.valueOf(p.getProperty("threshold", "0.5"));}catch(NumberFormatException e){res.addProperty("error", "invalid threshold value, expected double, found " + p.getProperty("xval"));return res;};

      String method = p.getProperty("family", "gaussian").toLowerCase();
      boolean nonZerosAsOnes = Boolean.valueOf(p.getProperty("bool","false"));
      res.addProperty("key", ary._key.toString());
      res.addProperty("keyHref", "/Inspect?Key=" + H2OPage.encode(ary._key));
      res.addProperty("h2o", H2O.SELF.urlEncode());
      long t1 = System.currentTimeMillis();
      if( method.equals("gaussian") ) res.addProperty("name","Linear regression");
      else if( method.equals("binomial") )
        res.addProperty("name", "Logistic regression");
      GLSM.Family f;
      try{f = GLSM.Family.valueOf(method.toLowerCase());}catch(IllegalArgumentException e){throw new InvalidInputException("unknown family " + method);}
      GLSM.Norm norm;
      try{norm = GLSM.Norm.valueOf(p.getProperty("norm", "NONE"));}catch(IllegalArgumentException e){throw new InvalidInputException("unknown norm " + p.getProperty("norm","NONE"));}
      double lambda = 0.0;
      double rho = 0;
      try{ rho = Double.valueOf(p.getProperty("rho", "0.01"));}catch(NumberFormatException e){throw new InvalidInputException("invalid lambda argument " + p.getProperty("rho", "0.01"));}
      double alpha = 1.0;
      try{ alpha = Double.valueOf(p.getProperty("alpha", "1"));}catch(NumberFormatException e){throw new InvalidInputException("invalid lambda argument " + p.getProperty("alpha", "1"));}
      if(norm != GLSM.Norm.NONE)try{ lambda = Double.valueOf(p.getProperty("lambda", "0.1"));}catch(NumberFormatException e){throw new InvalidInputException("invalid lambda argument " + p.getProperty("lambda", "0.1"));}
      GLM_Model m = GLSM.solve(ary, columns, null, 1, f, norm, new double[]{lambda,rho,alpha},GLSM.DataPreprocessing.AUTO,nonZerosAsOnes);
      long deltaT = System.currentTimeMillis() - t1;
      res.addProperty("rows",ary.num_rows());
      res.addProperty("time", deltaT);
      res.addProperty("DegreesOfFreedom", m.n - 1);
      res.addProperty("ResidualDegreesOfFreedom", m.n - X.length - 1);
      res.add("coefficients", getCoefficients(columns, colNames, m.beta));
      GLM_Validation val = m.validateOn(ary, null, columns,new double[]{threshold});
      if( val.nullDeviance() != 0) {
        res.addProperty("Null Deviance", val.nullDeviance());
        res.addProperty("Residual Deviance", val.residualDeviance());
        int k = X.length + 1;
        res.addProperty("AIC", 2 * k + val.residualDeviance());
      }
      res.addProperty("trainingSetErrorRate",val.errMean());

      // Cross Validation
      int xfactor;
      try{xfactor = Integer.valueOf(p.getProperty("xval","0"));}catch(NumberFormatException e){res.addProperty("error", "invalid cross factor value, expected integer, found " + p.getProperty("xval"));return res;};
      if(xfactor == 0)return res;
      if(xfactor > m.n)xfactor = (int)m.n;
      if(xfactor == 1)throw new InvalidInputException("Invalid value of xfactor. Has to be either 0 (no crossvalidation) or > 1.");
      res.addProperty("xfactor", xfactor);
      res.addProperty("threshold", threshold);
      ArrayList<GLM_Validation> vals = GLSM.xValidate(ary,f,columns,xfactor, threshold, 1, norm, new double[]{lambda,rho,alpha},nonZerosAsOnes);
      val = vals.get(0);
      if(val instanceof BinomialValidation){
        BinomialValidation v = (BinomialValidation)val;
        res.addProperty("trueNegative", dformat.format(v.tnMean()));
        res.addProperty("trueNegativeVar", dformat.format(v.tnVar()));
        res.addProperty("truePositive", dformat.format(v.tpMean()));
        res.addProperty("truePositiveVar", dformat.format(v.tpVar()));
        res.addProperty("falseNegative", dformat.format(v.fnMean()));
        res.addProperty("falseNegativeVar", dformat.format(v.fnVar()));
        res.addProperty("falsePositive", dformat.format(v.fpMean()));
        res.addProperty("falsePositiveVar", dformat.format(v.fpVar()));
        // add individual models
        JsonArray models = new JsonArray();
        for(int i = 1; i < vals.size(); ++i) {
          JsonObject im = new JsonObject();
          JsonArray arr = new JsonArray();
          v = (BinomialValidation)vals.get(i);
          for(int j = 0; j < 2;++j){
            JsonArray row = new JsonArray();
            for(int k = 0; k < 2;++k)
              row.add(new JsonPrimitive(v.cm(j,k)));
            arr.add(row);
          }
          im.add("cm", arr);
          im.add("coefs", getCoefficients(columns, colNames, v.m().beta));
          models.add(im);
        }
        res.add("models", models);
      } else {
        res.addProperty("trueNegative", "n/a");
        res.addProperty("trueNegativeVar", "n/a");
        res.addProperty("truePositive", "n/a");
        res.addProperty("truePositiveVar", "n/a");
        res.addProperty("falseNegative", "n/a");
        res.addProperty("falseNegativeVar", "n/a");
        res.addProperty("falsePositive", "n/a");
        res.addProperty("falsePositiveVar", "n/a");
      }
      res.addProperty("errRate", dformat.format(val.errMean()));
      res.addProperty("errRateVar", dformat.format(val.errVar()));

    } catch( InvalidInputException e1 ) {
      res.addProperty("error", "Invalid input:" + e1.getMessage());
    } catch( Exception e2 ) {
      res.addProperty("error", "Unable to run the regression on this data: '"
          + e2.getMessage() + "'");
    }
    return res;
  }


  static String getCoefficientsStr(JsonObject x, StringBuilder codeBldr){
    StringBuilder bldr = new StringBuilder();
    if( x.entrySet().size() < 10 ) {
      for( Entry<String, JsonElement> e : x.entrySet() ) {
        double val = e.getValue().getAsDouble();
        bldr.append("<span style=\"margin:5px;font-weight:normal;\">"
            + e.getKey() + " = " + dformat.format(val) + "</span>");
        if( codeBldr.length() > 0 )
          codeBldr.append((val >= 0) ? " - " : " + ");
        if( e.getKey().equals("Intercept") ) codeBldr.append(dformat
            .format(Math.abs(val)));
        else codeBldr.append(dformat.format(Math.abs(val)) + "*x[" + e.getKey()
            + "]");
      }
      return bldr.toString();
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
      return headerbldr.toString() + bldr.toString();
    }
  }

  static DecimalFormat dformat = new DecimalFormat("###.####");

  @Override
  protected String serveImpl(Server server, Properties args, String sessionID) throws PageError {
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
        "<div class='alert alert-success'>%name on data <a href=%keyHref>%key</a> computed on <b>%rows rows in %time ms.</b></div>"
            + "<h3>Coefficients</h3>"
            + "<div>%coefficientHTML</div>"
            + "<h5>Model SRC</h5>"
            + "<div><code>%modelSrc</code></div>"
            + "<br/>"
            + "<h3>Validation</h3>"
            + "<table class='table table-striped table-bordered table-condensed'>"
            + "<tr><th>Degrees of freedom:</th><td>%DegreesOfFreedom total (i.e. Null);  %ResidualDegreesOfFreedom Residual</td></tr>"
            + "<tr><th>Null Deviance</th><td>%NullDeviance</td></tr>"
            + "<tr><th>Residual Deviance</th><td>%ResidualDeviance</td></tr>"
            + "<tr><th>AIC</th><td>%AIC_formated</td></tr>"
            + "<tr><th>Training Error Rate</th><td>%trainingSetErrorRate</td></tr>"
            + "</table> %xvalidation");

    JsonObject json = serverJson(server, args, sessionID);
    if( json.has("error") )
      return H2OPage.error(json.get("error").getAsString());
    JsonArray models = (JsonArray)json.get("models");
    json.remove("models");
    responseTemplate.replace(json);
    StringBuilder bldr = new StringBuilder();
    StringBuilder codeBldr = new StringBuilder();
    JsonObject x = (JsonObject) json.get("coefficients");
    responseTemplate.replace("coefficientHTML",getCoefficientsStr(x, codeBldr));
    String method = args.getProperty("family", "gaussian");
    if( method.equalsIgnoreCase("gaussian") ) {
      RString m = new RString("y = %equation");
      m.replace("equation", codeBldr.toString());
      responseTemplate.replace("modelSrc", m.toString());
    } else if( method.equals("binomial") ) {
      RString m = new RString("y = 1/(1 + Math.exp(%equation))");
      m.replace("equation", codeBldr.toString());
      responseTemplate.replace("modelSrc", m.toString());
    }
    if( json.has("Null Deviance") )
      responseTemplate.replace("NullDeviance",
          dformat.format(json.get("Null Deviance").getAsDouble()));
    else
      responseTemplate.replace("NullDeviance","n/a");
    if( json.has("Residual Deviance") )
      responseTemplate.replace("ResidualDeviance",
          dformat.format(json.get("Residual Deviance").getAsDouble()));
    else
      responseTemplate.replace("ResidualDeviance","n/a");
    if( json.has("AIC") )
      responseTemplate.replace("AIC_formated",
          dformat.format(json.get("AIC").getAsDouble()));
    else
      responseTemplate.replace("AIC_formated","n/a");
    if(json.has("xfactor")){
      RString xValidationTemplate = new RString(
           "<h3>%xfactor fold Cross Validation</h3>"
          +"<div>decision threshold = %threshold</div>"
          +"<table class='table table-striped table-bordered table-condensed'>"
          +"<thead><tr><th></th><th>Mean</th><th>Variance</th></tr></thead>"
          +"<tbody>"
          +"<tr><th>Error rate</th><td>%errRate</td><td>%errRateVar</td></tr>"
          +"<tr><th>True Positive</th><td>%trueNegative</td><td>%trueNegativeVar</td></tr>"
          +"<tr><th>True Negative</th><td>%truePositive</td><td>%truePositiveVar</td></tr>"
          +"<tr><th>False Negative</th><td>%falseNegative</td><td>%falseNegativeVar</td></tr>"
          +"<tr><th>False Positive</th><td>%falsePositive</td><td>%falsePositiveVar</td></tr>"
          +"</tbody>"
          +"</table>");
      xValidationTemplate.replace(json);
      if(models != null){
        bldr =new StringBuilder("<h3>Individual Models</h3>");
        int modelIdx = 1;
        for(JsonElement o:models){
          bldr.append("<h4>Model " + modelIdx++ + "</h4>");
          JsonObject m = (JsonObject)o;
          bldr.append("\n<h5>Coefficients:</h5><div>" + getCoefficientsStr(m.get("coefs").getAsJsonObject(), codeBldr) + "</div><h5>Confusion Matrix</h5>");
          JsonArray arr = m.get("cm").getAsJsonArray();
          bldr.append("<table class='table table-striped table-bordered table-condensed'><thead><tr><th></th><th>Y<sub>real</sub>=0</th><th>Y<sub>real</sub>=1</th></tr></thead><tbody>\n");
          int rowidx = 0;
          for(JsonElement e:arr){
            bldr.append("<tr><th>Y<sub>model</sub>=" + rowidx++ + "</th>");
            JsonArray a = e.getAsJsonArray();
            for(JsonElement elem:a){
                 bldr.append("<td>" + elem.getAsString() + "</td>");
            }
            bldr.append("</tr>\n");
          }
          bldr.append("</tbody></table>\n");
        }
      }
      responseTemplate.replace("xvalidation",xValidationTemplate.toString() + bldr.toString());
    }
    return responseTemplate.toString();
  }
}
