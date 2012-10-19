package water.web;

import hex.*;
import hex.DGLM.BinomialArgs;
import hex.DGLM.BinomialValidation;
import hex.DGLM.Family;
import hex.DGLM.FamilyArgs;
import hex.DGLM.GLM_Model;
import hex.DGLM.GLM_Params;
import hex.DGLM.GLM_Validation;
import hex.DGLM.GLSMException;
import hex.DGLM.Link;
import hex.DLSM.LSM_Params;

import java.text.DecimalFormat;
import java.util.*;
import java.util.Map.Entry;

import water.H2O;
import water.ValueArray;

import com.google.gson.*;

public class GLM extends H2OPage {

  static class GLMInputException  extends  RuntimeException {
    public GLMInputException(String msg) {
      super(msg);
    }
  }
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
        throw new GLMInputException("Y has to refer to exactly one column!");
      int Y = yarr[0];
      if( 0 > Y || Y >= ary.num_cols() )
        throw new GLMInputException("invalid Y value, column " + Y
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
      res.addProperty("key", ary._key.toString());
      res.addProperty("keyHref", "/Inspect?Key=" + H2OPage.encode(ary._key));
      res.addProperty("h2o", H2O.SELF.urlEncode());
      long t1 = System.currentTimeMillis();
      if( method.equals("gaussian") ) res.addProperty("name","Linear regression");
      else if( method.equals("binomial") )
        res.addProperty("name", "Logistic regression");
      DGLM.Family f;
      try{f = DGLM.Family.valueOf(method.toLowerCase());}catch(IllegalArgumentException e){throw new GLMInputException("unknown family " + method);}
      DGLM.Norm norm;
      try{norm = DGLM.Norm.valueOf(p.getProperty("norm", "NONE"));}catch(IllegalArgumentException e){throw new GLMInputException("unknown norm " + p.getProperty("norm","NONE"));}
      double lambda = 0.0;
      double rho = 0;
      try{ rho = Double.valueOf(p.getProperty("rho", "0.01"));}catch(NumberFormatException e){throw new GLMInputException("invalid lambda argument " + p.getProperty("rho", "0.01"));}
      double alpha = 1.0;
      try{ alpha = Double.valueOf(p.getProperty("alpha", "1"));}catch(NumberFormatException e){throw new GLMInputException("invalid lambda argument " + p.getProperty("alpha", "1"));}
      if(norm != DGLM.Norm.NONE)try{ lambda = Double.valueOf(p.getProperty("lambda", "0.1"));}catch(NumberFormatException e){throw new GLMInputException("invalid lambda argument " + p.getProperty("lambda", "0.1"));}
      Link l = f.defaultLink;
      if(p.containsKey("link"))
        try{l = Link.valueOf(p.getProperty("link").toLowerCase());}catch(Exception e){throw new GLMInputException("invalid link argument " + p.getProperty("link"));}
      if(p.containsKey("link")) {
        try{l = DGLM.Link.valueOf(p.get("link").toString().toLowerCase());}catch(Exception e){throw new GLMInputException("invalid lambda argument " + p.getProperty("alpha", "1"));}
      }
      GLM_Params glmParams = new GLM_Params(f, l);
      FamilyArgs fargs = null;
      if(f == Family.binomial){
        //TODO
        fargs = new BinomialArgs(threshold, 1.0);
      }
      LSM_Params lsmParams = new LSM_Params(norm,lambda,rho,alpha,1);
      JsonObject jGlmParams = new JsonObject();

      jGlmParams.addProperty("link", glmParams.link.toString());
      jGlmParams.addProperty("family", glmParams.family.toString());
      res.add("glmParams", jGlmParams);
      JsonObject jLsmParams = new JsonObject();
      jLsmParams.addProperty("norm", lsmParams.n.toString());
      jLsmParams.addProperty("lambda", lsmParams.lambda);
      jLsmParams.addProperty("rho", lsmParams.rho);
      jLsmParams.addProperty("alpha", lsmParams.alpha);
      res.add("lsmParams", jLsmParams);


      GLM_Model m = DGLM.solve(ary, columns, null, glmParams,lsmParams,fargs);
      if(m.warnings != null){
        JsonArray warnings = new JsonArray();
        for(String w:m.warnings)warnings.add(new JsonPrimitive(w));
        res.add("warnings", warnings);
      }
      long deltaT = System.currentTimeMillis() - t1;
      res.addProperty("rows",ary.num_rows());
      res.addProperty("time", deltaT);
      res.add("coefficients", getCoefficients(columns, colNames, m.beta));

      GLM_Validation val = m.validateOn(ary, null);
      if(val != null){
        JsonObject trainingValidation = new JsonObject();
        trainingValidation.addProperty("DegreesOfFreedom", m.n - 1);
        trainingValidation.addProperty("ResidualDegreesOfFreedom", m.n - X.length - 1);
        trainingValidation.addProperty("NullDeviance", dformat.format(val.nullDeviance()));
        trainingValidation.addProperty("ResidualDeviance", dformat.format(val.residualDeviance()));
        int k = X.length + 1;
        trainingValidation.addProperty("AIC", dformat.format(2 * k + val.residualDeviance()));
        trainingValidation.addProperty("trainingSetErrorRate",dformat.format(val.errMean()));
        res.add("trainingSetValidation", trainingValidation);
        if(val instanceof BinomialValidation){
          BinomialValidation bv = (BinomialValidation)val;
          JsonObject errDetails = new JsonObject();
          errDetails.addProperty("falsePositive", dformat.format(bv.fpMean()));
          errDetails.addProperty("falseNegative", dformat.format(bv.fnMean()));
          errDetails.addProperty("truePositive", dformat.format(bv.tpMean()));
          errDetails.addProperty("trueNegative", dformat.format(bv.tnMean()));
          res.add("trainingErrorDetails", errDetails);
        }

      }
      // Cross Validation
      int xfactor;
      try{xfactor = Integer.valueOf(p.getProperty("xval","0"));}catch(NumberFormatException e){res.addProperty("error", "invalid cross factor value, expected integer, found " + p.getProperty("xval"));return res;};
      if(xfactor == 0)return res;
      if(xfactor > m.n)xfactor = (int)m.n;
      if(xfactor == 1)throw new GLMInputException("Invalid value of xfactor. Has to be either 0 (no crossvalidation) or > 1.");
      res.addProperty("xfactor", xfactor);
      res.addProperty("threshold", threshold);
      //ValueArray ary, int[] colIds, int xfactor, GLM_Params glmParams, LSM_Params lsmParams, FamilyArgs fargs)
      ArrayList<GLM_Validation> vals = DGLM.xValidate(ary,columns,xfactor, glmParams, lsmParams, fargs);
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
      }
      res.addProperty("errRate", dformat.format(val.errMean()));
      res.addProperty("errRateVar", dformat.format(val.errVar()));

    } catch( GLMInputException e1 ) {
      res.addProperty("error", "Invalid input:" + e1.getMessage());
    } catch( GLSMException e2 ) {
      res.addProperty("error", "Unable to run the regression on this data: '"
          + e2.getMessage() + "'");
    }
    return res;
  }

  static String getFormulaSrc(JsonObject x, boolean neg) {
    StringBuilder codeBldr = new StringBuilder();
    for( Entry<String, JsonElement> e : x.entrySet() ) {
      double val = e.getValue().getAsDouble();
      if(val == 0)continue;
      if(neg) val *= -1;
      if( codeBldr.length() > 0 ) {
        if(val >= 0)codeBldr.append(" + " + dformat.format(val));
        else codeBldr.append(" - " + dformat.format(-val));
      } else
        codeBldr.append(dformat.format(val));
      if( !e.getKey().equals("Intercept") )
        codeBldr.append("*x[" + e.getKey()+ "]");
    }
    return codeBldr.toString();
  }

  static String getCoefficientsStr(JsonObject x){
    StringBuilder bldr = new StringBuilder();

    if( x.entrySet().size() < 10 ) {
      for( Entry<String, JsonElement> e : x.entrySet() ) {
        double val = e.getValue().getAsDouble();
        bldr.append("<span style=\"margin:5px;font-weight:normal;\">"
            + e.getKey() + " = " + dformat.format(val) + "</span>");
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
      }
      headerbldr.append("</tr></thead>");
      bldr.append("</tr></tbody></table>");
      return headerbldr.toString() + bldr.toString();
    }
  }

  static String getGLMParams(JsonObject glmParams, JsonObject lsmParams){
    StringBuilder bldr = new StringBuilder();
    bldr.append("<span><b>family: </b>" + glmParams.get("family").getAsString() + "</span>");
    bldr.append(" <span><b>link: </b>" + glmParams.get("link").getAsString() + "</span> ");
    String norm = lsmParams.get("norm").getAsString();
    bldr.append(" <span><b>norm: </b>" + norm + "</span> ");
    if(norm.equals("L1")){
      bldr.append(" <span><b>&lambda;: </b>" + lsmParams.get("lambda").getAsString() + "</span> ");
      bldr.append(" <span><b>&rho;: </b>" + lsmParams.get("rho").getAsString() + "</span> ");
      bldr.append(" <span><b>&alpha;: </b>" + lsmParams.get("alpha").getAsString() + "</span> ");
    } else if(norm.equals("L2")){
      bldr.append(" <span><b>&lambda;: </b>" + lsmParams.get("lambda").getAsString() + "</span> ");
    }
    return bldr.toString();
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
        "<div class='alert %succ'>GLM on data <a href=%keyHref>%key</a> computed in %time[ms]. %warningMsgs</div>"
            + "<h3>GLM Parameters</h3>"
            + "%parameters"
            + "<h3>Coefficients</h3>"
            + "<div>%coefficientHTML</div>"
            + "<h5>Model SRC</h5>"
            + "<div><code>%modelSrc</code></div>"
            + "<br/> %tValid %confusion_matrix %xvalidation");

    RString trainingSetValidationTemplate = new RString(
             "<h3>Validation</h3>"
            + "<table class='table table-striped table-bordered table-condensed'>"
            + "<tr><th>Degrees of freedom:</th><td>%DegreesOfFreedom total (i.e. Null);  %ResidualDegreesOfFreedom Residual</td></tr>"
            + "<tr><th>Null Deviance</th><td>%NullDeviance</td></tr>"
            + "<tr><th>Residual Deviance</th><td>%ResidualDeviance</td></tr>"
            + "<tr><th>AIC</th><td>%AIC</td></tr>"
            + "<tr><th>Training Error Rate Avg</th><td>%trainingSetErrorRate</td></tr>"
            + "%errorDetails"
            + "</table>");

    RString errDetailTemplate = new RString(
          "<tr><th>False Positives</th><td>%falsePositive</td></tr>"
        + "<tr><th>False Negative</th><td>%falseNegative</td></tr>");


    JsonObject json = serverJson(server, args, sessionID);
    if( json.has("error") )
      return H2OPage.error(json.get("error").getAsString());
    if(json.has("warnings")){
      responseTemplate.replace("succ","alert-warning");
      JsonArray warnings = (JsonArray)json.get("warnings");
      StringBuilder wBldr = new StringBuilder("<div><b>Warnings:</b>");
      for(JsonElement w:warnings){
        wBldr.append(w.getAsString());
      }
      wBldr.append("</div>");
      responseTemplate.replace("warningMsgs",wBldr.toString());
      json.remove("warnings");
    } else {
      responseTemplate.replace("succ","alert-success");
    }
    JsonArray models = (JsonArray)json.get("models");
    json.remove("models");
    responseTemplate.replace(json);
    StringBuilder bldr = new StringBuilder();

    JsonObject x = json.get("coefficients").getAsJsonObject();
    responseTemplate.replace("coefficientHTML",getCoefficientsStr(x));
    JsonObject glmParams = json.getAsJsonObject("glmParams").getAsJsonObject();
    JsonObject lsmParams = json.getAsJsonObject("lsmParams").getAsJsonObject();

    responseTemplate.replace("parameters",getGLMParams(glmParams, lsmParams));
    RString m = null;
    if(glmParams.get("link").getAsString().equals("identity")){
      m = new RString("y = %equation");
      m.replace("equation", getFormulaSrc(x, false));
    } else if( glmParams.get("link").getAsString().equals("logit") ) {
      m = new RString("y = 1/(1 + Math.exp(%equation))");
      m.replace("equation", getFormulaSrc(x, true));
    } else if( glmParams.get("link").getAsString().equals("log") ) {
      m = new RString("y = Math.exp(%equation)");
      m.replace("equation", getFormulaSrc(x, false));
    } else if( glmParams.get("link").getAsString().equals("inverse") ) {
      m = new RString("y = 1/(%equation)");
      m.replace("equation", getFormulaSrc(x, false));
    }
    responseTemplate.replace("modelSrc", m.toString());
    if(json.has("trainingSetValidation")){
      trainingSetValidationTemplate.replace((JsonObject)json.get("trainingSetValidation"));
      if(json.has("trainingErrorDetails")){
        errDetailTemplate.replace((JsonObject)json.get("trainingErrorDetails"));
        trainingSetValidationTemplate.replace("errorDetails",errDetailTemplate.toString());
      }
      responseTemplate.replace("tValid",trainingSetValidationTemplate.toString());
    }

    if(json.has("xfactor")){
      RString xValidationTemplate = new RString(
           "<h3>%xfactor fold Cross Validation</h3>"
          + "%xvalidation_parameters"
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
      if(glmParams.get("family").getAsString().equals("binomial")){
        xValidationTemplate.replace("xvalidation_parameters","<div>decision threshold = %threshold</div>");
      }
      if(models != null){
        bldr =new StringBuilder("<h3>Individual Models</h3>");
        int modelIdx = 1;
        for(JsonElement o:models){
          bldr.append("<h4>Model " + modelIdx++ + "</h4>");
          JsonObject model = (JsonObject)o;
          bldr.append("\n<h5>Coefficients:</h5><div>" + getCoefficientsStr(model.get("coefs").getAsJsonObject()) + "</div><h5>Confusion Matrix</h5>");
          JsonArray arr = model.get("cm").getAsJsonArray();
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
