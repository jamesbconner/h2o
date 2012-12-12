package water.web;

import hex.*;
import hex.DGLM.BinomialArgs;
import hex.DGLM.Family;
import hex.DGLM.FamilyArgs;
import hex.DGLM.GLMBinomialValidation;
import hex.DGLM.GLMModel;
import hex.DGLM.GLMValidation;
import hex.DGLM.GLM_Params;
import hex.DGLM.GLSMException;
import hex.DGLM.Link;
import hex.DLSM.LSM_Params;
import hex.DLSM.Norm;
import hex.Models.ClassifierValidation;
import hex.Models.ModelValidation;

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

  FamilyArgs getFamilyArgs(Family f, Properties p){
    FamilyArgs res = null;
    if(f == Family.binomial){
      double caseVal = 1.0;
      double threshold = 0.5;
      double [] wt = new double[]{1.0,1.0};
      try{caseVal = Double.valueOf(p.getProperty("case", "1.0"));}catch(NumberFormatException e){throw new GLMInputException("illegal case value" + p.getProperty("case", "1.0"));}
      if(p.containsKey("weight")){
        try{wt[1] = Double.valueOf(p.getProperty("weight", "1.0"));}catch(NumberFormatException e){throw new GLMInputException("illegal weight value " + p.getProperty("weight"));}
      }
      if(p.containsKey("threshold"))
        try{threshold = Double.valueOf(p.getProperty("threshold"));}catch(NumberFormatException e){throw new GLMInputException("illegal threshold value " + p.getProperty("threshold"));}
      res = new BinomialArgs(threshold, caseVal,wt);
    }
    return res;
  }
  GLM_Params getGLMParams(Properties p){
    GLM_Params res = new GLM_Params();
    try{res._family = DGLM.Family.valueOf(p.getProperty("family", "gaussian").toLowerCase());}catch(IllegalArgumentException e){throw new GLMInputException("unknown family " + p.getProperty("family", "gaussian"));}

    if(p.containsKey("link"))
     try{res._link = Link.valueOf(p.getProperty("link").toLowerCase());}catch(Exception e){throw new GLMInputException("invalid link argument " + p.getProperty("link"));}
    else
      res._link = res._family.defaultLink;
    if(p.containsKey("ITER"))
      try{res._maxIter = Integer.valueOf(p.get("ITER").toString());}catch(Exception e){throw new GLMInputException("invalid ITER argument " + p.getProperty("ITER"));}
    if(p.containsKey("betaEps"))
      try{res._betaEps = Double.valueOf(p.get("ITER").toString());}catch(Exception e){throw new GLMInputException("invalid ITER argument " + p.getProperty("ITER"));}
    res._fargs = getFamilyArgs(res._family, p);
    return res;
  }

  LSM_Params getLSMParams(Properties p){
    LSM_Params res = new LSM_Params();
    if(p.containsKey("norm"))
      try{res.n = DLSM.Norm.valueOf(p.getProperty("norm"));}catch(IllegalArgumentException e){throw new GLMInputException("unknown norm " + p.getProperty("norm"));}
    if(res.n != Norm.NONE)
      if(p.containsKey("lambda"))
        try{ res.lambda = Double.valueOf(p.getProperty("lambda"));}catch(NumberFormatException e){throw new GLMInputException("invalid lambda argument " + p.getProperty("lambda"));}
      else
        res.lambda = 1e-5;
    if(res.n == Norm.L1 || res.n == Norm.ENET){
      if(p.containsKey("rho"))
        try{ res.rho = Double.valueOf(p.getProperty("rho"));}catch(NumberFormatException e){throw new GLMInputException("invalid rho argument " + p.getProperty("rho"));}
      else
        res.rho = 0.01;
      if(p.containsKey("alpha"))
        try{ res.alpha = Double.valueOf(p.getProperty("alpha"));}catch(NumberFormatException e){throw new GLMInputException("invalid alpha argument " + p.getProperty("alpha"));}
      else
        res.alpha = 1.0;
      if(res.n == Norm.ENET){
        if(p.containsKey("lambda2"))
          try{ res.lambda2 = Double.valueOf(p.getProperty("lambda2"));}catch(NumberFormatException e){throw new GLMInputException("invalid alpha argument " + p.getProperty("lambda2"));}
        else
          res.lambda2 = 1e-5;
      }
    }
    return res;
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
      // ignore empty X == make as if X not present
      if (p.containsKey("X") && ((p.getProperty("X") == null) || (p.getProperty("X").isEmpty())))
        p.remove("X");
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
      res.addProperty("key", ary._key.toString());
      res.addProperty("h2o", H2O.SELF.toString());

      GLM_Params glmParams = getGLMParams(p);
      LSM_Params lsmParams = getLSMParams(p);

      long t1 = System.currentTimeMillis();


      DGLM glm = new DGLM(glmParams,lsmParams);
      GLMModel m = glm.trainOn(ary, columns, null);
      if(m._warnings != null){
        JsonArray warnings = new JsonArray();
        for(String w:m._warnings)warnings.add(new JsonPrimitive(w));
        res.add("warnings", warnings);
      }
      res.addProperty("iterations", m._iterations);
      res.add("glmParams", glmParams.toJson());
      res.add("lsmParams", lsmParams.toJson());
      long deltaT = System.currentTimeMillis() - t1;
      res.addProperty("rows",ary.num_rows());
      res.addProperty("time", deltaT);
      res.add("coefficients", getCoefficients(columns, colNames, m.beta()));
      long t = System.currentTimeMillis();
      GLMValidation val = (GLMValidation)m.validateOn(ary._key, null);
      res.addProperty("validationTime", System.currentTimeMillis()-t);
      if(val != null){
        JsonObject trainingValidation = new JsonObject();
        trainingValidation.addProperty("DegreesOfFreedom", m.n() - 1);
        trainingValidation.addProperty("ResidualDegreesOfFreedom", m.n() - X.length - 1);
        trainingValidation.addProperty("NullDeviance", dformat.format(val.nullDeviance()));
        trainingValidation.addProperty("ResidualDeviance", dformat.format(val.resDeviance()));
        int k = X.length + 1;
        trainingValidation.addProperty("AIC", dformat.format(2 * k + val.resDeviance()));
        trainingValidation.addProperty("trainingSetErrorRate",dformat.format(val.err()));
        res.add("trainingSetValidation", trainingValidation);
        if(val instanceof GLMBinomialValidation){
          GLMBinomialValidation bv = (GLMBinomialValidation)val;
          JsonObject errDetails = new JsonObject();
          errDetails.addProperty("falsePositive", dformat.format(bv.fp()));
          errDetails.addProperty("falseNegative", dformat.format(bv.fn()));
          errDetails.addProperty("truePositive", dformat.format(bv.tp()));
          errDetails.addProperty("trueNegative", dformat.format(bv.tn()));
          JsonArray arr = new JsonArray();
          for(int j = 0; j < bv.classes(); ++j){
            JsonArray row = new JsonArray();
            for(int kk = 0; kk < bv.classes();++kk)
              row.add(new JsonPrimitive(bv.cm(j,kk)));
            arr.add(row);
          }
          errDetails.add("cm", arr);
          res.add("trainingErrorDetails", errDetails);
        }
      }
      // Cross Validation
      int xfactor;
      try{xfactor = Integer.valueOf(p.getProperty("xval","0"));}catch(NumberFormatException e){res.addProperty("error", "invalid cross factor value, expected integer, found " + p.getProperty("xval"));return res;};
      if(xfactor <= 1)return res;
      if(xfactor > m.n())xfactor = (int)m.n();
      res.addProperty("xfactor", xfactor);

      ModelValidation [] vals = Models.crossValidate(glm, xfactor, ary, columns, 20);
      if(vals[0] instanceof Models.BinaryClassifierValidation){
        Models.BinaryClassifierValidation v = (Models.BinaryClassifierValidation)vals[0];
        res.addProperty("trueNegative", dformat.format(v.tn()));
        res.addProperty("trueNegativeVar", dformat.format(v.tnVar()));
        res.addProperty("truePositive", dformat.format(v.tp()));
        res.addProperty("truePositiveVar", dformat.format(v.tpVar()));
        res.addProperty("falseNegative", dformat.format(v.fn()));
        res.addProperty("falseNegativeVar", dformat.format(v.fnVar()));
        res.addProperty("falsePositive", dformat.format(v.fp()));
      }
     // add individual models
      if(vals[0] instanceof Models.ClassifierValidation){
        JsonArray models = new JsonArray();
        for(int i = 1; i < vals.length; ++i) {
          JsonObject im = new JsonObject();
          JsonArray arr = new JsonArray();
          ClassifierValidation v = (ClassifierValidation)vals[i];
          for(int j = 0; j < v.classes(); ++j){
            JsonArray row = new JsonArray();
            for(int k = 0; k < v.classes();++k)
              row.add(new JsonPrimitive(v.cm(j,k)));
            arr.add(row);
          }
          im.add("cm", arr);
          models.add(im);
        }
        res.add("models", models);
        res.addProperty("errRate", dformat.format(val.err()));
      //res.addProperty("errRateVar", dformat.format(val.errVar()));
      }

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
    addGLMParamsHTML(glmParams, lsmParams, bldr);
    return bldr.toString();
  }

  static void addGLMParamsHTML(JsonObject glmParams, JsonObject lsmParams, StringBuilder bldr){
    bldr.append("<span><b>family: </b>" + glmParams.get("family").getAsString() + "</span>");
    bldr.append(" <span><b>link: </b>" + glmParams.get("link").getAsString() + "</span>");
    bldr.append(" <span><b>&epsilon;<sub>&beta;</sub>: </b>" + glmParams.get("betaEps").getAsString() + "</span>");
    bldr.append(" <span><b>threshold: </b>" + glmParams.get("threshold").getAsString() + "</span>");
    String [] params = new String[]{"norm","lambda","lambda2","rho","alpha","weights"};
    String [] paramHTML = new String[]{"norm","&lambda;<sub>1</sub>","&lambda;<sub>2</sub>","&rho;","&alpha;","weights"};
    for(int i = 0; i < params.length; ++i){
      if(!lsmParams.has(params[i]))continue;
      String s = lsmParams.get(params[i]).getAsString();
      if(s.equals("0.0"))continue;
      bldr.append(" <span><b>" + paramHTML[i] + ":</b>" + s + "</span> ");
    }
  }

  static DecimalFormat dformat = new DecimalFormat("###.####");


  static void buildCM(JsonArray arr,StringBuilder bldr){
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
        "<div class='alert %succ'>GLM on data <a href='/Inspect?Key=%$key'>%key</a> finished in %iterations iterations. Computed in %time[ms]. %warningMsgs</div>"
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
        wBldr.append("<div>" + w.getAsString() + "</div>");
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
    JsonObject glmParams = json.get("glmParams").getAsJsonObject();
    JsonObject lsmParams = json.get("lsmParams").getAsJsonObject();

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
        JsonObject e = (JsonObject)json.get("trainingErrorDetails");
        JsonArray arr = e.get("cm").getAsJsonArray();
        e.remove("cm");
        errDetailTemplate.replace(e);
        StringBuilder b = new StringBuilder();
        buildCM(arr, b);
        trainingSetValidationTemplate.replace("errorDetails",errDetailTemplate.toString() + "\n" + b.toString());
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
          //bldr.append("\n<h5>Coefficients:</h5><div>" + getCoefficientsStr(model.get("coefs").getAsJsonObject()) + "</div><h5>Confusion Matrix</h5>");
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
