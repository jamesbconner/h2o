package water.web;

import hex.*;
import hex.GLMSolver.Family;
import hex.GLMSolver.GLMModel;
import hex.GLMSolver.GLMParams;
import hex.GLMSolver.GLMValidation;
import hex.GLMSolver.Link;

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

  double [] getFamilyArgs(Family f, Properties p){
    double [] res = null;
    if(f == Family.binomial){
      res = new double []{1.0,1.0,0.5};
      try{res[GLMSolver.FAMILY_ARGS_CASE] = Double.valueOf(p.getProperty("case", "1.0"));}catch(NumberFormatException e){throw new GLMInputException("illegal case value" + p.getProperty("case", "1.0"));}
      if(p.containsKey("weight")){
        try{res[GLMSolver.FAMILY_ARGS_WEIGHT] = Double.valueOf(p.getProperty("weight", "1.0"));}catch(NumberFormatException e){throw new GLMInputException("illegal weight value " + p.getProperty("weight"));}
      }
      if(p.containsKey("threshold"))
        try{res[GLMSolver.FAMILY_ARGS_DECISION_THRESHOLD] = Double.valueOf(p.getProperty("threshold"));}catch(NumberFormatException e){throw new GLMInputException("illegal threshold value " + p.getProperty("threshold"));}
    }
    return res;
  }
  GLMParams getGLMParams(Properties p){
    GLMParams res = new GLMParams();
    try{res._f = GLMSolver.Family.valueOf(p.getProperty("family", "gaussian").toLowerCase()).ordinal();}catch(IllegalArgumentException e){throw new GLMInputException("unknown family " + p.getProperty("family", "gaussian"));}

    if(p.containsKey("link"))
     try{res._l = Link.valueOf(p.getProperty("link").toLowerCase()).ordinal();}catch(Exception e){throw new GLMInputException("invalid link argument " + p.getProperty("link"));}
    else
      res._l = Family.values()[res._f].defaultLink.ordinal();
    res._maxIter = getIntArg(p, "ITER", GLMSolver.DEFAULT_MAX_ITER);
    res._betaEps = getDoubleArg(p, "betaEps", GLMSolver.DEFAULT_BETA_EPS);
    res._familyArgs = getFamilyArgs(Family.values()[res._f], p);
    return res;
  }

  int getIntArg(Properties p, String name, int defaultValue){
    if(!p.containsKey(name))return defaultValue;
    try{return Integer.parseInt(p.getProperty(name));}catch (NumberFormatException e){throw new GLMInputException("invalid value of argument " + name);}
  }

  double getDoubleArg(Properties p, String name, double defaultValue){
    if(!p.containsKey(name))return defaultValue;
    try{return Double.parseDouble(p.getProperty(name));}catch (NumberFormatException e){throw new GLMInputException("invalid value of argument " + name);}
  }
  LSMSolver getLSMSolver(Properties p){
    if(!p.containsKey("norm"))
      return LSMSolver.makeSolver();
    String norm = p.getProperty("norm");
    if(norm.equalsIgnoreCase("L1")){
      double lambda = getDoubleArg(p, "lambda",LSMSolver.DEFAULT_LAMBDA);
      double rho = getDoubleArg(p, "rho",LSMSolver.DEFAULT_RHO);
      double alpha = getDoubleArg(p, "",LSMSolver.DEFAULT_ALPHA);
      return LSMSolver.makeL1Solver(lambda, rho, alpha);
    } else if(norm.equalsIgnoreCase("L2")){
      double lambda = getDoubleArg(p, "lambda",LSMSolver.DEFAULT_LAMBDA);
      return LSMSolver.makeL2Solver(lambda);
    } else if(norm.equalsIgnoreCase("ENET")){
      double lambda = getDoubleArg(p, "lambda",LSMSolver.DEFAULT_LAMBDA);
      double lambda2 = getDoubleArg(p, "lambda2",LSMSolver.DEFAULT_LAMBDA2);
      double rho = getDoubleArg(p, "rho",LSMSolver.DEFAULT_RHO);
      double alpha = getDoubleArg(p, "",LSMSolver.DEFAULT_ALPHA);
      return LSMSolver.makeElasticNetSolver(lambda, lambda2, rho, alpha);
    } else
      throw new GLMInputException("unknown norm " + norm);
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

      GLMParams glmParams = getGLMParams(p);
      LSMSolver lsm = getLSMSolver(p);
      GLMSolver glm = new GLMSolver(lsm, glmParams);
      long t1 = System.currentTimeMillis();
      GLMModel m = glm.computeGLM(ary, columns, null);
      if(m._warnings != null){
        JsonArray warnings = new JsonArray();
        for(String w:m._warnings)warnings.add(new JsonPrimitive(w));
        res.add("warnings", warnings);
      }
      m.validateOn(ary, null);
      res.add("GLMModel", m.toJson());
    } catch( GLMInputException e1 ) {
      res.addProperty("error", "Invalid input:" + e1.getMessage());
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
    bldr.append("<table class='table table-striped table-bordered table-condensed'><thead>");
    boolean firstRow = true;
    for(JsonElement e:arr){
      bldr.append("<tr>\n");
      String [] tags = new String[]{"<td>","</td>"};
      String [] htags = new String[]{"<th>","</th>"};
      boolean firstCol = true;
      for(JsonElement f:e.getAsJsonArray()){
        if(firstCol || firstRow)
          bldr.append(htags[0] + f.getAsString() + htags[1]);
        else
          bldr.append(tags[0] + f.getAsString() + tags[1]);
        firstCol = false;
      }
      bldr.append("</tr>\n");
      firstRow = false;
    }
    bldr.append("</tbody></table>\n");
  }

  @Override
  protected String serveImpl(Server server, Properties args, String sessionID) throws PageError {
    RString responseTemplate = new RString(
        "<div class='alert %succ'>GLM on data <a href='/Inspect?Key=%$key'>%key</a> finished in %iterations iterations. Computed in %time[ms]. %warningMsgs</div>"
            + "<h3>GLM Parameters</h3>"
            + "%parameters"
            + "<h3>Coefficients</h3>"
            + "<div>%coefficientHTML</div>"
            + "<h5>Model SRC</h5>"
            + "<div><code>%modelSrc</code></div>"
            + "<br/> "
            + "<h3> Training Set Validation</h3>"
            + "<table class='table table-striped table-bordered table-condensed'>"
            + "<tr><th>Degrees of freedom:</th><td>%DegreesOfFreedom total (i.e. Null);  %ResidualDegreesOfFreedom Residual</td></tr>"
            + "<tr><th>Null Deviance</th><td>%NullDeviance</td></tr>"
            + "<tr><th>Residual Deviance</th><td>%ResidualDeviance</td></tr>"
            + "<tr><th>AIC</th><td>%AIC</td></tr>"
            + "<tr><th>Training Error Rate Avg</th><td>%trainingSetErrorRate</td></tr>"
            + "</table>"
            + "<h4>Confusion Matrix</h4>"
            + "%cm " + " %xval");
    RString xTemplate = new RString(
          "<h3> %foldfold Validation</h3>"
        + "<h4>Average Confusion Matrix</h4>"
        + "%avgCM"
        + "<h4>Individual Models</h4>"
        + "%imodels");

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

    if(json.has("cm")){
      JsonArray arr = json.get("cm").getAsJsonArray();
      json.remove("cm");
      StringBuilder b = new StringBuilder();
      buildCM(arr, b);
      responseTemplate.replace("cm",b.toString());
    }
    if(json.has("validation"))responseTemplate.replace(json.get("validation").getAsJsonObject());
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
    JsonArray models = (JsonArray)json.get("models");
    if(models != null){
      bldr = new StringBuilder("<h3>Individual Models</h3>");
      for(JsonElement e:models)
        buildCM(e.getAsJsonArray(), bldr);
    }
    responseTemplate.replace("imodels", bldr.toString());
    return responseTemplate.toString();
  }
}
