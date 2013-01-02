/**
 *
 */
package water.api;


/**
 * Summary page referencing all tutorials.
 *
 * @author michal
 *
 */
public class Tutorials extends HTMLOnlyRequest {

  protected String build(Response response) {
    return "<div class='container'><div class='hero-unit'>"
    + "<h1>H<sub>2</sub>O Tutorials</h1>"
    + "<blockquote><small>A unique way to explore H<sub>2</sub>O</small></blockquote>"
    + "<div class='row'>"
    + "<div class='span3'>"
    + "<h2 style='margin: 20px 0 0 0'>Random Forest</h2>"
    + "<p style='height:150px'>Random forest is a classical machine learning algorithm serving for data classification. Learn how to use it with H<sub>2</sub>O.</it></p>"
    + "<p><a href='/TutorialRFIris.html' class='btn btn-primary btn-large'>Try it!</a></p>"
    + "</div>"
    + "<div class='span3'>"
    + "<h2 style='margin: 20px 0 0 0'>GLM</h2>"
    + "<p style='height:150px'>Generalized linear model is a generalization of linear regresion. Experience its unique power on the top of H<sub>2</sub>O.</p>"
    + "<p><a href='/TutorialGLMProstate.html' class='btn btn-primary btn-large'>Try it!</a></p>"
    + "</div>"
    + "<div class='span3'>"
    + "<h2 style='margin: 20px 0 0 0'>GLMNet</h2>"
    + "<p style='height:150px'>Coming soon!</p>"
    + "<p><a class='btn btn-primary btn-large disabled'>Try it!</a></p>"
    + "</div>"
    + "</div>"
    + "</div>"
    + "</div>";
  }
}
