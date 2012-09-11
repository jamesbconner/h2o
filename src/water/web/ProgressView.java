/**
 * 
 */
package water.web;

import java.util.Properties;

/**
 * Progress view showing node(s) output (stdout/stderr).
 * 
 * <p>It uses {@link ProgressReport} REST API to show current stdout/stderr.</p>
 * 
 * <p>NOTE: The page uses Javascript to get stream of data, however, it is not optimal solution, since 
 * whole response is buffered. See note below.</p>
 * 
 * @author michal
 *
 */
public class ProgressView extends H2OPage {
  
  // NOTE: the streaming javascript is not optional since it cache all data (default behavior of JS XMLHttpRequest).
  // It would be more beneficial to use socket.io or a similar concept based only on 
  // exchanging events.
  static final String html =
      "<script type='text/javascript'>"
      + "$(document).ready(function() {"
      + "  var xhr = null;"
      + "  if (window.XMLHttpRequest)      { xhr = new XMLHttpRequest(); }"
      + "  else if (window.ActiveXObject)  { xhr = new ActiveXObject('Microsoft.XMLDOM'); }"
      + "  var url = '/PR?Type=html'; xhr.open('GET', url, true); xhr.send();" 
      + "  var last_idx = -1;"
      + "  function parseStream() {"
      + "    if (xhr.responseText == null) return;"
      + "    var curr_idx = xhr.responseText.lastIndexOf('\\n');" 
      + "    if (last_idx == curr_idx) return;"
      + "    var s = xhr.responseText.substring(last_idx, curr_idx); last_idx = curr_idx;"
      + "    var ns = s.replace(/\\[out\\]/g, '<i class=\"icon-thumbs-up\" />&nbsp;').replace(/\\[err\\]/g, '<i class=\"icon-fire\" />&nbsp;');"      
      + "    $('#output').append(ns); "
      + "    $('#bottom')[0].scrollIntoView();"
      + "  }"
      + "  var interval = setInterval(parseStream, 1000);"
      + "  xhr.addEventListener('abort', function() { clearInterval(interval); });"
      + "  "
      + "});"
    + "</script>"
    + "<p class='lead'>Node(s) output <i class=\"icon-search\"></i></p>"    
    + "<pre id='output'></pre>" // use &nbsp; to have a bigger box
    + "<a id='bottom' href='#' class='btn btn-danger pull-right' onclick=\"$('#output').html('')\">Clear</a>";

  @Override
  protected String serve_impl(Properties args) {
    return html;
  }
}
