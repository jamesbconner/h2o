/**
 * 
 */
package water.web;

import java.util.Properties;

/**
 * Progress view showing node(s) output (stdout/stderr).
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
      + "  var xhr = new XMLHttpRequest();"
      + "  var url = '/PR'; xhr.open('GET', url, true); xhr.send();" 
      + "  var last_idx = -1;"
      + "  function parseStream() {"
      + "    var curr_idx = xhr.responseText.lastIndexOf('\\n');" 
      + "    if (last_idx == curr_idx) return;"
      + "    var s = xhr.responseText.substring(last_idx, curr_idx); last_idx = curr_idx;"
      + "    $('#output').append(s); "
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
