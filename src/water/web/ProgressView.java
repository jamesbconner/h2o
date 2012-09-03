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
  
  static final String html =
      "<script type='text/javascript'>"
      + "$(document).ready(function() {"
      + "  var xhr = new XMLHttpRequest();"
      + "  var url = '/PR'; xhr.open('GET', url, true); xhr.send();" 
      + "  var last_idx = 0;"
      + "  function parseStream() {"
      + "    var curr_idx = xhr.responseText.length;"
      + "    if (last_idx == curr_idx) return;"
      + "    var s = xhr.responseText.substring(last_idx, curr_idx); last_idx = curr_idx;"
      + "    $('#output').append(s);"
      + "  }"
      + "  var interval = setInterval(parseStream, 1000);"
      + "  xhr.addEventListener('abort', function() { clearInterval(interval); });"
      + "  "
      + "});"
    + "</script>"
    + "<p class='lead'>Node(s) output</p>"
    + "<a href='#' class='btn btn-danger pull-right' onclick=\"$('#output').html('&nbsp;')\">Clear</a>"
    + "<pre id='output'>&nbsp;</pre>"; // use &nbsp; to have a nicer box 

  @Override
  protected String serve_impl(Properties args) {
    return html;
  }
}
