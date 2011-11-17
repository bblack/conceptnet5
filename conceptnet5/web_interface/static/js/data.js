/**
 * Javascript for data pages.
 * Author: Justin Venezuela (jven@mit.edu)
 */

$(document).ready(function() {
  $('.vote_button').each(function(idx, elt_id) {
    var button = $(elt_id);
    button.click(function(event) {
      $.post(button.attr('data-assertion_url'), {
          'vote':button.attr('data-vote')
      }, function(response) {
          alert('ok!');
      });
    });
  });
});
