/*jshint browser: true */
/*jshint unused: false */
/*global require, exports, Backbone, EJS, $, flush, window, arangoHelper, nv, d3, localStorage*/
/*global document, console, Dygraph, _,templateEngine */

(function () {
  "use strict";

  window.FooterView = Backbone.View.extend({
    el: '#footerBar',
    template: templateEngine.createTemplate("footerView.ejs"),

    render: function () {
      $(this.el).html(this.template.render());
    }
  });
}());
