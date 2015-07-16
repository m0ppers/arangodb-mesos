/*global window, $, Backbone, document */

(function() {
  "use strict";

  $(document).ready(function() {
    window.App = new window.MesosRouter();

    Backbone.history.start();

  });

}());
