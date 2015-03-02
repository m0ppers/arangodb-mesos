/*global window, $, Backbone, document, arangoCollectionModel,arangoHelper,
arangoDatabase, btoa, _*/

(function() {
  "use strict";

  window.MesosRouter = Backbone.Router.extend({

    routes: {
      ""                       : "dashboard",
      "dashboard"              : "dashboard",
      "overview"               : "cluster",
      "debug"                  : "debug"
    },

    initialize: function() {

      window.modalView = new window.ModalView();

      // GLOBAL NAVIGATION
      this.navigationView = new window.NavigationView();
      this.navigationView.render();

      // FOOTER
      if (this.footer) {
      }
      else {
        this.footerView = new window.FooterView();
      }

      this.footerView.render();
    },

    dashboard: function() {
      if (this.dashboardView) {
      }
      else {
        this.dashboardView = new window.DashboardView();
      }

      this.navigationView.selectMenuItem('dashboard-menu');
      this.dashboardView.render();
    },

    debug: function() {
      if (this.debugView) {
      }
      else {
        this.debugView = new window.DebugView();
      }

      this.navigationView.selectMenuItem('debug-menu');
      this.debugView.render();
    },

    cluster: function() {
      if (this.clusterView) {
      }
      else {
        this.clusterView = new window.ClusterView();
      }

      this.navigationView.selectMenuItem('overview-menu');
      this.clusterView.render();
    }

  });

}());

/*global window, $, Backbone, document */

(function() {
  "use strict";

  $(document).ready(function() {
    window.App = new window.MesosRouter();

    Backbone.history.start();

  });

}());
