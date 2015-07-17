/*jshint browser: true */
/*jshint unused: false */
/*global require, exports, Backbone, EJS, $, flush, window, arangoHelper, nv, d3, localStorage*/
/*global document, console, Dygraph, _,templateEngine */

(function () {
  "use strict";

  window.DashboardView = Backbone.View.extend({
    el: '#content',

    events: {
      //"click #dashboard-content .t-row" : "drawServerModal",
      "click .advanced-menu" : "toggleAdvanced"
    },

    template: templateEngine.createTemplate("dashboardView.ejs"),

    render: function () {
      var self = this;
      $(this.el).html(this.template.render());
      this.drawServers();
      this.drawServers2();

      setInterval(function(){
        if (window.location.hash === '#dashboard' && $('#modal-dialog').is(':visible') === false) {
          self.drawServers();
          self.drawServers2();
        }
      }, 15000);
    },

    toggleAdvanced: function () {

      if ($('.advanced-content').hasClass('hidden')) {
        $('.advanced-content').removeClass('hidden');
        $('.advanced-menu .fa-plus-square').removeClass('active');
        $('.advanced-menu .fa-plus-square').addClass('fa-minus-square');
        $('.advanced-menu .fa-minus-square').removeClass('fa-plus-square');
      }
      else {
        $('.advanced-content').addClass('hidden');
        $('.advanced-menu .fa-minus-square').addClass('fa-plus-square');
        $('.advanced-menu .fa-plus-square').removeClass('fa-minus-square');
        $('.advanced-menu .fa-plus-square').addClass('active');
      }
    },

    drawServers: function () {
      var self = this;

      //ajax req for data before
      $.ajax({
        type : 'GET',
        dataType : 'json',
        async: true,
        url: '/debug/target.json'
      }).done(function(data) {console.log(data); 
        $('#dashboard-content .coordirnatorsValue').text(data.coordinators.instances);
        $('#dashboard-content .dbServersValue').text(data.dbservers.instances);
        /*
        if (data.clusters.length > 0) {
          $('.t-cluster-body').empty();
          _.each(data.clusters, function(val, key) {
            self.drawServerLine([
              val.name,
              val.planned.servers+' / '+val.running.servers,
              val.planned.cpus+' / '+val.running.cpus,
              filesize(val.planned.memory)+' / '+filesize(val.running.memory),
              filesize(val.planned.disk)+' / '+filesize(val.running.disk)
            ]);
          });
        }
      */
      });

    },

    drawServers2: function () {
      var self = this;

      //ajax req for data before
      $.ajax({
        type : 'GET',
        dataType : 'json',
        async: true,
        url: '/v1/state.json'
      }).done(function(data) {
        $('.t-cluster-body').empty();
        self.drawServerLine2([
          data.framework_name,
          data.mode,
          data.health
        ]);
      });
    },

    hideServerModal: function() {
      window.modalView.hide();
    },

    drawServerModal: function(ev, cluster) {

      var name = '';

      if (!cluster) {
        name = $(ev.currentTarget).first().children().first().text();
      }
      else {
        name = cluster;
      }

      var self = this,
      buttons = [],
      tableContent = [],
      advanced = {},
      advancedTableContent = [];

      $.ajax({
        type : 'GET',
        dataType : 'json',
        async: true,
        url: 'Videos.json'
        }).done(function(json) {

          tableContent = [
            {
              type: window.modalView.tables.READONLY,
              label: "Servers",
              id: "id_servers",
              value:
                '<span class="valuePlanned">' + json.planned.servers + '</span><span> / </span>' +
                '<span class="value">'+_.escape(json.running.servers)+'</span><i class="fa fa-plus"></i><i class="fa fa-minus"></i>',
            },
            {
              type: window.modalView.tables.READONLY,
              label: "Cpus",
              id: "id_cpus",
              value:
                '<span class="valuePlanned">' + json.planned.cpus +
                '</span><span> / </span><span class="value">' + _.escape(json.running.cpus) + '</span>'
            },
            {
              type: window.modalView.tables.READONLY,
              label: "Mem",
              id: "id_memory",
              value:
                '<span class="valuePlanned">' + filesize(_.escape(json.planned.memory)) +
                '</span><span> / </span><span class="value">' + filesize(_.escape(json.running.memory)) + '</span>'
            },
            {
              type: window.modalView.tables.READONLY,
              label: "Disk",
              id: "id_disk",
              value:
                '<span class="valuePlanned">' + filesize(_.escape(json.planned.disk)) +
                '</span><span> / </span><span class="value">' + filesize(_.escape(json.running.disk)) + '</span>'
            }
          ];

          advancedTableContent.push(
            window.modalView.createReadOnlyEntry(
              "id_agencies",
              "Agencies",
              '<span class="valuePlanned">' + json.planned.agencies + '</span><span> / </span>' +
              '<span class="value">' + json.running.agencies +
              '</span><i class="fa fa-plus"></i><i class="fa fa-minus"></i>'
            )
          );
          advancedTableContent.push(
            window.modalView.createReadOnlyEntry(
              "id_coordinators",
              "Coordinators",
              '<span class="valuePlanned">' + json.planned.coordinators + '</span><span> / </span>' +
              '<span class="value">' + json.running.coordinators +
              '</span><i class="fa fa-plus"></i><i class="fa fa-minus"></i>'
            )
          );
          advancedTableContent.push(
            window.modalView.createReadOnlyEntry(
              "id_dbservers",
              "DB Servers",
              '<span class="valuePlanned">' + json.planned.dbservers + '</span><span> / </span>' +
              '<span class="value">' + json.running.dbservers +
              '</span><i class="fa fa-plus"></i><i class="fa fa-minus"></i>'
            )
          );

          advanced.header = "Advanced";
          advanced.content = advancedTableContent;

          window.modalView.show(
            "modalTable.ejs", _.escape(json.name), buttons, tableContent, advanced
          );

          $(".fa-plus" ).bind( "click", function() {
            self.postCluster(this);
          });

          $(".fa-minus" ).bind( "click", function() {
            self.postCluster(this);
          });

          $(".modal-header .close" ).bind( "click", function() {
            self.render(this);
          });

          $(".modal-backdrop").bind( "click", function() {
            self.render(this);
          });

        }).fail(function(data) {
          console.log("something went wrong");
          console.log(data);
      });
    },

    rerenderValues: function(data) {

      _.each(data.planned, function(val, key) {

        if (key === 'memory' || key === 'disk') {
          $('#id_'+key+' .valuePlanned').text(filesize(_.escape(val)));
        }
        else {
          $('#id_'+key+' .valuePlanned').text(val);
        }
      });
      _.each(data.running, function(val, key) {
        if (key === 'memory' || key === 'disk') {
          $('#id_'+key+' .valuePlanned').text(filesize(_.escape(val)));
        }
        else {
          $('#id_'+key+' .value').text(val);
        }
      });

    },

    postCluster: function (e) {
      var attributeElement = $(e).parent().find('.value'),
      self = this,
      attributeValue = JSON.parse($(attributeElement).text()),
      parentid = $(e).parent().attr('id'),
      attributeName = parentid.substr(3, parentid.length),
      clusterName = $('.modal-header a').text();

      if ($(e).hasClass('fa-plus')) {

        var postMsg = {};
        postMsg[attributeName] = 1;

        $.ajax({
          type: "POST",
          url: "/v1/cluster/"+encodeURIComponent(clusterName),
          data: JSON.stringify(postMsg),
          contentType: "application/json",
          processData: false,
          success: function (data) {
            self.rerenderValues(data);
          },
          error: function () {
            console.log("post plus req error");
          }
        });

      }
      else if ($(e).hasClass('fa-minus')) {

        var postMsg = {};
        postMsg[attributeName] = -1;

        $.ajax({
          type: "POST",
          url: "/v1/cluster/"+encodeURIComponent(clusterName),
          data: JSON.stringify(postMsg),
          contentType: "application/json",
          processData: false,
          success: function (data) {
            self.rerenderValues(data);
          },
          error: function () {
            console.log("post minus req error");
          }
        });
      }

    },

    submitChanges: function () {
      newCluster.servers = JSON.parse($('#id_servers').text());
      newCluster.agencies = JSON.parse($('#id_agencies').text());
      newCluster.coordinators = JSON.parse($('#id_coordinators').text());
      newCluster.dbservers = JSON.parse($('#id_dbservers').text());
    },

    drawServerLine: function(parameters) {
      var htmlString = '<div class="t-row pure-g">';

      _.each(parameters, function(val) {
        htmlString += '<div class="pure-u-1-5"><p class="t-content">'+val+'</p></div>';
      });
      htmlString += '</div>';

      $('.t-cluster-body').append(htmlString);

    },

    drawServerLine2: function(parameters) {
      $('#dashboard-content .nameValue').text(parameters[0]);
      $('#dashboard-content .modeValue').text(parameters[1]);
      $('#dashboard-content .healthValue').text(parameters[2]);
      //var htmlString = '<div class="t-row pure-g">';

      //_.each(parameters, function(val) {
        //htmlString += '<div class="pure-u-1-3"><p class="t-content">'+val+'</p></div>';
      //});
      //htmlString += '</div>';


     },

  });
}());
