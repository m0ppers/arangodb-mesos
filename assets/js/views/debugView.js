/*jshint browser: true */
/*jshint unused: false */
/*global require, exports, Backbone, EJS, $, flush, window, arangoHelper, nv, d3, localStorage*/
/*global document, console, Dygraph, _,templateEngine */

(function () {
  "use strict";

  window.DebugView = Backbone.View.extend({
    el: '#content',

    events: {
      "click .debug-offers"    : "renderOfferTable",
      "click .debug-instances" : "renderInstancesTable",
      "click .fa-refresh"      : "refresh",
      "click .sorting"         : "sorting",
      "change .blue-select"    : "rerenderCurrent",
      "click .debug-target"    : "renderJSON",
      "click .debug-plan"      : "renderJSON",
      "click .debug-current"   : "renderJSON"
    },

    sortKey: "",
    sortAsc: true,

    template: templateEngine.createTemplate("debugView.ejs"),
    template2: templateEngine.createTemplate("debugView2.ejs"),

    render: function () {
      //this.renderOfferTable();
      this.renderJSON(undefined);
    },

    renderJSON: function(e) {

      $('.tab-div').removeClass('active');

      $(this.el).html(this.template.render());

      var url = "";

      if (e) {
        url = $(e.currentTarget).attr('data-url');
      }
      else {
        url = "target";
      }

      $('.debug-'+url).addClass('active');

      $.ajax({
        type : 'GET',
        dataType : 'json',
        async: true,
        url: '/debug/' + url + '.json'
      }).done(function(data) {
        $('.json-content').text(JSON.stringify(data, null, 2));
      });

      //TODO: disable not used stuff for the moment
      $('.select-box').css('visibility', 'hidden');
      $('.fa-refresh').css('visibility', 'hidden');

    },

    refresh: function() {
      $('.fa-refresh').addClass('fa-spin');

      this.rerenderCurrent();

      setTimeout(function() {
        $('.fa-refresh').removeClass('fa-spin');
      }, 1000);
    },

    rerenderCurrent: function() {
      if ($('.debug-instances').hasClass('active')) {
        this.drawInstanceTable();
      }
      else {
        this.drawOfferTable();
      }
    },

    renderOfferTable: function() {
      this.sortKey = "";
      this.sortAsc = true;

      $(this.el).html(this.template.render());
      this.renderSelectBox("offer");
    },

    renderInstancesTable: function() {
      this.sortKey = "";
      this.sortAsc = true;

      $(this.el).html(this.template2.render());
      this.renderSelectBox("instance");
    },

    renderSelectBox: function (view) {

      var self = this;

      $.ajax({
        type : 'GET',
        dataType : 'json',
        async: true,
        url: '/v1/cluster'
      }).done(function(data) {
        var i = 0;

        _.each(data.clusters, function(val, key) {
          if (i === 0) {
            $('.cluster-select').append('<option selected>'+val.name+'</option>');
          }
          else {
            $('.cluster-select').append('<option>'+val.name+'</option>');
          }
        });
        if (view === 'offer') {
          self.drawOfferTable();
        }
        else {
          self.drawInstanceTable();
        }
      });
    },

    drawInstanceTable: function () {

      var self = this;
      var selected = $('.cluster-select option:selected').text();

      $.ajax({
        type : 'GET',
        dataType : 'json',
        async: true,
        url: '/v1/instances/'+encodeURIComponent(selected)
      }).done(function(data) {
        $('.t-body').empty();

        var sorted;
        if (self.sortKey !== '') {
          sorted = self.sortByKey(data.instances, self.sortKey, self.sortAsc);
        }
        else {
          sorted = self.sortByKey(data.instances, 'aspect', true);
        }

        _.each(sorted, function(val, key) {
          self.drawServerLineInstances(
            val.aspect, val.slaveId, val.hostname, val.status, val.resources.cpus,
            filesize(val.resources.memory), filesize(val.resources.disk), val.started, val.lastUpdate, val.link
          );
        });
      }).fail(function(data) {
      });

    },

    drawServerLineInstances: function(aspect, slaveId, hostname, status, cpus, memory, disk, started, lastUpdate, link) {
      var htmlString = '<div class="t-row pure-g">';
      if (link) {
        htmlString += '<div class="pure-u-2-24"><p class="t-content"><a href="'+link+'" target="_blank">'+aspect+'</a></p></div>';
      }
      else {
        htmlString += '<div class="pure-u-2-24"><p class="t-content">'+aspect+'</p></div>';
      }
      htmlString += '<div class="pure-u-6-24"><p class="t-content">'+slaveId+'</p></div>';
      htmlString += '<div class="pure-u-2-24"><p class="t-content">'+hostname+'</p></div>';
      htmlString += '<div class="pure-u-2-24"><p class="t-content">'+status+'</p></div>';
      htmlString += '<div class="pure-u-2-24"><p class="t-content">'+cpus+'</p></div>';
      htmlString += '<div class="pure-u-2-24"><p class="t-content">'+memory+'</p></div>';
      htmlString += '<div class="pure-u-2-24"><p class="t-content">'+disk+'</p></div>';
      htmlString += '<div class="pure-u-3-24"><p class="t-content">'+started+'</p></div>';
      htmlString += '<div class="pure-u-3-24"><p class="t-content">'+lastUpdate+'</p></div>';
      htmlString += '</div>';

      $('.t-cluster-body').append(htmlString);
    },

    sorting: function(e) {

      var classes = $(e.currentTarget).attr('class');
      var elements = $('.t-head').children();

      _.each(elements, function(val, key) {
        var found = $(val).find('.sorting');
        $(found).parent().removeClass("active");
        $(found).removeClass("fa-sort-asc");
        $(found).removeClass("fa-sort-desc");
        $(found).addClass("fa-sort");
      });

      $(e.currentTarget).removeClass('fa-sort');
      $(e.currentTarget).addClass(classes);
      $(e.currentTarget).parent().addClass('active');

      this.sortKey = $(e.currentTarget).data('sortkey');

      if ($(e.currentTarget).hasClass('fa-sort')) {
        $(e.currentTarget).removeClass('fa-sort');
        $(e.currentTarget).addClass('fa-sort-asc');
        $(e.currentTarget).parent().addClass('active');
        this.sortAsc = true;
      }
      else if ($(e.currentTarget).hasClass('fa-sort-asc')) {
        $(e.currentTarget).removeClass('fa-sort-asc');
        $(e.currentTarget).addClass('fa-sort-desc');
        $(e.currentTarget).parent().addClass('active');
        this.sortAsc = false;
      }
      else if ($(e.currentTarget).hasClass('fa-sort-desc')) {
        $(e.currentTarget).removeClass('fa-sort-desc');
        $(e.currentTarget).addClass('fa-sort');
        $(e.currentTarget).parent().removeClass('active');
        this.sortAsc = true;
        this.sortKey = '';
      }

      this.rerenderCurrent();
    },

    sortByKey: function (array, key, asc) {
      return array.sort(function(a, b) {
        var x = a[key]; var y = b[key];

        if (asc) {
          return ((x < y) ? -1 : ((x > y) ? 1 : 0));
        }
        else {
          return ((x > y) ? -1 : ((x < y) ? 1 : 0));
        }
      });
    },

    drawOfferTable: function () {
      var self = this;
      var selected = $('.cluster-select option:selected').text();

      $.ajax({
        type : 'GET',
        dataType : 'json',
        async: true,
        url: '/v1/offers/'+encodeURIComponent(selected)
      }).done(function(data) {
        $('.t-body').empty();

        var sorted;
        if (self.sortKey !== '') {
          sorted = self.sortByKey(data.offers, self.sortKey, self.sortAsc);
        }
        else {
          sorted = self.sortByKey(data.offers, 'offerId', true);
        }

        _.each(sorted, function(val, key) {
          self.drawServerLineOffers(
            val.offerId, val.slaveId, val.status,
            val.resources.cpus, filesize(val.resources.memory), filesize(val.resources.disk)
          );
        });
      }).fail(function(data) {
      });

    },

    drawServerLineOffers: function(offerId, slaveId, status, cpus, memory, disk) {
      var htmlString = '<div class="t-row pure-g">';
      htmlString += '<div class="pure-u-6-24"><p class="t-content">'+offerId+'</p></div>';
      htmlString += '<div class="pure-u-6-24"><p class="t-content">'+slaveId+'</p></div>';
      htmlString += '<div class="pure-u-2-24"><p class="t-content">'+status.agency+'</p></div>';
      htmlString += '<div class="pure-u-2-24"><p class="t-content">'+status.coordinator+'</p></div>';
      htmlString += '<div class="pure-u-2-24"><p class="t-content">'+status.dbserver+'</p></div>';
      htmlString += '<div class="pure-u-2-24"><p class="t-content">'+cpus+'</p></div>';
      htmlString += '<div class="pure-u-2-24"><p class="t-content">'+memory+'</p></div>';
      htmlString += '<div class="pure-u-2-24"><p class="t-content">'+disk+'</p></div>';
      htmlString += '</div>';
      $('.t-cluster-body').append(htmlString);
    }

  });
}());
