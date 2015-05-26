/*jshint browser: true */
/*jshint unused: false */
/*global require, exports, Backbone, EJS, $, flush, window, arangoHelper, nv, d3, localStorage*/
/*global document, console, Dygraph, _,templateEngine */

(function () {
  "use strict";

  window.ClusterView = Backbone.View.extend({
    el: '#content',

    template: templateEngine.createTemplate("clusterView.ejs"),

    events: {
      "change .blue-select" : "rerenderOnChange",
      "click .fa-refresh" : "rerender",
      "mouseover .pie" : "detailInfo",
      "mouseleave .pie" : "clearDetailInfo"
    },

    layoutData: [],
    layoutNames: [],

    detailInfo: function (e) {
      $('.pie').addClass('shadow');
      $(e.currentTarget).removeClass('shadow');
      var id = (e.currentTarget.id).substr(3, e.currentTarget.id.length),
      current = this.layoutData[id];

      if (id === '') {
        return;
      }

      $('.cpu-label').text(this.round1Dec(current[0].percent) + ' %');
      $('.ram-label').text(this.round1Dec(current[1].percent) + ' %');
      $('.disk-label').text(this.round1Dec(current[2].percent) + ' %');
      $('.name-label').text($(e.currentTarget).find('.pie-name').text());
    },

    clearDetailInfo: function () {
      $('.pie').removeClass('shadow');
      $('.cpu-label').empty()
      $('.ram-label').empty();
      $('.disk-label').empty();
      $('.name-label').empty();
    },

    round1Dec: function(value) {
      return Math.round( value * 10 ) / 10;
    },

    render: function () {
      $(this.el).html(this.template.render());

      //TODO ajax not available
      //this.renderSelectBox();

      var selected = $('.cluster-select option:selected').text();
      this.getClusterServers(selected, true);
    },

    rerenderOnChange: function () {
      var selected = $('.cluster-select option:selected').text();
      this.getClusterServers(selected, true);
      $('.cluster-left').empty();
    },

    rerender: function() {
      $('.fa-refresh').addClass('fa-spin');
      var selected = $('.cluster-select option:selected').text();
      this.getClusterServers(selected, false);
      $('.cluster-left').empty();

      setTimeout(function() {
        $('.fa-refresh').removeClass('fa-spin');
      }, 1000);
    },

    getClusterServers: function(name, animation) {

      var self = this;

      $.ajax({
        type : 'GET',
        dataType : 'json',
        async: true,
        url: '/v1/servers/' + encodeURIComponent(name)
      }).done(function(data) {
          self.layoutData = [];
          self.layoutNames= [];
          _.each(data.servers, function(val, key) {
          self.calculateLayout(val);
        });
        self.renderCluster(animation);
      });
    },

    calculateLayout: function(node) {
      var pCPU, pMEM, pDISK;

      pCPU = this.pToValue(node.used.cpus, node.available.cpus);
      pMEM = this.pToValue(node.used.memory, node.available.memory);
      pDISK = this.pToValue(node.used.disk, node.available.disk);

      this.layoutNames.push({name: node.name});

      this.layoutData.push([
        {
          label: "CPU",
          value: pCPU,
          percent: this.pToPercent(pCPU)
        },
        {
          label: "MEM",
          value: pMEM,
          percent: this.pToPercent(pMEM)
        },
        {
          label: "DISK",
          value: pDISK,
          percent: this.pToPercent(pDISK)
        },
        {
          label: "None",
          value: 100 - pCPU - pMEM - pDISK
        }
      ]);

    },

    pToValue: function(used, available) {

      var division = used/available,
      calc = 0;

      calc = 33*division;
      return calc;
    },

    pToPercent: function(value) {

      var calc = value*3;

      if (calc === 99) {
        calc = 100;
      }
      else if (calc === 49.5) {
        calc = 50;
      }
      return calc;
    },

    renderSelectBox: function () {
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
      });
    },

    renderCluster: function (animation) {
      var self = this,
      delay = 100;

      _.each(this.layoutData, function(val, key) {
        jQuery('<div/>', {
          id: 'pie'+key,
          style: 'display:none',
          class: "pie"
        }).appendTo('.cluster-left');
        if (animation) {
          self.renderPie(key, val, delay);
          delay = delay + 100;
        }
        else {
          self.renderPie(key, val, 0);
        }
      });
    },

    renderPie: function(id, data, delay) {
      var name = this.layoutNames[id].name,
      isEmpty = false;

      if (data[0].value === 0 && data[1].value === 0 && data[2].value === 0) {
        isEmpty = true;
      }

      $('#pie'+id).epoch({
        type: 'pie',
        data: data,
        inner: 30
      });
      if (isEmpty === true) {
        $('#pie'+id).addClass('empty-pie');
      }

      if (delay === 0) {
        $('#pie'+id).append('<p class="pie-name">'+name+'</p>');
        $('#pie'+id).show();
      }
      else {
        $('#pie'+id).append('<p class="pie-name">'+name+'</p>');
        $('#pie'+id).delay(delay).fadeIn('slow');
      }
    }

  });
}());

/*jshint browser: true */
/*jshint unused: false */
/*global require, exports, Backbone, EJS, $, flush, window, arangoHelper, nv, d3, localStorage*/
/*global document, console, Dygraph, _,templateEngine */

(function () {
  "use strict";

  window.DashboardView = Backbone.View.extend({
    el: '#content',

    events: {
      "click #dashboard-content .t-row" : "drawServerModal"
    },

    template: templateEngine.createTemplate("dashboardView.ejs"),

    render: function () {

      //render dashboard

      var self = this;
      $(this.el).html(this.template.render());
      //TODO v1/cluster not available
      //this.drawServers();

      setInterval(function(){
        if (window.location.hash === '#dashboard' && $('#modal-dialog').is(':visible') === false) {
          //TODO v1/cluster not available
          //self.drawServers();
        }
      }, 15000);

      this.drawServers2();

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

    drawServers: function () {
      var self = this;

      //ajax req for data before
      $.ajax({
        type : 'GET',
        dataType : 'json',
        async: true,
        url: '/v1/cluster'
      }).done(function(data) {
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
        url: '/v1/cluster/' + name
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

    drawServerLine2: function(parameters) {
      var htmlString = '<div class="t-row pure-g">';

      _.each(parameters, function(val) {
        htmlString += '<div class="pure-u-1-3"><p class="t-content">'+val+'</p></div>';
      });
      htmlString += '</div>';

      $('.t-cluster-body').append(htmlString);

    },

    drawServerLine: function(parameters) {
      var htmlString = '<div class="t-row pure-g">';

      _.each(parameters, function(val) {
        htmlString += '<div class="pure-u-1-5"><p class="t-content">'+val+'</p></div>';
      });
      htmlString += '</div>';

      $('.t-cluster-body').append(htmlString);

    }

  });
}());

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
      "click .debug-target"    : "renderJSON",
      "click .debug-plan"      : "renderJSON",
      "click .debug-current"   : "renderJSON",
      "click .debug-instances" : "renderInstancesTable",
      "click .fa-refresh"      : "refresh",
      "click .sorting"         : "sorting",
      "change .blue-select"    : "rerenderCurrent"
    },

    sortKey: "",
    sortAsc: true,

    template: templateEngine.createTemplate("debugView.ejs"),
    template2: templateEngine.createTemplate("debugView2.ejs"),

    render: function () {
      this.renderJSON(undefined);
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

    renderJSON: function(e) {

      $('.tab-div').removeClass('active');

      $(this.el).html(this.template.render());

      var url = "";

      if (e) {
        url = $(e.currentTarget).attr('data-url');
        console.log(url);
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

/*jshint browser: true */
/*jshint unused: false */
/*global Backbone, $, window, setTimeout, Joi, _ */
/*global templateEngine*/

(function () {
  "use strict";

  var createButtonStub = function(type, title, cb) {
    return {
      type: type,
      title: title,
      callback: cb
    };
  };

  var createTextStub = function(type, label, value, info, placeholder, mandatory, joiObj,
                                addDelete, addAdd, maxEntrySize, tags) {
    var obj = {
      type: type,
      label: label
    };
    if (value !== undefined) {
      obj.value = value;
    }
    if (info !== undefined) {
      obj.info = info;
    }
    if (placeholder !== undefined) {
      obj.placeholder = placeholder;
    }
    if (mandatory !== undefined) {
      obj.mandatory = mandatory;
    }
    if (addDelete !== undefined) {
      obj.addDelete = addDelete;
    }
    if (addAdd !== undefined) {
      obj.addAdd = addAdd;
    }
    if (maxEntrySize !== undefined) {
      obj.maxEntrySize = maxEntrySize;
    }
    if (tags !== undefined) {
      obj.tags = tags;
    }
    if (joiObj){
      // returns true if the string contains the match
      obj.validateInput = function() {
        // return regexp.test(el.val());
        return joiObj;
      };
    }
    return obj;
  };

  window.ModalView = Backbone.View.extend({

    _validators: [],
    _validateWatchers: [],
    baseTemplate: templateEngine.createTemplate("modalBase.ejs"),
    tableTemplate: templateEngine.createTemplate("modalTable.ejs"),
    el: "#modalPlaceholder",
    contentEl: "#modalContent",
    hideFooter: false,
    confirm: {
      list: "#modal-delete-confirmation",
      yes: "#modal-confirm-delete",
      no: "#modal-abort-delete"
    },
    enabledHotkey: false,
    enableHotKeys : true,

    buttons: {
      SUCCESS: "success",
      NOTIFICATION: "notification",
      DELETE: "danger",
      NEUTRAL: "neutral",
      CLOSE: "close"
    },
    tables: {
      READONLY: "readonly",
      TEXT: "text",
      PASSWORD: "password",
      SELECT: "select",
      SELECT2: "select2",
      CHECKBOX: "checkbox"
    },
    closeButton: {
      type: "close",
      title: "Cancel"
    },

    initialize: function() {
      Object.freeze(this.buttons);
      Object.freeze(this.tables);
      var self = this;
      this.closeButton.callback = function() {
        self.hide();
      };
    },

    createModalHotkeys: function() {
      //submit modal
      $(this.el).bind('keydown', 'return', function(){
        $('.modal-footer .button-success').click();
      });
      $("input", $(this.el)).bind('keydown', 'return', function(){
        $('.modal-footer .button-success').click();
      });
      $("select", $(this.el)).bind('keydown', 'return', function(){
        $('.modal-footer .button-success').click();
      });
    },

    createInitModalHotkeys: function() {
      var self = this;
      //navigate through modal buttons
      //left cursor
      $(this.el).bind('keydown', 'left', function(){
        self.navigateThroughButtons('left');
      });
      //right cursor
      $(this.el).bind('keydown', 'right', function(){
        self.navigateThroughButtons('right');
      });

    },

    navigateThroughButtons: function(direction) {
      var hasFocus = $('.modal-footer button').is(':focus');
      if (hasFocus === false) {
        if (direction === 'left') {
          $('.modal-footer button').first().focus();
        }
        else if (direction === 'right') {
          $('.modal-footer button').last().focus();
        }
      }
      else if (hasFocus === true) {
        if (direction === 'left') {
          $(':focus').prev().focus();
        }
        else if (direction === 'right') {
          $(':focus').next().focus();
        }
      }

    },

    createCloseButton: function(cb) {
      var self = this;
      return createButtonStub(this.buttons.CLOSE, this.closeButton.title, function () {
          self.closeButton.callback();
          cb();
      });
    },

    createSuccessButton: function(title, cb) {
      return createButtonStub(this.buttons.SUCCESS, title, cb);
    },

    createNotificationButton: function(title, cb) {
      return createButtonStub(this.buttons.NOTIFICATION, title, cb);
    },

    createDeleteButton: function(title, cb) {
      return createButtonStub(this.buttons.DELETE, title, cb);
    },

    createNeutralButton: function(title, cb) {
      return createButtonStub(this.buttons.NEUTRAL, title, cb);
    },

    createDisabledButton: function(title) {
      var disabledButton = createButtonStub(this.buttons.NEUTRAL, title);
      disabledButton.disabled = true;
      return disabledButton;
    },

    createReadOnlyEntry: function(id, label, value, info, addDelete, addAdd) {
      var obj = createTextStub(this.tables.READONLY, label, value, info,undefined, undefined,
        undefined,addDelete, addAdd);
      obj.id = id;
      return obj;
    },

    createTextEntry: function(id, label, value, info, placeholder, mandatory, regexp) {
      var obj = createTextStub(this.tables.TEXT, label, value, info, placeholder, mandatory,
                               regexp);
      obj.id = id;
      return obj;
    },

    createSelect2Entry: function(
      id, label, value, info, placeholder, mandatory, addDelete, addAdd, maxEntrySize, tags) {
      var obj = createTextStub(this.tables.SELECT2, label, value, info, placeholder,
        mandatory, undefined, addDelete, addAdd, maxEntrySize, tags);
      obj.id = id;
      return obj;
    },

    createPasswordEntry: function(id, label, value, info, placeholder, mandatory) {
      var obj = createTextStub(this.tables.PASSWORD, label, value, info, placeholder, mandatory);
      obj.id = id;
      return obj;
    },

    createCheckboxEntry: function(id, label, value, info, checked) {
      var obj = createTextStub(this.tables.CHECKBOX, label, value, info);
      obj.id = id;
      if (checked) {
        obj.checked = checked;
      }
      return obj;
    },

    createSelectEntry: function(id, label, selected, info, options) {
      var obj = createTextStub(this.tables.SELECT, label, null, info);
      obj.id = id;
      if (selected) {
        obj.selected = selected;
      }
      obj.options = options;
      return obj;
    },

    createOptionEntry: function(label, value) {
      return {
        label: label,
        value: value || label
      };
    },

    show: function(templateName, title, buttons, tableContent, advancedContent,
        events, ignoreConfirm) {
      var self = this, lastBtn, closeButtonFound = false;
      buttons = buttons || [];
      ignoreConfirm = ignoreConfirm || false;
      this.clearValidators();
      // Insert close as second from right
      if (buttons.length > 0) {
        buttons.forEach(function (b) {
            if (b.type === self.buttons.CLOSE) {
                closeButtonFound = true;
            }
        });
        if (!closeButtonFound) {
            lastBtn = buttons.pop();
            buttons.push(self.closeButton);
            buttons.push(lastBtn);
        }
      } else {
        buttons.push(this.closeButton);
      }
      $(this.el).html(this.baseTemplate.render({
        title: title,
        buttons: buttons,
        hideFooter: this.hideFooter
      }));
      _.each(buttons, function(b, i) {
        if (b.disabled || !b.callback) {
          return;
        }
        if (b.type === self.buttons.DELETE && !ignoreConfirm) {
          $("#modalButton" + i).bind("click", function() {
            $(self.confirm.yes).unbind("click");
            $(self.confirm.yes).bind("click", b.callback);
            $(self.confirm.list).css("display", "block");
          });
          return;
        }
        $("#modalButton" + i).bind("click", b.callback);
      });
      $(this.confirm.no).bind("click", function() {
        $(self.confirm.list).css("display", "none");
      });

      var template = templateEngine.createTemplate(templateName),
        model = {};
      model.content = tableContent || [];
      model.advancedContent = advancedContent || false;
      $(".modal-body").html(template.render(model));
      $('.modalTooltips').tooltip({
        position: {
          my: "left top",
          at: "right+55 top-1"
        }
      });
      var ind = buttons.indexOf(this.closeButton);
      buttons.splice(ind, 1);

      var completeTableContent = tableContent;
      try {
        _.each(advancedContent.content, function(x) {
          completeTableContent.push(x);
        });
      }
      catch(ignore) {
      }

      //handle select2
      _.each(completeTableContent, function(r) {
        if (r.type === self.tables.SELECT2) {
          $('#'+r.id).select2({
            tags: r.tags || [],
            showSearchBox: false,
            minimumResultsForSearch: -1,
            width: "336px",
            maximumSelectionSize: r.maxEntrySize || 8
          });
        }
      });//handle select2

      self.testInput = (function(){
        _.each(completeTableContent, function(r){
          self.modalBindValidation(r);
        });
      }());
      if (events) {
          this.events = events;
          this.delegateEvents();
      }

      $("#modal-dialog").modal("show");

      //enable modal hotkeys after rendering is complete
      if (this.enabledHotkey === false) {
        this.createInitModalHotkeys();
        this.enabledHotkey = true;
      }
      if (this.enableHotKeys) {
        this.createModalHotkeys();
      }

      //if input-field is available -> autofocus first one
      var focus = $('#modal-dialog').find('input');
      if (focus) {
        setTimeout(function() {
          var focus = $('#modal-dialog');
          if (focus.length > 0) {
            focus = focus.find('input'); 
              if (focus.length > 0) {
                $(focus[0]).focus();
              }
          }
        }, 800);
      }

    },

    modalBindValidation: function(entry) {
      var self = this;
      if (entry.hasOwnProperty("id")
        && entry.hasOwnProperty("validateInput")) {
        var validCheck = function() {
          var $el = $("#" + entry.id);
          var validation = entry.validateInput($el);
          var error = false, msg;
          _.each(validation, function(validator) {
            var schema = Joi.object().keys({
              toCheck: validator.rule
            });
            var valueToCheck = $el.val();
            Joi.validate(
              {
                toCheck: valueToCheck
              },
              schema,
              function (err) {
                if (err) {
                  msg = validator.msg;
                  error = true;
                }
              }
            );
          });
          if (error) {
            return msg;
          }
        };
        var $el = $('#' + entry.id);
        // catch result of validation and act
        $el.on('keyup focusout', function() {
          var msg = validCheck();
          var errorElement = $el.next()[0];
          if (msg !== undefined) {
            $el.addClass('invalid-input');
            if (errorElement) {
              //error element available
              $(errorElement).text(msg);
            }
            else {
              //error element not available
              $el.after('<p class="errorMessage">' + msg+ '</p>');
            }
            $('.modal-footer .button-success')
              .prop('disabled', true)
              .addClass('disabled');
          } else {
            $el.removeClass('invalid-input');
            if (errorElement) {
              $(errorElement).remove();
            }
            self.modalTestAll();
          }
        });
        this._validators.push(validCheck);
        this._validateWatchers.push($el);
      }
      
    },

    modalTestAll: function() {
      var tests = _.map(this._validators, function(v) {
        return v();
      });
      if (_.any(tests)) {
        $('.modal-footer .button-success')
          .prop('disabled', true)
          .addClass('disabled');
      } else {
        $('.modal-footer .button-success')
          .prop('disabled', false)
          .removeClass('disabled');
      }
    },

    clearValidators: function() {
      this._validators = [];
      _.each(this._validateWatchers, function(w) {
        w.unbind('keyup focusout');
      });
      this._validateWatchers = [];
    },

    hide: function() {
      this.clearValidators();
      $("#modal-dialog").modal("hide");
    }
  });
}());

/*jshint browser: true */
/*jshint unused: false */
/*global Backbone, templateEngine, $, window, arangoHelper*/
(function () {
  "use strict";
  window.NavigationView = Backbone.View.extend({
    el: '#navigationBar',

    events: {
      "change #arangoCollectionSelect": "navigateBySelect",
      "click .tab": "navigateByTab",
      "mouseenter .dropdown": "showDropdown",
      "mouseleave .dropdown": "hideDropdown"
    },

    template: templateEngine.createTemplate("navigationView.ejs"),

    render: function () {
      $(this.el).html(this.template.render());
    },

    navigateByTab: function (e) {
      var tab = e.target || e.srcElement;
      var navigateTo = tab.id;
      if (navigateTo === "") {
        navigateTo = $(tab).attr("class");
      }
      if (navigateTo === "links") {
        $("#link_dropdown").slideToggle(200);
        e.preventDefault();
        return;
      }
      if (navigateTo === "tools") {
        $("#tools_dropdown").slideToggle(200);
        e.preventDefault();
        return;
      }
      if (navigateTo === "dbselection") {
        $("#dbs_dropdown").slideToggle(200);
        e.preventDefault();
        return;
      }
      window.App.navigate(navigateTo, {trigger: true});
      e.preventDefault();
    },

    handleSelectNavigation: function () {
      var self = this;
      $("#arangoCollectionSelect").change(function() {
        self.navigateBySelect();
      });
    },

    selectMenuItem: function (menuItem) {
      $('.navlist li').removeClass('active');
      if (menuItem) {
        $('.' + menuItem).addClass('active');
      }
    },

    showDropdown: function (e) {
      var tab = e.target || e.srcElement;
      var navigateTo = tab.id;
      if (navigateTo === "links" || navigateTo === "link_dropdown") {
        $("#link_dropdown").show(200);
        return;
      }
      if (navigateTo === "tools" || navigateTo === "tools_dropdown") {
        $("#tools_dropdown").show(200);
        return;
      }
      if (navigateTo === "dbselection" || navigateTo === "dbs_dropdown") {
        $("#dbs_dropdown").show(200);
        return;
      }
    },

    hideDropdown: function (e) {
      var tab = e.target || e.srcElement;
      tab = $(tab).closest(".dropdown");
      var navigateTo = tab.attr("id");
      if (navigateTo === "linkDropdown") {
        $("#link_dropdown").hide();
        return;
      }
      if (navigateTo === "toolsDropdown") {
        $("#tools_dropdown").hide();
        return;
      }
      if (navigateTo === "dbSelect") {
        $("#dbs_dropdown").hide();
        return;
      }
    }

  });
}());

/*jshint browser: true */
/*jshint unused: false */
/*global require, exports, Backbone, EJS, $, flush, window, arangoHelper, nv, d3, localStorage*/
/*global document, console, Dygraph, _,templateEngine */

(function () {
  "use strict";

  window.TestView = Backbone.View.extend({
    el: '#content',
    template: templateEngine.createTemplate("testView.ejs"),

    render: function () {
      $(this.el).html(this.template.render());
      this.test();
    },

test2: function() {
var margin = {top: 350, right: 480, bottom: 350, left: 480},
radius = Math.min(margin.top, margin.right, margin.bottom, margin.left) - 10;

var hue = d3.scale.category10();

var luminance = d3.scale.sqrt()
.domain([0, 1e6])
.clamp(true)
.range([90, 20]);

var svg = d3.select("body").append("svg")
  .attr("width", margin.left + margin.right)
  .attr("height", margin.top + margin.bottom)
  .append("g")
  .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

var partition = d3.layout.partition()
  .sort(function(a, b) { return d3.ascending(a.name, b.name); })
  .size([2 * Math.PI, radius]);

var arc = d3.svg.arc()
  .startAngle(function(d) { return d.x; })
  .endAngle(function(d) { return d.x + d.dx - .01 / (d.depth + .5); })
  .innerRadius(function(d) { return radius / 3 * d.depth; })
  .outerRadius(function(d) { return radius / 3 * (d.depth + 1) - 1; });

d3.json("flare.json", function(error, root) {


    // Compute the initial layout on the entire tree to sum sizes.
    // Also compute the full name and fill color for each node,
    // and stash the children so they can be restored as we descend.
    partition
    .value(function(d) { return d.size; })
    .nodes(root)
    .forEach(function(d) {
      d._children = d.children;
      d.sum = d.value;
      d.key = key(d);
      d.fill = fill(d);
      });

    // Now redefine the value function to use the previously-computed sum.
    partition
    .children(function(d, depth) { return depth < 2 ? d._children : null; })
    .value(function(d) { return d.sum; });

    var center = svg.append("circle")
      .attr("r", radius / 3)
      .on("click", zoomOut);

    center.append("title")
      .text("zoom out");

    var path = svg.selectAll("path")
      .data(partition.nodes(root).slice(1))
      .enter().append("path")
      .attr("d", arc)
      .style("fill", function(d) {
        if (d.color === 'white') {
          return 'white';
        }
        return d.fill;
      })
      .each(function(d) { this._current = updateArc(d); })
      .on("click", zoomIn);

    function zoomIn(p) {
      if (p.depth > 1) p = p.parent;
      if (!p.children) return;
      zoom(p, p);
    }

    function zoomOut(p) {
      if (!p.parent) return;
      zoom(p.parent, p);
    }

    // Zoom to the specified new root.
    function zoom(root, p) {
      if (document.documentElement.__transition__) return;

      // Rescale outside angles to match the new layout.
      var enterArc,
          exitArc,
          outsideAngle = d3.scale.linear().domain([0, 2 * Math.PI]);

      function insideArc(d) {
        return p.key > d.key
          ? {depth: d.depth - 1, x: 0, dx: 0} : p.key < d.key
          ? {depth: d.depth - 1, x: 2 * Math.PI, dx: 0}
        : {depth: 0, x: 0, dx: 2 * Math.PI};
      }

      function outsideArc(d) {
        return {depth: d.depth + 1, x: outsideAngle(d.x), dx: outsideAngle(d.x + d.dx) - outsideAngle(d.x)};
      }

      center.datum(root);

      // When zooming in, arcs enter from the outside and exit to the inside.
      // Entering outside arcs start from the old layout.
      if (root === p) enterArc = outsideArc, exitArc = insideArc, outsideAngle.range([p.x, p.x + p.dx]);

      path = path.data(partition.nodes(root).slice(1), function(d) { return d.key; });

      // When zooming out, arcs enter from the inside and exit to the outside.
      // Exiting outside arcs transition to the new layout.
      if (root !== p) enterArc = insideArc, exitArc = outsideArc, outsideAngle.range([p.x, p.x + p.dx]);

      d3.transition().duration(d3.event.altKey ? 7500 : 750).each(function() {
          path.exit().transition()
          .style("fill-opacity", function(d) { return d.depth === 1 + (root === p) ? 1 : 0; })
          .attrTween("d", function(d) { return arcTween.call(this, exitArc(d)); })
          .remove();

          path.enter().append("path")
          .style("fill-opacity", function(d) { return d.depth === 2 - (root === p) ? 1 : 0; })
          .style("fill", function(d) { return d.fill; })
          .on("click", zoomIn)
          .each(function(d) { this._current = enterArc(d); });

          path.transition()
          .style("fill-opacity", 1)
          .attrTween("d", function(d) { return arcTween.call(this, updateArc(d)); });
          });
    }
});

function key(d) {
  var k = [], p = d;
  while (p.depth) k.push(p.name), p = p.parent;
  return k.reverse().join(".");
}

function fill(d) {
  var p = d;
  while (p.depth > 1) p = p.parent;
  var c = d3.lab(hue(p.name));
  c.l = luminance(d.sum);
  return c;
}

function arcTween(b) {
  var i = d3.interpolate(this._current, b);
  this._current = i(0);
  return function(t) {
    return arc(i(t));
  };
}

function updateArc(d) {
  return {depth: d.depth, x: d.x, dx: d.dx};
}

d3.select(self.frameElement).style("height", margin.top + margin.bottom + "px");
},

    test: function() {
      var width = 750;
      var height = 600;
      var radius = Math.min(width, height) / 2;

      // Breadcrumb dimensions: width, height, spacing, width of tip/tail.
      var b = {
        w: 75, h: 30, s: 3, t: 10
      };

      // Mapping of step names to colors.
      var colors = {
        "white": "#EDEDED",
        "db1": "#FFC0CB",
        "db2": "#556B2F",
        "db3": "#8A2BE2",
        "db4": "#2F4F4F",
        "node1": "#87CEEB",
        "node2": "#D2691E",
        "node3": "#FFD700",
        "node4": "#2F4F4F",
        "node5": "#FF0000",
        "node6": "#00FF00",
        "node7": "#0000FF",
        "collection1": "#FF0000",
        "collection2": "#00FF00",
        "collection3": "#FF69B4",
        "collection4": "#0000FF",
        "shard1": "#87CEEB",
        "shard2": "#D2691E",
        "shard3": "#FFD700",
        "shard4": "#2F4F4F"
      };

      // Total size of all segments; we set this later, after loading the data.
      var totalSize = 0; 

      var vis = d3.select("#chart").append("svg:svg")
      .attr("width", width)
      .attr("height", height)
      .append("svg:g")
      .attr("id", "container")
      .attr("transform", "translate(" + width / 2 + "," + height / 2 + ")");

      var partition = d3.layout.partition()
      .size([2 * Math.PI, radius * radius])
      .value(function(d) { return d.size; });

      var arc = d3.svg.arc()
      .startAngle(function(d) { return d.x; })
      .endAngle(function(d) { return d.x + d.dx; })
      .innerRadius(function(d) { return Math.sqrt(d.y); })
      .outerRadius(function(d) { return Math.sqrt(d.y + d.dy); });

      // Use d3.text and d3.csv.parseRows so that we do not need to have a header
      // row, and can receive the csv as an array of arrays.
      d3.text("visit-sequences.csv", function(text) {
        //var csv = d3.csv.parseRows(text);
        //var json = buildHierarchy(csv);

        $.getJSON("flare.json", function(data) {
          var json = data;
          createVisualization(json);
        });

      });

      // Main function to draw and set up the visualization, once we have the data.
      function createVisualization(json) {

        // Basic setup of page elements.
        initializeBreadcrumbTrail();
        drawLegend();
        d3.select("#togglelegend").on("click", toggleLegend);

        // Bounding circle underneath the sunburst, to make it easier to detect
        // when the mouse leaves the parent g.
        vis.append("svg:circle")
        .attr("r", radius)
        .style("opacity", 0);

        // For efficiency, filter nodes to keep only those large enough to see.
        var nodes = partition.nodes(json)
        .filter(function(d) {
          return (d.dx > 0.005); // 0.005 radians = 0.29 degrees
        });

        var path = vis.data([json]).selectAll("path")
        .data(nodes)
        .enter().append("svg:path")
        .attr("display", function(d) { return d.depth ? null : "none"; })
        .attr("d", arc)
        .attr("fill-rule", "evenodd")
        .style("fill", function(d) { return colors[d.name]; })
        .style("opacity", 1)
        .on("mouseover", mouseover);

        // Add the mouseleave handler to the bounding circle.
        d3.select("#container").on("mouseleave", mouseleave);

        // Get total size of the tree = value of root node from partition.
        totalSize = path.node().__data__.value;
      };

      // Fade all but the current sequence, and show it in the breadcrumb trail.
      function mouseover(d) {
        $('#information').empty();

        var selected = [];
        var percentage = (100 * d.value / totalSize).toPrecision(3);
        var percentageString = percentage + "%";
        if (percentage < 0.1) {
          percentageString = "< 0.1%";
        }

        d3.select("#percentage")
        .text(percentageString);

        d3.select("#explanation")
        .style("visibility", "");

        var sequenceArray = getAncestors(d);
        updateBreadcrumbs(sequenceArray, percentageString);

        // Fade all the segments.
        d3.selectAll("path")
        .style("opacity", 0.2);

        // Then highlight only those that are an ancestor of the current segment.
        vis.selectAll("path")
        .filter(function(node) {
          return (sequenceArray.indexOf(node) >= 0);
        })
        .style("opacity", 1);

        //colorize same element groups
        vis.selectAll("path")
        .filter(function(node) {
          if (node.name === d.name) {
            selected.push(node);
            return true;
          }
        })
        .style("opacity", 1);

        $('#information').append(d.name + " @ ");
        _.each(selected, function(node) {
        console.log(node);
          $('#information').append(node.parent.name + "(33%), ");
        });
      }

      // Restore everything to full opacity when moving off the visualization.
      function mouseleave(d) {
        $('#information').empty();

        // Hide the breadcrumb trail
        d3.select("#trail")
        .style("visibility", "hidden");

        // Deactivate all segments during transition.
        d3.selectAll("path").on("mouseover", null);

        // Transition each segment to full opacity and then reactivate it.
        d3.selectAll("path")
        .transition()
        .duration(1000)
        .style("opacity", 1)
        .each("end", function() {
          d3.select(this).on("mouseover", mouseover);
        });

        d3.select("#explanation")
          .style("visibility", "hidden");
        }

        // Given a node in a partition layout, return an array of all of its ancestor
        // nodes, highest first, but excluding the root.
        function getAncestors(node) {
          var path = [];
          var current = node;
          while (current.parent) {
            path.unshift(current);
            current = current.parent;
          }
          return path;
        }

        function initializeBreadcrumbTrail() {
          // Add the svg area.
          var trail = d3.select("#sequence").append("svg:svg")
          .attr("width", width)
          .attr("height", 50)
          .attr("id", "trail");
          // Add the label at the end, for the percentage.
          trail.append("svg:text")
          .attr("id", "endlabel")
          .style("fill", "#000");
        }

        // Generate a string that describes the points of a breadcrumb polygon.
        function breadcrumbPoints(d, i) {
          var points = [];
          points.push("0,0");
          points.push(b.w + ",0");
          points.push(b.w + b.t + "," + (b.h / 2));
          points.push(b.w + "," + b.h);
          points.push("0," + b.h);
          if (i > 0) { // Leftmost breadcrumb; don't include 6th vertex.
            points.push(b.t + "," + (b.h / 2));
          }
          return points.join(" ");
        }

        // Update the breadcrumb trail to show the current sequence and percentage.
        function updateBreadcrumbs(nodeArray, percentageString) {

          // Data join; key function combines name and depth (= position in sequence).
          var g = d3.select("#trail")
          .selectAll("g")
          .data(nodeArray, function(d) { return d.name + d.depth; });

          // Add breadcrumb and label for entering nodes.
          var entering = g.enter().append("svg:g");

          entering.append("svg:polygon")
          .attr("points", breadcrumbPoints)
          .style("fill", function(d) { return colors[d.name]; });

          entering.append("svg:text")
          .attr("x", (b.w + b.t) / 2)
          .attr("y", b.h / 2)
          .attr("dy", "0.35em")
          .attr("text-anchor", "middle")
          .text(function(d) { return d.name; });

          // Set position for entering and updating nodes.
          g.attr("transform", function(d, i) {
            return "translate(" + i * (b.w + b.s) + ", 0)";
          });

          // Remove exiting nodes.
          g.exit().remove();

          // Now move and update the percentage at the end.
          d3.select("#trail").select("#endlabel")
          .attr("x", (nodeArray.length + 0.5) * (b.w + b.s))
          .attr("y", b.h / 2)
          .attr("dy", "0.35em")
          .attr("text-anchor", "middle")
          .text(percentageString);

          // Make the breadcrumb trail visible, if it's hidden.
          d3.select("#trail")
          .style("visibility", "");
        }

        function drawLegend() {

          // Dimensions of legend item: width, height, spacing, radius of rounded rect.
          var li = {
            w: 75, h: 30, s: 3, r: 3
          };

          var legend = d3.select("#legend").append("svg:svg")
          .attr("width", li.w)
          .attr("height", d3.keys(colors).length * (li.h + li.s));

          var g = legend.selectAll("g")
          .data(d3.entries(colors))
          .enter().append("svg:g")
          .attr("transform", function(d, i) {
            return "translate(0," + i * (li.h + li.s) + ")";
          });

          g.append("svg:rect")
          .attr("rx", li.r)
          .attr("ry", li.r)
          .attr("width", li.w)
          .attr("height", li.h)
          .style("fill", function(d) { return d.value; });

          g.append("svg:text")
          .attr("x", li.w / 2)
          .attr("y", li.h / 2)
          .attr("dy", "0.35em")
          .attr("text-anchor", "middle")
          .text(function(d) { return d.key; });
        }

function toggleLegend() {
  var legend = d3.select("#legend");
  if (legend.style("visibility") == "hidden") {
    legend.style("visibility", "");
  } else {
    legend.style("visibility", "hidden");
  }
}

// Take a 2-column CSV and transform it into a hierarchical structure suitable
// for a partition layout. The first column is a sequence of step names, from
// root to leaf, separated by hyphens. The second column is a count of how 
// often that sequence occurred.
function buildHierarchy(csv) {
  var root = {"name": "root", "children": []};
  for (var i = 0; i < csv.length; i++) {
    var sequence = csv[i][0];
    var size = +csv[i][1];
    if (isNaN(size)) { // e.g. if this is a header row
      continue;
    }
    var parts = sequence.split("-");
    var currentNode = root;
    for (var j = 0; j < parts.length; j++) {
      var children = currentNode["children"];
      var nodeName = parts[j];
      var childNode;
      if (j + 1 < parts.length) {
        // Not yet at the end of the sequence; move down the tree.
        var foundChild = false;
        for (var k = 0; k < children.length; k++) {
          if (children[k]["name"] == nodeName) {
            childNode = children[k];
            foundChild = true;
            break;
          }
        }
        // If we don't already have a child node for this branch, create it.
        if (!foundChild) {
          childNode = {"name": nodeName, "children": []};
          children.push(childNode);
        }
        currentNode = childNode;
      } else {
        // Reached the end of the sequence; create a leaf node.
        childNode = {"name": nodeName, "size": size};
        children.push(childNode);
      }
    }
  }

  return root;
};
}
});
}());
