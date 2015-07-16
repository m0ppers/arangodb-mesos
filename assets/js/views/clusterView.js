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
