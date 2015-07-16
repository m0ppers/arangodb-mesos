module.exports = function (grunt) {

  grunt.initConfig({

    pkg: grunt.file.readJSON('package.json'),

    watch: {
      options: {
        debounceDelay: 1000,
      },
      js: {
        files: ['js/**/*', '!js/generated/*.js'],
        tasks: ['concat']
      },
      scss: {
        files: ['scss/*', '!scss/generated-scss.css', '!scss/generated-scss.css.map'],
        tasks: ['sass']
      },
      html: {
        files: ['html/*', 'js/templates/*.ejs'],
        tasks: ['concat']
      }
    },

    concat: {
      css: {
        src: ['css/epoch.min.css','css/cluster.css', 'css/style.css', '!css/generated-css.css'],
        dest: 'css/generated-css.css'
      },
      js: {
        src: [
          'js/libs/jquery-2.1.0.min.js',
          'js/libs/underscore-min.js',
          'js/libs/backbone-min.js',
          'js/libs/bootstrap.js',
          'js/libs/jquery-ui-1.9.2.custom.js',
          'js/libs/d3.min.js',
          'js/libs/epoch.min.js',
          'js/libs/filesize.min.js'
        ],
        dest: 'js/generated/libs.js'
      },
      views: {
        src: [
          'js/views/*.js',
        ],
        dest: 'js/generated/views.js'
      },
      models: {
        src: [
          'js/models/*.js',
        ],
        dest: 'js/generated/models.js'
      },
      collections: {
        src: [
          'js/collections/*.js',
        ],
        dest: 'js/generated/collections.js'
      },
      routers: {
        src: [
          'js/routers/*.js',
        ],
        dest: 'js/generated/routers.js'
      },
      html: {
        src: [
          'html/start.html.part',
          'html/head.html.part',
          'js/templates/*.ejs',
          'html/body.html.part',
          'html/scripts.html.part',
          'html/end.html.part'
        ],
        dest: 'index.html'
      }
    },

    sass: {
      dist: {
        options: {
          style: 'expanded',
        },
        files: {
          'scss/generated-scss.css': 'scss/style.scss'
        }
      }
    }

  });

  grunt.loadNpmTasks('grunt-contrib-concat');
  grunt.loadNpmTasks('grunt-contrib-cssmin');
  grunt.loadNpmTasks('grunt-contrib-uglify');
  grunt.loadNpmTasks('grunt-contrib-watch');
  grunt.loadNpmTasks('grunt-contrib-sass');
  grunt.loadNpmTasks("grunt-sass");

  grunt.registerTask('default', ['concat', 'sass']);
}
