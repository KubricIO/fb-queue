'use strict';

import gulp from 'gulp';
import exit from 'gulp-exit';
import mocha from 'gulp-mocha';
import istanbul from 'gulp-istanbul';
import babel from 'gulp-babel';
import {
  addBabelifyTasks,
  getTaskNames as getBabelifyTasks,
} from './gulp-tasks/babelify';
import clean from './gulp-tasks/clean';


const buildRoot = 'build';
const releaseRoot = 'release';

const serverFiles = {
  src: {
    files: ['src/**/*.js', '!src/Workflow/tests/index.js', '!src/Workflow/tests/stats.js', '!src/index.js'],
    to: `${buildRoot}`
  },
  index: {
    root: 'src',
    fileName: 'index.js',
    addPolyfill: true,
    to: `${buildRoot}`,
  },
  wftest: {
    root: 'src/Workflow/tests',
    fileName: 'index.js',
    addPolyfill: true,
    to: `${buildRoot}/Workflow/tests`
  },
  wfstat: {
    root: 'src/Workflow/tests',
    fileName: 'stats.js',
    addPolyfill: true,
    to: `${buildRoot}/Workflow/tests`
  },
};

const testPaths = {
  js: {
    srcFiles: [
      'src/**/*.js'
    ],
    destDir: 'dist'
  },

  tests: [
    'test/Queue.spec.js',
    'test/Worker.spec.js'
  ]
};

const babelifyTasks = getBabelifyTasks(serverFiles);
gulp.task('build', babelifyTasks);
addBabelifyTasks(gulp, serverFiles, {
  srcRoot: __dirname,
  buildRoot,
  deps: ['clean'],
});

gulp.task('clean', clean(buildRoot));

// Runs the Mocha test suite
gulp.task('test', () => {
  return gulp.src(testPaths.js.srcFiles)
    .pipe(babel({
      presets: ['env'],
      plugins: ['add-module-exports', 'transform-class-properties', 'transform-object-rest-spread', 'transform-function-bind'],
    }))
    .pipe(istanbul())
    .pipe(istanbul.hookRequire())
    .on('finish', () => {
      gulp.src(testPaths.tests)
        .pipe(mocha({
          reporter: 'spec',
          timeout: 10000
        }))
        .pipe(exit());
    });
});

gulp.task('default', ['build']);