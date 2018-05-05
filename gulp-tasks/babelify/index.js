import { add, remove } from './babel-polyfill';
import babelify from './babelify';
import watch from '../watch';

const getTaskName = file => `babelify-${file}`;
const getWatchTaskName = file => `watch-babelify-${file}`;
const getDest = (root, file) => `${root}/${/\/\*\*\/\*\.js/.test(file) ? file.replace('/**/*.js', '') : ''}`;

export const addBabelifyTasks = (gulp, files, { srcRoot, buildRoot, deps = [] }) => {
  for (let file in files) {
    const conf = files[file];
    const taskName = getTaskName(file);
    const watchTaskName = getWatchTaskName(file);
    let watchSrc = '';
    if (typeof conf === 'string') {
      watchSrc = `${srcRoot}/${conf}`;
      gulp.task(taskName, deps, babelify(watchSrc, getDest(buildRoot, conf)));
    } else {
      const { root, fileName, from, addPolyfill, to = buildRoot, files = [] } = conf;
      if (!addPolyfill) {
        watchSrc = files.length > 0 ? files : `${srcRoot}/${from}`;
        gulp.task(taskName, deps, babelify(watchSrc, to));
      } else {
        const addPolyfillTaskName = `add-${file}-polyfill`;
        const polyfilledTaskName = `babelify-polyfilled-${file}`;
        const srcFileRoot = `${srcRoot}/${root}`;
        gulp.task(addPolyfillTaskName, deps, add(srcFileRoot, fileName));
        gulp.task(polyfilledTaskName, [addPolyfillTaskName], babelify(`${srcFileRoot}/_${fileName}`, to || `${buildRoot}/${root}`, fileName));
        gulp.task(taskName, [polyfilledTaskName], remove(srcFileRoot, fileName));
        watchSrc = `${srcFileRoot}/${fileName}`;
      }
    }
    watchSrc.length > 0 && gulp.task(watchTaskName, watch(watchSrc, [taskName]));
  }
};

export const getTaskNames = files => {
  const taskNames = [];
  for (let file in files) {
    taskNames.push(getTaskName(file));
  }
  return taskNames;
};

export const getWatchTaskNames = files => {
  const taskNames = [];
  for (let file in files) {
    taskNames.push(getWatchTaskName(file));
  }
  return taskNames;
};