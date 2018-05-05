import gulp from 'gulp';

export default (src, tasks) => () => {
  gulp.watch(src, tasks);
};
