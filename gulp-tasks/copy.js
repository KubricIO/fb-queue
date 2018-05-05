import gulp from 'gulp';

export default (src, dest) =>
  () => gulp.src(src)
    .pipe(gulp.dest(dest));
