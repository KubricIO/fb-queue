import gulp from 'gulp';
import clean from 'gulp-clean';

export default src =>
  () => gulp.src(src, {
    read: false,
  }).pipe(clean({
    force: true,
  }));
