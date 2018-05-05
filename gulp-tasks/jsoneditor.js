import gulp from 'gulp';
import jsonEditor from 'gulp-json-editor';

export default (src, dest, { merge = {}, transform }) =>
  () => gulp.src(src)
    .pipe(jsonEditor(merge))
    .pipe(jsonEditor(json => typeof transform === 'function' ? transform(json) : json))
    .pipe(gulp.dest(dest));
