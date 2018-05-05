import gulp from 'gulp';
import babel from 'gulp-babel';
import es from 'event-stream';
import sourcemaps from 'gulp-sourcemaps';

const rename = renameTo => es.map((f, cb) => {
  let path = f.path.split('/');
  path.pop();
  path.push(renameTo);
  f.path = path.join('/');
  cb(null, f);
});

export default (srcFile, dest, renameTo) => () => {
  const stream = gulp.src(srcFile)
    .pipe(sourcemaps.init())
    .pipe(babel({
      presets: ['env'],
      plugins: ['add-module-exports', 'transform-class-properties', 'transform-object-rest-spread', 'transform-function-bind'],
    }));
  if (renameTo) {
    stream.pipe(rename(renameTo));
  }
  return stream.pipe(sourcemaps.write())
    .pipe(gulp.dest(dest));
};
