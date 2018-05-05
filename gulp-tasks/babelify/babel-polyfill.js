import gulp from 'gulp';
import clean from 'gulp-clean';
import es from 'event-stream';

const addPolyfill = filename => es.map((f, cb) => {
  const content = f.contents.toString();
  const modifiedContents = `import babelPolyfill from 'babel-polyfill';\n${content}`;
  f.contents = new Buffer(modifiedContents);
  f.path = f.path.replace(filename, `_${filename}`);
  cb(null, f);
});

export const add = (src, filename) => () =>
  gulp.src(`${src}/${filename}`)
    .pipe(addPolyfill(filename))
    .pipe(gulp.dest(src));

export const remove = (src, filename) => () =>
  gulp.src(`${src}/_${filename}`)
    .pipe(clean({
      force: true,
    }));
