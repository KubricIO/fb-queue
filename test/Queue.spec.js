import _ from 'lodash'
import Helpers from './helpers';
import chai from 'chai';
import winston from 'winston';
import chaiAsPromised from 'chai-as-promised';

const expect = chai.expect;

winston.level = 'none';

chai.should();
chai.use(chaiAsPromised);

const th = new Helpers();

describe('Queue', () => {
  describe('initialize', () => {
    it('should not create a Queue with only a queue reference', () => {
      expect(() => {
        new th.Queue(th.testRef);
      }).to.throw('Queue must at least have the queueRef and processingFunction arguments.');
    });

    _.forEach(['', 'foo', NaN, Infinity, true, false, 0, 1, ['foo', 'bar'], { foo: 'bar' }, null, { foo: 'bar' }, { foo: { bar: { baz: true } } }, _.noop], function (nonFirebaseObject) {
      it('should not create a Queue with a non-firebase object: ' + JSON.stringify(nonFirebaseObject), () => expect(() => new th.Queue(nonFirebaseObject, _.noop)).to.throw);
    });

    _.forEach([{}, { foo: 'bar' }, { tasksRef: th.testRef }, { specsRef: th.testRef }], function (invalidRefConfigurationObject) {
      it('should not create a Queue with a ref configuration object that contains keys: {' + _.keys(invalidRefConfigurationObject).join(', ') + '}', () => {
        expect(() => {
          new th.Queue(invalidRefConfigurationObject, _.noop);
        }).to.throw('When ref is an object it must contain both keys \'tasksRef\' and \'specsRef\'');
      });
    });

    _.forEach(['', 'foo', NaN, Infinity, true, false, 0, 1, ['foo', 'bar'], { foo: 'bar' }, null, { foo: 'bar' }, { foo: { bar: { baz: true } } }], function (nonFunctionObject) {
      it('should not create a Queue with a non-function callback: ' + JSON.stringify(nonFunctionObject), () => {
        expect(() => {
          new th.Queue(th.testRef, nonFunctionObject);
        }).to.throw('No processing function provided.');
      });
    });

    it('should create a default Queue with just a Firebase reference and a processing callback', () => {
      new th.Queue(th.testRef, _.noop);
    });

    it('should create a default Queue with tasks and specs Firebase references and a processing callback', () => {
      new th.Queue({ tasksRef: th.testRef, specsRef: th.testRef }, _.noop);
    });

    _.forEach(['', 'foo', NaN, Infinity, true, false, 0, 1, ['foo', 'bar'], null, _.noop], function (nonPlainObject) {
      it('should not create a Queue with a Firebase reference, a non-plain object options parameter (' + JSON.stringify(nonPlainObject) + '), and a processingCallback', () => {
        expect(() => {
          new th.Queue(th.testRef, nonPlainObject, _.noop);
        }).to.throw('Options parameter must be a plain object.');
      });
    });

    it('should create a default Queue with a Firebase reference, an empty options object, and a processing callback', () => {
      new th.Queue(th.testRef, {}, _.noop);
    });

    _.forEach([NaN, Infinity, true, false, 0, 1, ['foo', 'bar'], { foo: 'bar' }, null, { foo: 'bar' }, { foo: { bar: { baz: true } } }, _.noop], function (nonStringObject) {
      it('should not create a Queue with a non-string specId specified', () => {
        expect(() => {
          new th.Queue(th.testRef, { specId: nonStringObject }, _.noop);
        }).to.throw('options.specId must be a String.');
      });
    });

    _.forEach(['', 'foo', NaN, Infinity, true, false, 0, -1, ['foo', 'bar'], { foo: 'bar' }, null, { foo: 'bar' }, { foo: { bar: { baz: true } } }, _.noop], function (nonPositiveIntigerObject) {
      it('should not create a Queue with a non-positive integer numWorkers specified', () => {
        expect(() => {
          new th.Queue(th.testRef, { numWorkers: nonPositiveIntigerObject }, _.noop);
        }).to.throw('options.numWorkers must be a positive integer.');
      });
    });

    _.forEach([NaN, Infinity, '', 'foo', 0, 1, ['foo', 'bar'], { foo: 'bar' }, null, { foo: 'bar' }, { foo: { bar: { baz: true } } }, _.noop], function (nonBooleanObject) {
      it('should not create a Queue with a non-boolean sanitize option specified', () => {
        expect(() => {
          new th.Queue(th.testRef, { sanitize: nonBooleanObject }, _.noop);
        }).to.throw('options.sanitize must be a boolean.');
      });
    });

    _.forEach([NaN, Infinity, '', 'foo', 0, 1, ['foo', 'bar'], { foo: 'bar' }, null, { foo: 'bar' }, { foo: { bar: { baz: true } } }, _.noop], function (nonBooleanObject) {
      it('should not create a Queue with a non-boolean suppressStack option specified', () => {
        expect(() => {
          new th.Queue(th.testRef, { suppressStack: nonBooleanObject }, _.noop);
        }).to.throw('options.suppressStack must be a boolean.');
      });
    });

    _.forEach(_.range(1, 20), function (numWorkers) {
      it('should create a Queue with ' + numWorkers + ' workers when specified in options.numWorkers', () => {
        const q = new th.Queue(th.testRef, { numWorkers: numWorkers }, _.noop);
        expect(q.workers.length).to.equal(numWorkers);
      });
    });

    it('should create a Queue with a specific specId when specified', function (done) {
      const specId = 'test_task';
      const q = new th.Queue(th.testRef, { specId: specId }, _.noop);
      expect(q.specId).to.equal(specId);
      const interval = setInterval(() => {
        if (q.initialized) {
          clearInterval(interval);
          try {
            const specRegex = new RegExp('^' + specId + ':0:[a-f0-9\\-]{36}$');
            expect(q.workers[0].workerId).to.match(specRegex);
            done();
          } catch (error) {
            done(error);
          }
        }
      }, 100);
    });

    [true, false].forEach(function (bool) {
      it('should create a Queue with a ' + bool + ' sanitize option when specified', () => {
        const q = new th.Queue(th.testRef, { sanitize: bool }, _.noop);
        expect(q.sanitize).to.equal(bool);
      });
    });

    [true, false].forEach(function (bool) {
      it('should create a Queue with a ' + bool + ' suppressStack option when specified', () => {
        const q = new th.Queue(th.testRef, { suppressStack: bool }, _.noop);
        expect(q.suppressStack).to.equal(bool);
      });
    });

    it('should not create a Queue when initialized with 4 parameters', () => {
      expect(() => {
        new th.Queue(th.testRef, {}, _.noop, null);
      }).to.throw('Queue can only take at most three arguments - queueRef, options (optional), and processingFunction.');
    });
  });

  describe('#getWorkerCount', () => {
    it('should return worker count with options.numWorkers', () => {
      const numWorkers = 10;
      const q = new th.Queue(th.testRef, { numWorkers: numWorkers }, _.noop);
      expect(q.getWorkerCount()).to.equal(numWorkers);
    });
  });

  describe('#addWorker', () => {
    it('should add worker', () => {
      const q = new th.Queue(th.testRef, _.noop);
      expect(q.getWorkerCount()).to.equal(1);
      q.addWorker();
      expect(q.getWorkerCount()).to.equal(2);
    });

    it('should add worker with correct process id', () => {
      const specId = 'test_task';
      const q = new th.Queue(th.testRef, { specId: specId }, _.noop);
      const worker = q.addWorker();
      const specRegex = new RegExp('^' + specId + ':1:[a-f0-9\\-]{36}$');
      expect(worker.workerId).to.match(specRegex);
    });

    it('should not allow a worker to be added if the queue is shutting down', () => {
      const q = new th.Queue(th.testRef, _.noop);
      expect(q.getWorkerCount()).to.equal(1);
      q.shutdown();
      expect(() => {
        q.addWorker();
      }).to.throw('Cannot add worker while queue is shutting down');
    });
  });

  describe('#shutdownWorker', () => {
    it('should remove worker', () => {
      const q = new th.Queue(th.testRef, _.noop);
      expect(q.getWorkerCount()).to.equal(1);
      q.shutdownWorker();
      expect(q.getWorkerCount()).to.equal(0);
    });

    it('should shutdown worker', () => {
      const q = new th.Queue(th.testRef, _.noop);
      expect(q.getWorkerCount()).to.equal(1);
      const workerShutdownPromise = q.shutdownWorker();
      return workerShutdownPromise;
    });

    it('should reject when no workers remaining', () => {
      const q = new th.Queue(th.testRef, _.noop);
      expect(q.getWorkerCount()).to.equal(1);
      q.shutdownWorker();
      return q.shutdownWorker().catch(function (error) {
        expect(error.message).to.equal('No workers to shutdown');
      });
    });
  });

  describe('#shutdown', () => {
    let q;

    it('should shutdown a queue initialized with the default spec', () => {
      q = new th.Queue(th.testRef, _.noop);
      return q.shutdown().should.eventually.be.fulfilled;
    });

    it('should shutdown a queue initialized with a custom spec before the listener callback', () => {
      q = new th.Queue(th.testRef, { specId: 'test_task' }, _.noop);
      return q.shutdown().should.eventually.be.fulfilled;
    });

    it('should shutdown a queue initialized with a custom spec after the listener callback', function (done) {
      q = new th.Queue(th.testRef, { specId: 'test_task' }, _.noop);
      const interval = setInterval(() => {
        if (q.initialized) {
          clearInterval(interval);
          try {
            const shutdownPromise = q.shutdown();
            expect(q.specChangeListener).to.be.null;
            shutdownPromise.should.eventually.be.fulfilled.notify(done);
          } catch (error) {
            done(error);
          }
        }
      }, 100);
    });
  });
});
