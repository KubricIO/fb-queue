import _ from 'lodash';
import Helpers from './helpers.js';
import chai from 'chai';
import sinon from 'sinon';
import sinonChai from 'sinon-chai';
import winston from 'winston';
import chaiAsPromised from 'chai-as-promised';

winston.level = 'none';

let expect = chai.expect;
chai.should();
chai.use(sinonChai);
chai.use(chaiAsPromised);

let th = new Helpers();
let tasksRef = th.testRef.child('tasks');

describe('QueueWorker', () => {
  describe('initialize', () => {
    it('should not create a QueueWorker with no parameters', () => {
      expect(() => {
        new th.QueueWorker();
      }).to.throw('No tasks reference provided.');
    });

    it('should not create a QueueWorker with only a tasksRef', () => {
      expect(() => {
        new th.QueueWorker(tasksRef);
      }).to.throw('Invalid process ID provided.');
    });

    it('should not create a QueueWorker with only a tasksRef, process ID, sanitize and suppressStack option', () => {
      expect(() => {
        new th.QueueWorker(tasksRef, '0', true, false);
      }).to.throw('No processing function provided.');
    });

    it('should not create a QueueWorker with a tasksRef, workerId, sanitize option and an invalid processing function', () => {
      ['', 'foo', NaN, Infinity, true, false, 0, 1, ['foo', 'bar'], { foo: 'bar' }, null, { foo: 'bar' }, { foo: { bar: { baz: true } } }].forEach(nonFunctionObject => {
        expect(() => {
          new th.QueueWorker(tasksRef, '0', true, false, nonFunctionObject);
        }).to.throw('No processing function provided.');
      });
    });

    it('should create a QueueWorker with a tasksRef, workerId, sanitize option and a processing function', () => {
      new th.QueueWorker(tasksRef, '0', true, false, _.noop);
    });

    it('should not create a QueueWorker with a non-string workerId specified', () => {
      [NaN, Infinity, true, false, 0, 1, ['foo', 'bar'], { foo: 'bar' }, null, { foo: 'bar' }, { foo: { bar: { baz: true } } }, _.noop].forEach(nonStringObject => {
        expect(() => {
          new th.QueueWorker(tasksRef, nonStringObject, true, false, _.noop);
        }).to.throw('Invalid process ID provided.');
      });
    });

    it('should not create a QueueWorker with a non-boolean sanitize option specified', () => {
      [NaN, Infinity, '', 'foo', 0, 1, ['foo', 'bar'], { foo: 'bar' }, null, { foo: 'bar' }, { foo: { bar: { baz: true } } }, _.noop].forEach(nonBooleanObject => {
        expect(() => {
          new th.QueueWorker(tasksRef, '0', nonBooleanObject, false, _.noop);
        }).to.throw('Invalid sanitize option.');
      });
    });

    it('should not create a QueueWorker with a non-boolean suppressStack option specified', () => {
      [NaN, Infinity, '', 'foo', 0, 1, ['foo', 'bar'], { foo: 'bar' }, null, { foo: 'bar' }, { foo: { bar: { baz: true } } }, _.noop].forEach(nonBooleanObject => {
        expect(() => {
          new th.QueueWorker(tasksRef, '0', true, nonBooleanObject, _.noop);
        }).to.throw('Invalid suppressStack option.');
      });
    });
  });

  describe('#getLogEntry', () => {
    let qw = new th.QueueWorker(tasksRef, '0', true, false, _.noop);

    it('should construct a log entry given a string', () => {
      expect(qw.getLogEntry('informative message')).to.equal('QueueWorker ' + qw.workerId + ' informative message');
    });

    it('should construct a log entry given a non-string', () => {
      [NaN, Infinity, true, false, 0, 1, ['foo', 'bar'], { foo: 'bar' }, null, { foo: 'bar' }, { foo: { bar: { baz: true } } }, _.noop].forEach(nonStringObject => {
        expect(qw.getLogEntry(nonStringObject)).to.equal('QueueWorker ' + qw.workerId + ' ' + nonStringObject);
      });
    });
  });

  describe('#resetTask', () => {
    let qw;
    let testRef;

    afterEach(done => {
      qw.setTaskSpec();
      testRef.off();
      tasksRef.set(null, done);
    });

    it('should reset a task that is currently in progress', done => {
      qw = new th.QueueWorkerWithoutProcessingOrTimeouts(tasksRef, '0', true, false, _.noop);
      qw.setTaskSpec(th.validBasicTaskSpec);
      testRef = tasksRef.push({
        '_state': th.validBasicTaskSpec.inProgressState,
        '_state_changed': new Date().getTime(),
        '_owner': qw.getProcessId(),
        '_progress': 10
      }, errorA => {
        if (errorA) {
          return done(errorA);
        }
        qw.currentTaskRef = testRef;
        let initial = true;
        return testRef.on('value', snapshot => {
          if (initial) {
            initial = false;
            qw.resetTask(testRef, true);
          } else {
            try {
              let task = snapshot.val();
              expect(task).to.have.all.keys(['_state_changed']);
              expect(task._state_changed).to.be.closeTo(new Date().getTime() + th.offset, 250);
              done();
            } catch (errorB) {
              done(errorB);
            }
          }
        });
      });
    });

    it('should not reset a task if immediate set but no longer owned by current worker', done => {
      qw = new th.QueueWorkerWithoutProcessingOrTimeouts(tasksRef, '0', true, false, _.noop);
      qw.setTaskSpec(th.validBasicTaskSpec);
      let originalTask = {
        '_state': th.validBasicTaskSpec.inProgressState,
        '_state_changed': new Date().getTime(),
        '_owner': 'someone-else',
        '_progress': 0
      };
      testRef = tasksRef.push(originalTask, errorA => {
        if (errorA) {
          return done(errorA);
        }
        qw.currentTaskRef = testRef;
        return qw.resetTask(testRef, true).then(() => {
          testRef.once('value', snapshot => {
            try {
              expect(snapshot.val()).to.deep.equal(originalTask);
              done();
            } catch (errorB) {
              done(errorB);
            }
          });
        }).catch(done);
      });
    });

    it('should not reset a task if immediate not set and it is has changed state recently', done => {
      qw = new th.QueueWorkerWithoutProcessingOrTimeouts(tasksRef, '0', true, false, _.noop);
      qw.setTaskSpec(th.validBasicTaskSpec);
      let originalTask = {
        '_state': th.validBasicTaskSpec.inProgressState,
        '_state_changed': new Date().getTime(),
        '_owner': 'someone',
        '_progress': 0
      };
      testRef = tasksRef.push(originalTask, errorA => {
        if (errorA) {
          return done(errorA);
        }
        qw.currentTaskRef = testRef;
        return qw.resetTask(testRef, false).then(() => {
          testRef.once('value', snapshot => {
            try {
              expect(snapshot.val()).to.deep.equal(originalTask);
              done();
            } catch (errorB) {
              done(errorB);
            }
          });
        }).catch(done);
      });
    });

    it('should reset a task that is currently in progress that has timed out', done => {
      qw = new th.QueueWorkerWithoutProcessingOrTimeouts(tasksRef, '0', true, false, _.noop);
      qw.setTaskSpec(th.validTaskSpecWithTimeout);
      testRef = tasksRef.push({
        '_state': th.validBasicTaskSpec.inProgressState,
        '_state_changed': new Date().getTime() - th.validTaskSpecWithTimeout.timeout,
        '_owner': 'someone',
        '_progress': 10
      }, errorA => {
        if (errorA) {
          return done(errorA);
        }
        qw.currentTaskRef = testRef;
        let initial = true;
        return testRef.on('value', snapshot => {
          if (initial) {
            initial = false;
            qw.resetTask(testRef, false);
          } else {
            try {
              let task = snapshot.val();
              expect(task).to.have.all.keys(['_state_changed']);
              expect(task._state_changed).to.be.closeTo(new Date().getTime() + th.offset, 250);
              done();
            } catch (errorB) {
              done(errorB);
            }
          }
        });
      });
    });

    it('should not reset a task that no longer exists', done => {
      qw = new th.QueueWorkerWithoutProcessingOrTimeouts(tasksRef, '0', true, false, _.noop);
      qw.setTaskSpec(th.validBasicTaskSpec);

      testRef = tasksRef.push();
      qw.currentTaskRef = testRef;
      qw.resetTask(testRef, true).then(() => {
        testRef.once('value', snapshot => {
          try {
            expect(snapshot.val()).to.be.null;
            done();
          } catch (error) {
            done(error);
          }
        });
      }).catch(done);
    });

    it('should not reset a task if it is has already changed state', done => {
      qw = new th.QueueWorkerWithoutProcessingOrTimeouts(tasksRef, '0', true, false, _.noop);
      let originalTask = {
        '_state': th.validTaskSpecWithFinishedState.finishedState,
        '_state_changed': new Date().getTime(),
        '_owner': qw.getProcessId(),
        '_progress': 0
      };
      qw.setTaskSpec(th.validTaskSpecWithFinishedState);
      testRef = tasksRef.push(originalTask, errorA => {
        if (errorA) {
          return done(errorA);
        }
        qw.currentTaskRef = testRef;
        return qw.resetTask(testRef, true).then(() => {
          testRef.once('value', snapshot => {
            try {
              expect(snapshot.val()).to.deep.equal(originalTask);
              done();
            } catch (errorB) {
              done(errorB);
            }
          });
        }).catch(done);
      });
    });

    it('should not reset a task if it is has no state', done => {
      qw = new th.QueueWorkerWithoutProcessingOrTimeouts(tasksRef, '0', true, false, _.noop);
      let originalTask = {
        '_state_changed': new Date().getTime(),
        '_owner': qw.getProcessId(),
        '_progress': 0
      };
      qw.setTaskSpec(th.validTaskSpecWithFinishedState);
      testRef = tasksRef.push(originalTask, errorA => {
        if (errorA) {
          return done(errorA);
        }
        qw.currentTaskRef = testRef;
        return qw.resetTask(testRef, true).then(() => {
          testRef.once('value', snapshot => {
            try {
              expect(snapshot.val()).to.deep.equal(originalTask);
              done();
            } catch (errorB) {
              done(errorB);
            }
          });
        }).catch(done);
      });
    });
  });

  describe('#resolve', () => {
    let qw;
    let testRef;

    afterEach(done => {
      qw.setTaskSpec();
      testRef.off();
      tasksRef.set(null, done);
    });

    it('should resolve a task owned by the current worker and remove it when no finishedState is specified', done => {
      qw = new th.QueueWorkerWithoutProcessingOrTimeouts(tasksRef, '0', true, false, _.noop);
      qw.setTaskSpec(th.validBasicTaskSpec);
      testRef = tasksRef.push({
        '_state': th.validBasicTaskSpec.inProgressState,
        '_state_changed': new Date().getTime(),
        '_owner': qw.getProcessId(),
        '_progress': 0
      }, errorA => {
        if (errorA) {
          return done(errorA);
        }
        qw.currentTaskRef = testRef;
        let initial = true;
        return testRef.on('value', snapshot => {
          if (initial) {
            initial = false;
            qw.resolve(qw.taskNumber)();
          } else {
            try {
              expect(snapshot.val()).to.be.null;
              done();
            } catch (errorB) {
              done(errorB);
            }
          }
        });
      });
    });

    it('should resolve a task owned by the current worker and change the state when a finishedState is specified and no object passed', done => {
      qw = new th.QueueWorkerWithoutProcessingOrTimeouts(tasksRef, '0', true, false, _.noop);
      qw.setTaskSpec(th.validTaskSpecWithFinishedState);
      testRef = tasksRef.push({
        '_state': th.validTaskSpecWithFinishedState.inProgressState,
        '_state_changed': new Date().getTime(),
        '_owner': qw.getProcessId(),
        '_progress': 0
      }, errorA => {
        if (errorA) {
          return done(errorA);
        }
        qw.currentTaskRef = testRef;
        let initial = true;
        return testRef.on('value', snapshot => {
          if (initial) {
            initial = false;
            qw.resolve(qw.taskNumber)();
          } else {
            try {
              let task = snapshot.val();
              expect(task).to.have.all.keys(['_state', '_state_changed', '_progress']);
              expect(task._progress).to.equal(100);
              expect(task._state).to.equal(th.validTaskSpecWithFinishedState.finishedState);
              expect(task._state_changed).to.be.closeTo(new Date().getTime() + th.offset, 250);
              done();
            } catch (errorB) {
              done(errorB);
            }
          }
        });
      });
    });

    ['', 'foo', NaN, Infinity, true, false, 0, 1, ['foo', 'bar'], null, _.noop].forEach(nonPlainObject => {
      it('should resolve an task owned by the current worker and change the state when a finishedState is specified and an invalid object ' + nonPlainObject + ' passed', done => {
        qw = new th.QueueWorkerWithoutProcessingOrTimeouts(tasksRef, '0', true, false, _.noop);
        qw.setTaskSpec(th.validTaskSpecWithFinishedState);
        testRef = tasksRef.push({
          '_state': th.validTaskSpecWithFinishedState.inProgressState,
          '_state_changed': new Date().getTime(),
          '_owner': qw.getProcessId(),
          '_progress': 0
        }, errorA => {
          if (errorA) {
            return done(errorA);
          }
          qw.currentTaskRef = testRef;
          let initial = true;
          return testRef.on('value', snapshot => {
            if (initial) {
              initial = false;
              qw.resolve(qw.taskNumber)(nonPlainObject);
            } else {
              try {
                let task = snapshot.val();
                expect(task).to.have.all.keys(['_state', '_state_changed', '_progress']);
                expect(task._progress).to.equal(100);
                expect(task._state).to.equal(th.validTaskSpecWithFinishedState.finishedState);
                expect(task._state_changed).to.be.closeTo(new Date().getTime() + th.offset, 250);
                done();
              } catch (errorB) {
                done(errorB);
              }
            }
          });
        });
      });
    });

    it('should resolve a task owned by the current worker and change the state when a finishedState is specified and a plain object passed', done => {
      qw = new th.QueueWorkerWithoutProcessingOrTimeouts(tasksRef, '0', true, false, _.noop);
      qw.setTaskSpec(th.validTaskSpecWithFinishedState);
      testRef = tasksRef.push({
        '_state': th.validTaskSpecWithFinishedState.inProgressState,
        '_state_changed': new Date().getTime(),
        '_owner': qw.getProcessId(),
        '_progress': 0
      }, errorA => {
        if (errorA) {
          return done(errorA);
        }
        qw.currentTaskRef = testRef;
        let initial = true;
        return testRef.on('value', snapshot => {
          if (initial) {
            initial = false;
            qw.resolve(qw.taskNumber)({ foo: 'bar' });
          } else {
            try {
              let task = snapshot.val();
              expect(task).to.have.all.keys(['_state', '_state_changed', '_progress', 'foo']);
              expect(task._progress).to.equal(100);
              expect(task._state).to.equal(th.validTaskSpecWithFinishedState.finishedState);
              expect(task._state_changed).to.be.closeTo(new Date().getTime() + th.offset, 250);
              expect(task.foo).to.equal('bar');
              done();
            } catch (errorB) {
              done(errorB);
            }
          }
        });
      });
    });

    it('should resolve a task owned by the current worker and change the state to a provided valid string _new_state', done => {
      qw = new th.QueueWorkerWithoutProcessingOrTimeouts(tasksRef, '0', true, false, _.noop);
      qw.setTaskSpec(th.validTaskSpecWithFinishedState);
      testRef = tasksRef.push({
        '_state': th.validTaskSpecWithFinishedState.inProgressState,
        '_state_changed': new Date().getTime(),
        '_owner': qw.getProcessId(),
        '_progress': 0
      }, errorA => {
        if (errorA) {
          return done(errorA);
        }
        qw.currentTaskRef = testRef;
        let initial = true;
        return testRef.on('value', snapshot => {
          if (initial) {
            initial = false;
            qw.resolve(qw.taskNumber)({
              foo: 'bar',
              _new_state: 'valid_new_state'
            });
          } else {
            try {
              let task = snapshot.val();
              expect(task).to.have.all.keys(['_state', '_state_changed', '_progress', 'foo']);
              expect(task._progress).to.equal(100);
              expect(task._state).to.equal('valid_new_state');
              expect(task._state_changed).to.be.closeTo(new Date().getTime() + th.offset, 250);
              expect(task.foo).to.equal('bar');
              done();
            } catch (errorB) {
              done(errorB);
            }
          }
        });
      });
    });

    it('should resolve a task owned by the current worker and change the state to a provided valid null _new_state', done => {
      qw = new th.QueueWorkerWithoutProcessingOrTimeouts(tasksRef, '0', true, false, _.noop);
      qw.setTaskSpec(th.validTaskSpecWithFinishedState);
      testRef = tasksRef.push({
        '_state': th.validTaskSpecWithFinishedState.inProgressState,
        '_state_changed': new Date().getTime(),
        '_owner': qw.getProcessId(),
        '_progress': 0
      }, errorA => {
        if (errorA) {
          return done(errorA);
        }
        qw.currentTaskRef = testRef;
        let initial = true;
        return testRef.on('value', snapshot => {
          if (initial) {
            initial = false;
            qw.resolve(qw.taskNumber)({
              foo: 'bar',
              _new_state: null
            });
          } else {
            try {
              let task = snapshot.val();
              expect(task).to.have.all.keys(['_state_changed', '_progress', 'foo']);
              expect(task._progress).to.equal(100);
              expect(task._state_changed).to.be.closeTo(new Date().getTime() + th.offset, 250);
              expect(task.foo).to.equal('bar');
              done();
            } catch (errorB) {
              done(errorB);
            }
          }
        });
      });
    });

    it('should resolve a task owned by the current worker and remove the task when provided _new_state = false', done => {
      qw = new th.QueueWorkerWithoutProcessingOrTimeouts(tasksRef, '0', true, false, _.noop);
      qw.setTaskSpec(th.validTaskSpecWithFinishedState);
      testRef = tasksRef.push({
        '_state': th.validTaskSpecWithFinishedState.inProgressState,
        '_state_changed': new Date().getTime(),
        '_owner': qw.getProcessId(),
        '_progress': 0
      }, errorA => {
        if (errorA) {
          return done(errorA);
        }
        qw.currentTaskRef = testRef;
        let initial = true;
        return testRef.on('value', snapshot => {
          if (initial) {
            initial = false;
            qw.resolve(qw.taskNumber)({
              foo: 'bar',
              _new_state: false
            });
          } else {
            try {
              expect(snapshot.val()).to.be.null;
              done();
            } catch (errorB) {
              done(errorB);
            }
          }
        });
      });
    });

    it('should resolve a task owned by the current worker and change the state to finishedState when provided an invalid _new_state', done => {
      qw = new th.QueueWorkerWithoutProcessingOrTimeouts(tasksRef, '0', true, false, _.noop);
      qw.setTaskSpec(th.validTaskSpecWithFinishedState);
      testRef = tasksRef.push({
        '_state': th.validTaskSpecWithFinishedState.inProgressState,
        '_state_changed': new Date().getTime(),
        '_owner': qw.getProcessId(),
        '_progress': 0
      }, errorA => {
        if (errorA) {
          return done(errorA);
        }
        qw.currentTaskRef = testRef;
        let initial = true;
        return testRef.on('value', snapshot => {
          if (initial) {
            initial = false;
            qw.resolve(qw.taskNumber)({
              foo: 'bar',
              _new_state: {
                state: 'object_is_an_invalid_new_state'
              }
            });
          } else {
            try {
              let task = snapshot.val();
              expect(task).to.have.all.keys(['_state', '_state_changed', '_progress', 'foo']);
              expect(task._progress).to.equal(100);
              expect(task._state).to.equal(th.validTaskSpecWithFinishedState.finishedState);
              expect(task._state_changed).to.be.closeTo(new Date().getTime() + th.offset, 250);
              expect(task.foo).to.equal('bar');
              done();
            } catch (errorB) {
              done(errorB);
            }
          }
        });
      });
    });

    it('should not resolve a task that no longer exists', done => {
      qw = new th.QueueWorkerWithoutProcessingOrTimeouts(tasksRef, '0', true, false, _.noop);
      qw.setTaskSpec(th.validTaskSpecWithFinishedState);

      testRef = tasksRef.push();
      qw.currentTaskRef = testRef;
      qw.resolve(qw.taskNumber)().then(() => {
        testRef.once('value', snapshot => {
          try {
            expect(snapshot.val()).to.be.null;
            done();
          } catch (error) {
            done(error);
          }
        });
      }).catch(done);
    });

    it('should not resolve a task if it is no longer owned by the current worker', done => {
      qw = new th.QueueWorkerWithoutProcessingOrTimeouts(tasksRef, '0', true, false, _.noop);
      let originalTask = {
        '_state': th.validTaskSpecWithFinishedState.inProgressState,
        '_state_changed': new Date().getTime(),
        '_owner': 'other_worker',
        '_progress': 0
      };
      qw.setTaskSpec(th.validTaskSpecWithFinishedState);
      testRef = tasksRef.push(originalTask, errorA => {
        if (errorA) {
          return done(errorA);
        }
        qw.currentTaskRef = testRef;
        return qw.resolve(qw.taskNumber)().then(() => {
          testRef.once('value', snapshot => {
            try {
              expect(snapshot.val()).to.deep.equal(originalTask);
              done();
            } catch (errorB) {
              done(errorB);
            }
          });
        }).catch(done);
      });
    });

    it('should not resolve a task if it is has already changed state', done => {
      qw = new th.QueueWorkerWithoutProcessingOrTimeouts(tasksRef, '0', true, false, _.noop);
      let originalTask = {
        '_state': th.validTaskSpecWithFinishedState.finishedState,
        '_state_changed': new Date().getTime(),
        '_owner': qw.getProcessId(),
        '_progress': 0
      };
      qw.setTaskSpec(th.validTaskSpecWithFinishedState);
      testRef = tasksRef.push(originalTask, errorA => {
        if (errorA) {
          return done(errorA);
        }
        qw.currentTaskRef = testRef;
        return qw.resolve(qw.taskNumber)().then(() => {
          testRef.once('value', snapshot => {
            try {
              expect(snapshot.val()).to.deep.equal(originalTask);
              done();
            } catch (errorB) {
              done(errorB);
            }
          });
        }).catch(done);
      });
    });

    it('should not resolve a task if it is has no state', done => {
      qw = new th.QueueWorkerWithoutProcessingOrTimeouts(tasksRef, '0', true, false, _.noop);
      let originalTask = {
        '_state_changed': new Date().getTime(),
        '_owner': qw.getProcessId(),
        '_progress': 0
      };
      qw.setTaskSpec(th.validTaskSpecWithFinishedState);
      testRef = tasksRef.push(originalTask, errorA => {
        if (errorA) {
          return done(errorA);
        }
        qw.currentTaskRef = testRef;
        return qw.resolve(qw.taskNumber)().then(() => {
          testRef.once('value', snapshot => {
            try {
              expect(snapshot.val()).to.deep.equal(originalTask);
              done();
            } catch (errorB) {
              done(errorB);
            }
          });
        }).catch(done);
      });
    });

    it('should not resolve a task if it is no longer being processed', done => {
      qw = new th.QueueWorkerWithoutProcessingOrTimeouts(tasksRef, '0', true, false, _.noop);
      let originalTask = {
        '_state': th.validTaskSpecWithFinishedState.inProgressState,
        '_state_changed': new Date().getTime(),
        '_owner': qw.getProcessId(),
        '_progress': 0
      };
      qw.setTaskSpec(th.validTaskSpecWithFinishedState);
      testRef = tasksRef.push(originalTask, errorA => {
        if (errorA) {
          return done(errorA);
        }
        return qw.resolve(qw.taskNumber)().then(() => {
          testRef.once('value', snapshot => {
            try {
              expect(snapshot.val()).to.deep.equal(originalTask);
              done();
            } catch (errorB) {
              done(errorB);
            }
          });
        }).catch(done);
      });
    });

    it('should not resolve a task if a new task is being processed', done => {
      qw = new th.QueueWorkerWithoutProcessingOrTimeouts(tasksRef, '0', true, false, _.noop);
      let originalTask = {
        '_state': th.validTaskSpecWithFinishedState.inProgressState,
        '_state_changed': new Date().getTime(),
        '_owner': qw.getProcessId(),
        '_progress': 0
      };
      qw.setTaskSpec(th.validTaskSpecWithFinishedState);
      testRef = tasksRef.push(originalTask, errorA => {
        if (errorA) {
          return done(errorA);
        }
        qw.currentTaskRef = testRef;
        let resolve = qw.resolve(qw.taskNumber);
        qw.taskNumber += 1;
        return resolve().then(() => {
          testRef.once('value', snapshot => {
            try {
              expect(snapshot.val()).to.deep.equal(originalTask);
              done();
            } catch (errorB) {
              done(errorB);
            }
          });
        }).catch(done);
      });
    });
  });

  describe('#reject', () => {
    let qw;
    let testRef;

    afterEach(done => {
      qw.setTaskSpec();
      testRef.off();
      tasksRef.set(null, done);
    });

    it('should reject a task owned by the current worker', done => {
      qw = new th.QueueWorkerWithoutProcessingOrTimeouts(tasksRef, '0', true, false, _.noop);
      qw.setTaskSpec(th.validBasicTaskSpec);
      testRef = tasksRef.push({
        '_state': th.validBasicTaskSpec.inProgressState,
        '_state_changed': new Date().getTime(),
        '_owner': qw.getProcessId(),
        '_progress': 0
      }, errorA => {
        if (errorA) {
          return done(errorA);
        }
        qw.currentTaskRef = testRef;
        let initial = true;
        return testRef.on('value', snapshot => {
          if (initial) {
            initial = false;
            qw.reject(qw.taskNumber)();
          } else {
            try {
              let task = snapshot.val();
              expect(task).to.have.all.keys(['_state', '_progress', '_state_changed', '_error_details']);
              expect(task._state).to.equal('error');
              expect(task._state_changed).to.be.closeTo(new Date().getTime() + th.offset, 250);
              expect(task._progress).to.equal(0);
              expect(task._error_details).to.have.all.keys(['previous_state', 'attempts']);
              expect(task._error_details.previous_state).to.equal(th.validBasicTaskSpec.inProgressState);
              expect(task._error_details.attempts).to.equal(1);
              done();
            } catch (errorB) {
              done(errorB);
            }
          }
        });
      });
    });

    it('should reject a task owned by the current worker and reset if more retries are specified', done => {
      qw = new th.QueueWorkerWithoutProcessingOrTimeouts(tasksRef, '0', true, false, _.noop);
      qw.setTaskSpec(th.validTaskSpecWithRetries);
      testRef = tasksRef.push({
        '_state': th.validTaskSpecWithRetries.inProgressState,
        '_state_changed': new Date().getTime(),
        '_owner': qw.getProcessId(),
        '_progress': 0,
        '_error_details': {
          'previous_state': th.validTaskSpecWithRetries.inProgressState,
          'attempts': 1
        }
      }, errorA => {
        if (errorA) {
          return done(errorA);
        }
        qw.currentTaskRef = testRef;
        let initial = true;
        return testRef.on('value', snapshot => {
          if (initial) {
            initial = false;
            qw.reject(qw.taskNumber)();
          } else {
            try {
              let task = snapshot.val();
              expect(task).to.have.all.keys(['_progress', '_state_changed', '_error_details']);
              expect(task._state_changed).to.be.closeTo(new Date().getTime() + th.offset, 250);
              expect(task._progress).to.equal(0);
              expect(task._error_details).to.have.all.keys(['previous_state', 'attempts']);
              expect(task._error_details.previous_state).to.equal(th.validBasicTaskSpec.inProgressState);
              expect(task._error_details.attempts).to.equal(2);
              done();
            } catch (errorB) {
              done(errorB);
            }
          }
        });
      });
    });

    it('should reject a task owned by the current worker and reset the attempts count if chaning error handlers', done => {
      qw = new th.QueueWorkerWithoutProcessingOrTimeouts(tasksRef, '0', true, false, _.noop);
      qw.setTaskSpec(th.validTaskSpecWithRetries);
      testRef = tasksRef.push({
        '_state': th.validTaskSpecWithRetries.inProgressState,
        '_state_changed': new Date().getTime(),
        '_owner': qw.getProcessId(),
        '_progress': 0,
        '_error_details': {
          'previous_state': 'other_in_progress_state',
          'attempts': 1
        }
      }, errorA => {
        if (errorA) {
          return done(errorA);
        }
        qw.currentTaskRef = testRef;
        let initial = true;
        return testRef.on('value', snapshot => {
          if (initial) {
            initial = false;
            qw.reject(qw.taskNumber)();
          } else {
            try {
              let task = snapshot.val();
              expect(task).to.have.all.keys(['_progress', '_state_changed', '_error_details']);
              expect(task._state_changed).to.be.closeTo(new Date().getTime() + th.offset, 250);
              expect(task._progress).to.equal(0);
              expect(task._error_details).to.have.all.keys(['previous_state', 'attempts']);
              expect(task._error_details.previous_state).to.equal(th.validBasicTaskSpec.inProgressState);
              expect(task._error_details.attempts).to.equal(1);
              done();
            } catch (errorB) {
              done(errorB);
            }
          }
        });
      });
    });

    it('should reject a task owned by the current worker and a non-standard error state', done => {
      qw = new th.QueueWorkerWithoutProcessingOrTimeouts(tasksRef, '0', true, false, _.noop);
      qw.setTaskSpec(th.validTaskSpecWithErrorState);
      testRef = tasksRef.push({
        '_state': th.validBasicTaskSpec.inProgressState,
        '_state_changed': new Date().getTime(),
        '_owner': qw.getProcessId(),
        '_progress': 0
      }, errorA => {
        if (errorA) {
          return done(errorA);
        }
        qw.currentTaskRef = testRef;
        let initial = true;
        return testRef.on('value', snapshot => {
          if (initial) {
            initial = false;
            qw.reject(qw.taskNumber)();
          } else {
            try {
              let task = snapshot.val();
              expect(task).to.have.all.keys(['_state', '_progress', '_state_changed', '_error_details']);
              expect(task._state).to.equal(th.validTaskSpecWithErrorState.errorState);
              expect(task._state_changed).to.be.closeTo(new Date().getTime() + th.offset, 250);
              expect(task._progress).to.equal(0);
              expect(task._error_details).to.have.all.keys(['previous_state', 'attempts']);
              expect(task._error_details.previous_state).to.equal(th.validBasicTaskSpec.inProgressState);
              expect(task._error_details.attempts).to.equal(1);
              done();
            } catch (errorB) {
              done(errorB);
            }
          }
        });
      });
    });

    [NaN, Infinity, true, false, 0, 1, ['foo', 'bar'], { foo: 'bar' }, { foo: 'bar' }, { foo: { bar: { baz: true } } }, _.noop].forEach(nonStringObject => {
      it('should reject a task owned by the current worker and convert the error to a string if not a string: ' + nonStringObject, done => {
        qw = new th.QueueWorkerWithoutProcessingOrTimeouts(tasksRef, '0', true, false, _.noop);
        qw.setTaskSpec(th.validBasicTaskSpec);
        testRef = tasksRef.push({
          '_state': th.validBasicTaskSpec.inProgressState,
          '_state_changed': new Date().getTime(),
          '_owner': qw.getProcessId(),
          '_progress': 0
        }, errorA => {
          if (errorA) {
            return done(errorA);
          }
          qw.currentTaskRef = testRef;
          let initial = true;
          return testRef.on('value', snapshot => {
            if (initial) {
              initial = false;
              qw.reject(qw.taskNumber)(nonStringObject);
            } else {
              try {
                let task = snapshot.val();
                expect(task).to.have.all.keys(['_state', '_progress', '_state_changed', '_error_details']);
                expect(task._state).to.equal('error');
                expect(task._state_changed).to.be.closeTo(new Date().getTime() + th.offset, 250);
                expect(task._progress).to.equal(0);
                expect(task._error_details).to.have.all.keys(['previous_state', 'error', 'attempts']);
                expect(task._error_details.previous_state).to.equal(th.validBasicTaskSpec.inProgressState);
                expect(task._error_details.error).to.equal(nonStringObject.toString());
                expect(task._error_details.attempts).to.equal(1);
                done();
              } catch (errorB) {
                done(errorB);
              }
            }
          });
        });
      });
    });

    it('should reject a task owned by the current worker and append the error string to the _error_details', done => {
      qw = new th.QueueWorkerWithoutProcessingOrTimeouts(tasksRef, '0', true, false, _.noop);
      let error = 'My error message';
      qw.setTaskSpec(th.validBasicTaskSpec);
      testRef = tasksRef.push({
        '_state': th.validBasicTaskSpec.inProgressState,
        '_state_changed': new Date().getTime(),
        '_owner': qw.getProcessId(),
        '_progress': 0
      }, errorA => {
        if (errorA) {
          return done(errorA);
        }
        qw.currentTaskRef = testRef;
        let initial = true;
        return testRef.on('value', snapshot => {
          if (initial) {
            initial = false;
            qw.reject(qw.taskNumber)(error);
          } else {
            try {
              let task = snapshot.val();
              expect(task).to.have.all.keys(['_state', '_progress', '_state_changed', '_error_details']);
              expect(task._state).to.equal('error');
              expect(task._state_changed).to.be.closeTo(new Date().getTime() + th.offset, 250);
              expect(task._progress).to.equal(0);
              expect(task._error_details).to.have.all.keys(['previous_state', 'error', 'attempts']);
              expect(task._error_details.previous_state).to.equal(th.validBasicTaskSpec.inProgressState);
              expect(task._error_details.attempts).to.equal(1);
              expect(task._error_details.error).to.equal(error);
              done();
            } catch (errorB) {
              done(errorB);
            }
          }
        });
      });
    });

    it('should reject a task owned by the current worker and append the error string and stack to the _error_details', done => {
      qw = new th.QueueWorkerWithoutProcessingOrTimeouts(tasksRef, '0', true, false, _.noop);
      let error = new Error('My error message');
      qw.setTaskSpec(th.validBasicTaskSpec);
      testRef = tasksRef.push({
        '_state': th.validBasicTaskSpec.inProgressState,
        '_state_changed': new Date().getTime(),
        '_owner': qw.getProcessId(),
        '_progress': 0
      }, errorA => {
        if (errorA) {
          return done(errorA);
        }
        qw.currentTaskRef = testRef;
        let initial = true;
        return testRef.on('value', snapshot => {
          if (initial) {
            initial = false;
            qw.reject(qw.taskNumber)(error);
          } else {
            try {
              let task = snapshot.val();
              expect(task).to.have.all.keys(['_state', '_progress', '_state_changed', '_error_details']);
              expect(task._state).to.equal('error');
              expect(task._state_changed).to.be.closeTo(new Date().getTime() + th.offset, 250);
              expect(task._progress).to.equal(0);
              expect(task._error_details).to.have.all.keys(['previous_state', 'error', 'attempts', 'error_stack']);
              expect(task._error_details.previous_state).to.equal(th.validBasicTaskSpec.inProgressState);
              expect(task._error_details.attempts).to.equal(1);
              expect(task._error_details.error).to.equal(error.message);
              expect(task._error_details.error_stack).to.be.a.string;
              done();
            } catch (errorB) {
              done(errorB);
            }
          }
        });
      });
    });

    it('should reject a task owned by the current worker and append the error string to the _error_details', done => {
      qw = new th.QueueWorkerWithoutProcessingOrTimeouts(tasksRef, '0', true, false, _.noop);
      qw.suppressStack = true;
      let error = new Error('My error message');
      qw.setTaskSpec(th.validBasicTaskSpec);
      testRef = tasksRef.push({
        '_state': th.validBasicTaskSpec.inProgressState,
        '_state_changed': new Date().getTime(),
        '_owner': qw.getProcessId(),
        '_progress': 0
      }, errorA => {
        if (errorA) {
          return done(errorA);
        }
        qw.currentTaskRef = testRef;
        let initial = true;
        return testRef.on('value', snapshot => {
          if (initial) {
            initial = false;
            qw.reject(qw.taskNumber)(error);
          } else {
            try {
              let task = snapshot.val();
              expect(task).to.have.all.keys(['_state', '_progress', '_state_changed', '_error_details']);
              expect(task._state).to.equal('error');
              expect(task._state_changed).to.be.closeTo(new Date().getTime() + th.offset, 250);
              expect(task._progress).to.equal(0);
              expect(task._error_details).to.have.all.keys(['previous_state', 'error', 'attempts']);
              expect(task._error_details.previous_state).to.equal(th.validBasicTaskSpec.inProgressState);
              expect(task._error_details.attempts).to.equal(1);
              expect(task._error_details.error).to.equal(error.message);
              done();
            } catch (errorB) {
              done(errorB);
            }
          }
        });
      });
    });

    it('should not reject a task that no longer exists', done => {
      qw = new th.QueueWorkerWithoutProcessingOrTimeouts(tasksRef, '0', true, false, _.noop);
      qw.setTaskSpec(th.validTaskSpecWithFinishedState);
      testRef = tasksRef.push();
      qw.currentTaskRef = testRef;
      qw.reject(qw.taskNumber)().then(() => {
        testRef.once('value', snapshot => {
          try {
            expect(snapshot.val()).to.be.null;
            done();
          } catch (error) {
            done(error);
          }
        });
      }).catch(done);
    });

    it('should not reject a task if it is no longer owned by the current worker', done => {
      qw = new th.QueueWorkerWithoutProcessingOrTimeouts(tasksRef, '0', true, false, _.noop);
      let originalTask = {
        '_state': th.validTaskSpecWithFinishedState.inProgressState,
        '_state_changed': new Date().getTime(),
        '_owner': 'other_worker',
        '_progress': 0
      };
      qw.setTaskSpec(th.validTaskSpecWithFinishedState);
      testRef = tasksRef.push(originalTask, errorA => {
        if (errorA) {
          return done(errorA);
        }
        qw.currentTaskRef = testRef;
        return qw.reject(qw.taskNumber)().then(() => {
          testRef.once('value', snapshot => {
            try {
              expect(snapshot.val()).to.deep.equal(originalTask);
              done();
            } catch (errorB) {
              done(errorB);
            }
          });
        }).catch(done);
      });
    });

    it('should not reject a task if it is has already changed state', done => {
      qw = new th.QueueWorkerWithoutProcessingOrTimeouts(tasksRef, '0', true, false, _.noop);
      let originalTask = {
        '_state': th.validTaskSpecWithFinishedState.finishedState,
        '_state_changed': new Date().getTime(),
        '_owner': qw.getProcessId(),
        '_progress': 0
      };
      qw.setTaskSpec(th.validTaskSpecWithFinishedState);
      testRef = tasksRef.push(originalTask, errorA => {
        if (errorA) {
          return done(errorA);
        }
        qw.currentTaskRef = testRef;
        return qw.reject(qw.taskNumber)().then(() => {
          testRef.once('value', snapshot => {
            try {
              expect(snapshot.val()).to.deep.equal(originalTask);
              done();
            } catch (errorB) {
              done(errorB);
            }
          });
        }).catch(done);
      });
    });

    it('should not reject a task if it is has no state', done => {
      qw = new th.QueueWorkerWithoutProcessingOrTimeouts(tasksRef, '0', true, false, _.noop);
      let originalTask = {
        '_state_changed': new Date().getTime(),
        '_owner': qw.getProcessId(),
        '_progress': 0
      };
      qw.setTaskSpec(th.validTaskSpecWithFinishedState);
      testRef = tasksRef.push(originalTask, errorA => {
        if (errorA) {
          return done(errorA);
        }
        qw.currentTaskRef = testRef;
        return qw.reject(qw.taskNumber)().then(() => {
          testRef.once('value', snapshot => {
            try {
              expect(snapshot.val()).to.deep.equal(originalTask);
              done();
            } catch (errorB) {
              done(errorB);
            }
          });
        }).catch(done);
      });
    });

    it('should not reject a task if it is no longer being processed', done => {
      qw = new th.QueueWorkerWithoutProcessingOrTimeouts(tasksRef, '0', true, false, _.noop);
      let originalTask = {
        '_state': th.validTaskSpecWithFinishedState.inProgressState,
        '_state_changed': new Date().getTime(),
        '_owner': qw.getProcessId(),
        '_progress': 0
      };
      qw.setTaskSpec(th.validTaskSpecWithFinishedState);
      testRef = tasksRef.push(originalTask, errorA => {
        if (errorA) {
          return done(errorA);
        }
        return qw.reject(qw.taskNumber)().then(() => {
          testRef.once('value', snapshot => {
            try {
              expect(snapshot.val()).to.deep.equal(originalTask);
              done();
            } catch (errorB) {
              done(errorB);
            }
          });
        }).catch(done);
      });
    });

    it('should not reject a task if a new task is being processed', done => {
      qw = new th.QueueWorkerWithoutProcessingOrTimeouts(tasksRef, '0', true, false, _.noop);
      let originalTask = {
        '_state': th.validTaskSpecWithFinishedState.inProgressState,
        '_state_changed': new Date().getTime(),
        '_owner': qw.getProcessId(),
        '_progress': 0
      };
      qw.setTaskSpec(th.validTaskSpecWithFinishedState);
      testRef = tasksRef.push(originalTask, errorA => {
        if (errorA) {
          return done(errorA);
        }
        qw.currentTaskRef = testRef;
        let reject = qw.reject(qw.taskNumber);
        qw.taskNumber += 1;
        return reject().then(() => {
          testRef.once('value', snapshot => {
            try {
              expect(snapshot.val()).to.deep.equal(originalTask);
              done();
            } catch (errorB) {
              done(errorB);
            }
          });
        }).catch(done);
      });
    });
  });

  describe('#updateProgress', () => {
    let qw;

    beforeEach(() => {
      qw = new th.QueueWorkerWithoutProcessingOrTimeouts(tasksRef, '0', true, false, _.noop);
      qw.tryToProcess = _.noop;
    });

    afterEach(done => {
      qw.setTaskSpec();
      tasksRef.set(null, done);
    });

    ['', 'foo', NaN, Infinity, true, false, -1, 100.1, ['foo', 'bar'], { foo: 'bar' }, { foo: 'bar' }, { foo: { bar: { baz: true } } }, _.noop].forEach(invalidPercentageValue => {
      it('should ignore invalid input ' + invalidPercentageValue + ' to update the progress', () => {
        qw.currentTaskRef = tasksRef.push();
        return qw.updateProgress(qw.taskNumber)(invalidPercentageValue).should.eventually.be.rejectedWith('Invalid progress');
      });
    });

    it('should not update the progress of a task no longer owned by the current worker', done => {
      qw.setTaskSpec(th.validBasicTaskSpec);
      qw.currentTaskRef = tasksRef.push({
        '_state': th.validBasicTaskSpec.inProgressState,
        '_owner': 'someone_else'
      }, error => {
        if (error) {
          return done(error);
        }
        return qw.updateProgress(qw.taskNumber)(10).should.eventually.be.rejectedWith('Can\'t update progress - current task no longer owned by this process').notify(done);
      });
    });

    it('should not update the progress of a task if the worker is no longer processing it', done => {
      qw.setTaskSpec(th.validBasicTaskSpec);
      tasksRef.push({
        '_state': th.validBasicTaskSpec.inProgressState,
        '_owner': qw.getProcessId()
      }, error => {
        if (error) {
          return done(error);
        }
        return qw.updateProgress(qw.taskNumber)(10).should.eventually.be.rejectedWith('Can\'t update progress - no task currently being processed').notify(done);
      });
    });

    it('should not update the progress of a task if the task is no longer in progress', done => {
      qw.setTaskSpec(th.validTaskSpecWithFinishedState);
      qw.currentTaskRef = tasksRef.push({
        '_state': th.validTaskSpecWithFinishedState.finishedState,
        '_owner': qw.getProcessId()
      }, error => {
        if (error) {
          return done(error);
        }
        return qw.updateProgress(qw.taskNumber)(10).should.eventually.be.rejectedWith('Can\'t update progress - current task no longer owned by this process').notify(done);
      });
    });

    it('should not update the progress of a task if the task has no _state', done => {
      qw.setTaskSpec(th.validBasicTaskSpec);
      qw.currentTaskRef = tasksRef.push({ '_owner': qw.getProcessId() }, error => {
        if (error) {
          return done(error);
        }
        return qw.updateProgress(qw.taskNumber)(10).should.eventually.be.rejectedWith('Can\'t update progress - current task no longer owned by this process').notify(done);
      });
    });

    it('should update the progress of the current task', done => {
      qw.setTaskSpec(th.validBasicTaskSpec);
      qw.currentTaskRef = tasksRef.push({
        '_state': th.validBasicTaskSpec.inProgressState,
        '_owner': qw.getProcessId()
      }, error => {
        if (error) {
          return done(error);
        }
        return qw.updateProgress(qw.taskNumber)(10).should.eventually.be.fulfilled.notify(done);
      });
    });

    it('should not update the progress of a task if a new task is being processed', done => {
      qw.setTaskSpec(th.validBasicTaskSpec);
      qw.currentTaskRef = tasksRef.push({ '_owner': qw.getProcessId() }, error => {
        if (error) {
          return done(error);
        }
        let updateProgress = qw.updateProgress(qw.taskNumber);
        qw.taskNumber += 1;
        return updateProgress(10).should.eventually.be.rejectedWith('Can\'t update progress - no task currently being processed').notify(done);
      });
    });
  });

  describe('#tryToProcess', () => {
    let qw;

    beforeEach(() => {
      qw = new th.QueueWorker(tasksRef, '0', true, false, _.noop);
    });

    afterEach(done => {
      qw.setTaskSpec();
      tasksRef.set(null, done);
    });

    it('should not try and process a task if busy', done => {
      qw.startState = th.validTaskSpecWithStartState.startState;
      qw.inProgressState = th.validTaskSpecWithStartState.inProgressState;
      qw.busy = true;
      qw.newTaskRef = tasksRef;
      tasksRef.push({
        '_state': th.validTaskSpecWithStartState.startState
      }, errorA => {
        if (errorA) {
          return done(errorA);
        }
        return qw.tryToProcess().then(() => {
          try {
            expect(qw.currentTaskRef).to.be.null;
            done();
          } catch (errorB) {
            done(errorB);
          }
        }).catch(done);
      });
    });

    it('should try and process a task if not busy', done => {
      qw.startState = th.validTaskSpecWithStartState.startState;
      qw.inProgressState = th.validTaskSpecWithStartState.inProgressState;
      qw.newTaskRef = tasksRef;
      tasksRef.push({
        '_state': th.validTaskSpecWithStartState.startState
      }, errorA => {
        if (errorA) {
          return done(errorA);
        }
        return qw.tryToProcess().then(() => {
          try {
            expect(qw.currentTaskRef).to.not.be.null;
            expect(qw.busy).to.be.true;
            done();
          } catch (errorB) {
            done(errorB);
          }
        }).catch(done);
      });
    });

    it('should try and process a task if not busy, rejecting it if it throws', done => {
      qw = new th.QueueWorker(tasksRef, '0', true, false, () => {
        throw new Error('Error thrown in processingFunction');
      });
      qw.startState = th.validTaskSpecWithStartState.startState;
      qw.inProgressState = th.validTaskSpecWithStartState.inProgressState;
      qw.finishedState = th.validTaskSpecWithFinishedState.finishedState;
      qw.taskRetries = 0;
      qw.newTaskRef = tasksRef;
      let testRef = tasksRef.push({
        '_state': th.validTaskSpecWithStartState.startState
      }, errorA => {
        if (errorA) {
          return done(errorA);
        }
        return qw.tryToProcess().then(() => {
          try {
            expect(qw.currentTaskRef).to.not.be.null;
            expect(qw.busy).to.be.true;
            let initial = true;
            testRef.on('value', snapshot => {
              if (initial) {
                initial = false;
              } else {
                try {
                  testRef.off();
                  let task = snapshot.val();
                  expect(task).to.have.all.keys(['_state', '_progress', '_state_changed', '_error_details']);
                  expect(task._state).to.equal('error');
                  expect(task._state_changed).to.be.closeTo(new Date().getTime() + th.offset, 250);
                  expect(task._progress).to.equal(0);
                  expect(task._error_details).to.have.all.keys(['previous_state', 'attempts', 'error', 'error_stack']);
                  expect(task._error_details.previous_state).to.equal(th.validTaskSpecWithStartState.inProgressState);
                  expect(task._error_details.attempts).to.equal(1);
                  expect(task._error_details.error).to.equal('Error thrown in processingFunction');
                  expect(task._error_details.error_stack).to.be.a.string;
                  done();
                } catch (errorC) {
                  done(errorC);
                }
              }
            });
          } catch (errorB) {
            done(errorB);
          }
        }).catch(done);
      });
    });

    it('should try and process a task without a _state if not busy', done => {
      qw.startState = null;
      qw.inProgressState = th.validBasicTaskSpec.inProgressState;
      qw.newTaskRef = tasksRef;
      tasksRef.push({
        foo: 'bar'
      }, errorA => {
        if (errorA) {
          return done(errorA);
        }
        return qw.tryToProcess().then(() => {
          try {
            expect(qw.currentTaskRef).to.not.be.null;
            expect(qw.busy).to.be.true;
            done();
          } catch (errorB) {
            done(errorB);
          }
        }).catch(done);
      });
    });

    it('should not try and process a task if not a plain object [1]', done => {
      qw.inProgressState = th.validTaskSpecWithStartState.inProgressState;
      qw.suppressStack = true;
      qw.newTaskRef = tasksRef;
      let testRef = tasksRef.push('invalid', errorA => {
        if (errorA) {
          return done(errorA);
        }
        return qw.tryToProcess().then(() => {
          try {
            expect(qw.currentTaskRef).to.be.null;
            expect(qw.busy).to.be.false;
            testRef.once('value', snapshot => {
              try {
                let task = snapshot.val();
                expect(task).to.have.all.keys(['_error_details', '_state', '_state_changed']);
                expect(task._error_details).to.have.all.keys(['error', 'original_task']);
                expect(task._error_details.error).to.equal('Task was malformed');
                expect(task._error_details.original_task).to.equal('invalid');
                expect(task._state).to.equal('error');
                expect(task._state_changed).to.be.closeTo(new Date().getTime() + th.offset, 500);
                done();
              } catch (errorB) {
                done(errorB);
              }
            });
          } catch (errorC) {
            done(errorC);
          }
        }).catch(done);
      });
    });

    it('should not try and process a task if not a plain object [2]', done => {
      qw.inProgressState = th.validTaskSpecWithStartState.inProgressState;
      qw.newTaskRef = tasksRef;
      let testRef = tasksRef.push('invalid', errorA => {
        if (errorA) {
          return done(errorA);
        }
        return qw.tryToProcess().then(() => {
          try {
            expect(qw.currentTaskRef).to.be.null;
            expect(qw.busy).to.be.false;
            testRef.once('value', snapshot => {
              try {
                let task = snapshot.val();
                expect(task).to.have.all.keys(['_error_details', '_state', '_state_changed']);
                expect(task._error_details).to.have.all.keys(['error', 'original_task', 'error_stack']);
                expect(task._error_details.error).to.equal('Task was malformed');
                expect(task._error_details.original_task).to.equal('invalid');
                expect(task._error_details.error_stack).to.be.a.string;
                expect(task._state).to.equal('error');
                expect(task._state_changed).to.be.closeTo(new Date().getTime() + th.offset, 500);
                done();
              } catch (errorB) {
                done(errorB);
              }
            });
          } catch (errorC) {
            done(errorC);
          }
        }).catch(done);
      });
    });

    it('should not try and process a task if no longer in correct startState', done => {
      qw.startState = th.validTaskSpecWithStartState.startState;
      qw.inProgressState = th.validTaskSpecWithStartState.inProgressState;
      qw.newTaskRef = tasksRef;
      tasksRef.push({
        '_state': th.validTaskSpecWithStartState.inProgressState
      }, errorA => {
        if (errorA) {
          return done(errorA);
        }
        return qw.tryToProcess().then(() => {
          try {
            expect(qw.currentTaskRef).to.be.null;
            done();
          } catch (errorB) {
            done(errorB);
          }
        }).catch(done);
      });
    });

    it('should not try and process a task if no task to process', done => {
      qw.startState = th.validTaskSpecWithStartState.startState;
      qw.inProgressState = th.validTaskSpecWithStartState.inProgressState;
      qw.newTaskRef = tasksRef;
      qw.tryToProcess().then(() => {
        try {
          expect(qw.currentTaskRef).to.be.null;
          done();
        } catch (errorB) {
          done(errorB);
        }
      }).catch(done);
    });

    it('should invalidate callbacks if another process times the task out', done => {
      qw.startState = th.validTaskSpecWithStartState.startState;
      qw.inProgressState = th.validTaskSpecWithStartState.inProgressState;
      qw.newTaskRef = tasksRef;
      let testRef = tasksRef.push({
        '_state': th.validTaskSpecWithStartState.startState
      }, errorA => {
        if (errorA) {
          return done(errorA);
        }
        return qw.tryToProcess().then(() => {
          try {
            expect(qw.currentTaskRef).to.not.be.null;
            expect(qw.busy).to.be.true;
            testRef.update({
              '_owner': null
            }, errorB => {
              if (errorB) {
                return done(errorB);
              }
              try {
                expect(qw.currentTaskRef).to.be.null;
                done();
              } catch (errorC) {
                done(errorC);
              }
              return undefined;
            });
          } catch (errorD) {
            done(errorD);
          }
        }).catch(done);
      });
    });

    it('should sanitize data passed to the processing function when specified', done => {
      qw = new th.QueueWorker(tasksRef, '0', true, false, data => {
        try {
          expect(data).to.have.all.keys(['foo']);
          done();
        } catch (error) {
          done(error);
        }
      });
      qw.setTaskSpec(th.validBasicTaskSpec);
      tasksRef.push({ foo: 'bar' });
    });

    it('should not sanitize data passed to the processing function when specified', done => {
      qw = new th.QueueWorker(tasksRef, '0', false, false, data => {
        try {
          expect(data).to.have.all.keys(['foo', '_owner', '_progress', '_state', '_state_changed', '_id']);
          done();
        } catch (error) {
          done(error);
        }
      });
      qw.setTaskSpec(th.validBasicTaskSpec);
      tasksRef.push({ foo: 'bar' });
    });
  });

  describe('#setupTimeouts', () => {
    let qw;
    let clock;

    beforeEach(() => {
      clock = sinon.useFakeTimers(new Date().getTime());
      qw = new th.QueueWorkerWithoutProcessing(tasksRef, '0', true, false, _.noop);
    });

    afterEach(done => {
      qw.setTaskSpec();
      clock.restore();
      tasksRef.set(null, done);
    });

    it('should not set up timeouts when no task timeout is set', done => {
      qw.setTaskSpec(th.validBasicTaskSpec);
      tasksRef.push({
        '_state': th.validBasicTaskSpec.inProgressState,
        '_state_changed': new Date().getTime()
      }, errorA => {
        if (errorA) {
          return done(errorA);
        }
        try {
          expect(qw.expiryTimeouts).to.deep.equal({});
          done();
        } catch (errorB) {
          done(errorB);
        }
        return undefined;
      });
    });

    it('should not set up timeouts when a task not in progress is added and a task timeout is set', done => {
      qw.setTaskSpec(th.validTaskSpecWithTimeout);
      tasksRef.push({
        '_state': th.validTaskSpecWithFinishedState.finishedState,
        '_state_changed': new Date().getTime()
      }, errorA => {
        if (errorA) {
          return done(errorA);
        }
        try {
          expect(qw.expiryTimeouts).to.deep.equal({});
          done();
        } catch (errorB) {
          done(errorB);
        }
        return undefined;
      });
    });

    it('should set up timeout listeners when a task timeout is set', () => {
      expect(qw.expiryTimeouts).to.deep.equal({});
      expect(qw.processingTasksRef).to.be.null;
      expect(qw.processingTaskAddedListener).to.be.null;
      expect(qw.processingTaskRemovedListener).to.be.null;

      qw.setTaskSpec(th.validTaskSpecWithTimeout);

      expect(qw.expiryTimeouts).to.deep.equal({});
      expect(qw.processingTasksRef).to.not.be.null;
      expect(qw.processingTaskAddedListener).to.not.be.null;
      expect(qw.processingTaskRemovedListener).to.not.be.null;
    });

    it('should remove timeout listeners when a task timeout is not specified after a previous task specified a timeout', () => {
      qw.setTaskSpec(th.validTaskSpecWithTimeout);

      expect(qw.expiryTimeouts).to.deep.equal({});
      expect(qw.processingTasksRef).to.not.be.null;
      expect(qw.processingTaskAddedListener).to.not.be.null;
      expect(qw.processingTaskRemovedListener).to.not.be.null;

      qw.setTaskSpec(th.validBasicTaskSpec);

      expect(qw.expiryTimeouts).to.deep.equal({});
      expect(qw.processingTasksRef).to.be.null;
      expect(qw.processingTaskAddedListener).to.be.null;
      expect(qw.processingTaskRemovedListener).to.be.null;
    });

    it('should set up a timeout when a task timeout is set and a task added', done => {
      let spy = sinon.spy(global, 'setTimeout');
      qw.setTaskSpec(th.validTaskSpecWithTimeout);
      let testRef = tasksRef.push({
        '_state': th.validTaskSpecWithTimeout.inProgressState,
        '_state_changed': new Date().getTime() - 5
      }, errorA => {
        if (errorA) {
          return done(errorA);
        }
        try {
          expect(qw.expiryTimeouts).to.have.all.keys([testRef.key]);
          expect(setTimeout.getCall(0).args[1]).to.equal(th.validTaskSpecWithTimeout.timeout - 5);
          spy.restore();
          done();
        } catch (errorB) {
          spy.restore();
          done(errorB);
        }
        return undefined;
      });
    });

    it('should set up a timeout when a task timeout is set and a task owner changed', done => {
      qw.setTaskSpec(th.validTaskSpecWithTimeout);
      let testRef = tasksRef.push({
        '_owner': qw.workerId + ':0',
        '_state': th.validTaskSpecWithTimeout.inProgressState,
        '_state_changed': new Date().getTime() - 10
      }, errorA => {
        if (errorA) {
          return done(errorA);
        }
        try {
          expect(qw.expiryTimeouts).to.have.all.keys([testRef.key]);
          let spy = sinon.spy(global, 'setTimeout');
          testRef.update({
            '_owner': qw.workerId + ':1',
            '_state_changed': new Date().getTime() - 5
          }, errorB => {
            if (errorB) {
              return done(errorB);
            }
            try {
              expect(qw.expiryTimeouts).to.have.all.keys([testRef.key]);
              expect(setTimeout.getCall(setTimeout.callCount - 1).args[1]).to.equal(th.validTaskSpecWithTimeout.timeout - 5);
              spy.restore();
              done();
            } catch (errorC) {
              spy.restore();
              done(errorC);
            }
            return undefined;
          });
        } catch (errorB) {
          done(errorB);
        }
        return undefined;
      });
    });

    it('should not set up a timeout when a task timeout is set and a task updated', done => {
      qw.setTaskSpec(th.validTaskSpecWithTimeout);
      let spy = sinon.spy(global, 'setTimeout');
      let testRef = tasksRef.push({
        '_owner': qw.workerId + ':0',
        '_progress': 0,
        '_state': th.validTaskSpecWithTimeout.inProgressState,
        '_state_changed': new Date().getTime() - 5
      }, errorA => {
        if (errorA) {
          return done(errorA);
        }
        try {
          expect(qw.expiryTimeouts).to.have.all.keys([testRef.key]);
          testRef.update({
            '_progress': 1
          }, errorB => {
            if (errorB) {
              return done(errorB);
            }
            try {
              expect(qw.expiryTimeouts).to.have.all.keys([testRef.key]);
              expect(setTimeout.getCall(0).args[1]).to.equal(th.validTaskSpecWithTimeout.timeout - 5);
              spy.restore();
              done();
            } catch (errorC) {
              spy.restore();
              done(errorC);
            }
            return undefined;
          });
        } catch (errorB) {
          done(errorB);
        }
        return undefined;
      });
    });

    it('should set up a timeout when a task timeout is set and a task added without a _state_changed time', done => {
      let spy = sinon.spy(global, 'setTimeout');
      qw.setTaskSpec(th.validTaskSpecWithTimeout);
      let testRef = tasksRef.push({
        '_state': th.validTaskSpecWithTimeout.inProgressState
      }, errorA => {
        if (errorA) {
          return done(errorA);
        }
        try {
          expect(qw.expiryTimeouts).to.have.all.keys([testRef.key]);
          expect(setTimeout.getCall(0).args[1]).to.equal(th.validTaskSpecWithTimeout.timeout);
          spy.restore();
          done();
        } catch (errorB) {
          spy.restore();
          done(errorB);
        }
        return undefined;
      });
    });

    it('should clear timeouts when a task timeout is not set and a timeout exists', done => {
      qw.setTaskSpec(th.validTaskSpecWithTimeout);
      let testRef = tasksRef.push({
        '_state': th.validTaskSpecWithTimeout.inProgressState,
        '_state_changed': new Date().getTime()
      }, errorA => {
        if (errorA) {
          return done(errorA);
        }
        try {
          expect(qw.expiryTimeouts).to.have.all.keys([testRef.key]);
          qw.setTaskSpec();
          expect(qw.expiryTimeouts).to.deep.equal({});
          done();
        } catch (errorB) {
          done(errorB);
        }
        return undefined;
      });
    });

    it('should clear a timeout when a task is completed', done => {
      let spy = sinon.spy(qw, 'resetTask');
      let taskSpec = _.clone(th.validTaskSpecWithTimeout);
      taskSpec.finishedState = th.validTaskSpecWithFinishedState.finishedState;
      qw.setTaskSpec(taskSpec);
      let testRef = tasksRef.push({
        '_state': taskSpec.inProgressState,
        '_state_changed': new Date().getTime()
      }, errorA => {
        if (errorA) {
          spy.restore();
          return done(errorA);
        }
        try {
          expect(qw.expiryTimeouts).to.have.all.keys([testRef.key]);
          testRef.update({
            '_state': taskSpec.finishedState
          }, errorB => {
            if (errorB) {
              return done(errorB);
            }
            try {
              expect(qw.expiryTimeouts).to.deep.equal({});
              expect(qw.resetTask).to.not.have.been.called;
              spy.restore();
              done();
            } catch (errorC) {
              spy.restore();
              done(errorC);
            }
            return undefined;
          });
        } catch (errorD) {
          spy.restore();
          done(errorD);
        }
        return undefined;
      });
    });
  });

  describe('#isValidTaskSpec', () => {
    it('should not accept a non-plain object as a valid task spec', () => {
      ['', 'foo', NaN, Infinity, true, false, 0, 1, ['foo', 'bar'], null, _.noop].forEach(nonPlainObject => {
        expect(th.QueueWorker.isValidTaskSpec(nonPlainObject)).to.be.false;
      });
    });

    it('should not accept an empty object as a valid task spec', () => {
      expect(th.QueueWorker.isValidTaskSpec({})).to.be.false;
    });

    it('should not accept a non-empty object without the required keys as a valid task spec', () => {
      expect(th.QueueWorker.isValidTaskSpec({ foo: 'bar' })).to.be.false;
    });

    it('should not accept a startState that is not a string as a valid task spec', () => {
      [NaN, Infinity, true, false, 0, 1, ['foo', 'bar'], { foo: 'bar' }, { foo: 'bar' }, { foo: { bar: { baz: true } } }, _.noop].forEach(nonStringObject => {
        let taskSpec = _.clone(th.validBasicTaskSpec);
        taskSpec.startState = nonStringObject;
        expect(th.QueueWorker.isValidTaskSpec(taskSpec)).to.be.false;
      });
    });

    it('should not accept an inProgressState that is not a string as a valid task spec', () => {
      [NaN, Infinity, true, false, 0, 1, ['foo', 'bar'], { foo: 'bar' }, null, { foo: 'bar' }, { foo: { bar: { baz: true } } }, _.noop].forEach(nonStringObject => {
        let taskSpec = _.clone(th.validBasicTaskSpec);
        taskSpec.inProgressState = nonStringObject;
        expect(th.QueueWorker.isValidTaskSpec(taskSpec)).to.be.false;
      });
    });

    it('should not accept a finishedState that is not a string as a valid task spec', () => {
      [NaN, Infinity, true, false, 0, 1, ['foo', 'bar'], { foo: 'bar' }, { foo: 'bar' }, { foo: { bar: { baz: true } } }, _.noop].forEach(nonStringObject => {
        let taskSpec = _.clone(th.validBasicTaskSpec);
        taskSpec.finishedState = nonStringObject;
        expect(th.QueueWorker.isValidTaskSpec(taskSpec)).to.be.false;
      });
    });

    it('should not accept a finishedState that is not a string as a valid task spec', () => {
      [NaN, Infinity, true, false, 0, 1, ['foo', 'bar'], { foo: 'bar' }, { foo: 'bar' }, { foo: { bar: { baz: true } } }, _.noop].forEach(nonStringObject => {
        let taskSpec = _.clone(th.validBasicTaskSpec);
        taskSpec.errorState = nonStringObject;
        expect(th.QueueWorker.isValidTaskSpec(taskSpec)).to.be.false;
      });
    });

    it('should not accept a timeout that is not a positive integer as a valid task spec', () => {
      ['', 'foo', NaN, Infinity, true, false, 0, -1, 1.1, ['foo', 'bar'], { foo: 'bar' }, { foo: 'bar' }, { foo: { bar: { baz: true } } }, _.noop].forEach(nonPositiveIntigerObject => {
        let taskSpec = _.clone(th.validBasicTaskSpec);
        taskSpec.timeout = nonPositiveIntigerObject;
        expect(th.QueueWorker.isValidTaskSpec(taskSpec)).to.be.false;
      });
    });

    it('should not accept a retries that is not a positive or 0 integer as a valid task spec', () => {
      ['', 'foo', NaN, Infinity, true, false, -1, 1.1, ['foo', 'bar'], { foo: 'bar' }, { foo: 'bar' }, { foo: { bar: { baz: true } } }, _.noop].forEach(nonPositiveIntigerObject => {
        let taskSpec = _.clone(th.validBasicTaskSpec);
        taskSpec.retries = nonPositiveIntigerObject;
        expect(th.QueueWorker.isValidTaskSpec(taskSpec)).to.be.false;
      });
    });

    it('should accept a valid task spec without a timeout', () => {
      expect(th.QueueWorker.isValidTaskSpec(th.validBasicTaskSpec)).to.be.true;
    });

    it('should accept a valid task spec with a startState', () => {
      expect(th.QueueWorker.isValidTaskSpec(th.validTaskSpecWithStartState)).to.be.true;
    });

    it('should not accept a taskSpec with the same startState and inProgressState', () => {
      let taskSpec = _.clone(th.validBasicTaskSpec);
      taskSpec.startState = taskSpec.inProgressState;
      expect(th.QueueWorker.isValidTaskSpec(taskSpec)).to.be.false;
    });

    it('should accept a valid task spec with a finishedState', () => {
      expect(th.QueueWorker.isValidTaskSpec(th.validTaskSpecWithFinishedState)).to.be.true;
    });

    it('should not accept a taskSpec with the same finishedState and inProgressState', () => {
      let taskSpec = _.clone(th.validBasicTaskSpec);
      taskSpec.finishedState = taskSpec.inProgressState;
      expect(th.QueueWorker.isValidTaskSpec(taskSpec)).to.be.false;
    });

    it('should accept a valid task spec with a errorState', () => {
      expect(th.QueueWorker.isValidTaskSpec(th.validTaskSpecWithErrorState)).to.be.true;
    });

    it('should not accept a taskSpec with the same errorState and inProgressState', () => {
      let taskSpec = _.clone(th.validBasicTaskSpec);
      taskSpec.errorState = taskSpec.inProgressState;
      expect(th.QueueWorker.isValidTaskSpec(taskSpec)).to.be.false;
    });

    it('should accept a valid task spec with a timeout', () => {
      expect(th.QueueWorker.isValidTaskSpec(th.validTaskSpecWithTimeout)).to.be.true;
    });

    it('should accept a valid task spec with retries', () => {
      expect(th.QueueWorker.isValidTaskSpec(th.validTaskSpecWithRetries)).to.be.true;
    });

    it('should accept a valid task spec with 0 retries', () => {
      let taskSpec = _.clone(th.validBasicTaskSpec);
      taskSpec.retries = 0;
      expect(th.QueueWorker.isValidTaskSpec(taskSpec)).to.be.true;
    });

    it('should not accept a taskSpec with the same startState and finishedState', () => {
      let taskSpec = _.clone(th.validTaskSpecWithFinishedState);
      taskSpec.startState = taskSpec.finishedState;
      expect(th.QueueWorker.isValidTaskSpec(taskSpec)).to.be.false;
    });

    it('should accept a taskSpec with the same errorState and startState', () => {
      let taskSpec = _.clone(th.validTaskSpecWithStartState);
      taskSpec.errorState = taskSpec.startState;
      expect(th.QueueWorker.isValidTaskSpec(taskSpec)).to.be.true;
    });

    it('should accept a taskSpec with the same errorState and finishedState', () => {
      let taskSpec = _.clone(th.validTaskSpecWithFinishedState);
      taskSpec.errorState = taskSpec.finishedState;
      expect(th.QueueWorker.isValidTaskSpec(taskSpec)).to.be.true;
    });

    it('should accept a valid task spec with a startState, a finishedState, an errorState, a timeout, and retries', () => {
      expect(th.QueueWorker.isValidTaskSpec(th.validTaskSpecWithEverything)).to.be.true;
    });

    it('should accept a valid basic task spec with null parameters for everything else', () => {
      let taskSpec = _.clone(th.validBasicTaskSpec);
      taskSpec = _.assign(taskSpec, {
        startState: null,
        finishedState: null,
        errorState: null,
        timeout: null,
        retries: null
      });
      expect(th.QueueWorker.isValidTaskSpec(taskSpec)).to.be.true;
    });
  });

  describe('#setTaskSpec', () => {
    let qw;

    afterEach(done => {
      qw.setTaskSpec();
      tasksRef.set(null, done);
    });

    it('should reset the worker when called with an invalid task spec', () => {
      ['', 'foo', NaN, Infinity, true, false, null, undefined, 0, -1, 10, ['foo', 'bar'], { foo: 'bar' }, { foo: 'bar' }, { foo: { bar: { baz: true } } }, _.noop].forEach(invalidTaskSpec => {
        qw = new th.QueueWorkerWithoutProcessingOrTimeouts(tasksRef, '0', true, false, _.noop);
        let oldTaskNumber = qw.taskNumber;
        qw.setTaskSpec(invalidTaskSpec);
        expect(qw.taskNumber).to.not.equal(oldTaskNumber);
        expect(qw.startState).to.be.null;
        expect(qw.inProgressState).to.be.null;
        expect(qw.finishedState).to.be.null;
        expect(qw.taskTimeout).to.be.null;
        expect(qw.newTaskRef).to.be.null;
        expect(qw.newTaskListener).to.be.null;
        expect(qw.expiryTimeouts).to.deep.equal({});
      });
    });

    it('should reset the worker when called with an invalid task spec after a valid task spec', () => {
      ['', 'foo', NaN, Infinity, true, false, null, undefined, 0, -1, 10, ['foo', 'bar'], { foo: 'bar' }, { foo: 'bar' }, { foo: { bar: { baz: true } } }, _.noop].forEach(invalidTaskSpec => {
        qw = new th.QueueWorkerWithoutProcessingOrTimeouts(tasksRef, '0', true, false, _.noop);
        qw.setTaskSpec(th.validBasicTaskSpec);
        let oldTaskNumber = qw.taskNumber;
        qw.setTaskSpec(invalidTaskSpec);
        expect(qw.taskNumber).to.not.equal(oldTaskNumber);
        expect(qw.startState).to.be.null;
        expect(qw.inProgressState).to.be.null;
        expect(qw.finishedState).to.be.null;
        expect(qw.taskTimeout).to.be.null;
        expect(qw.newTaskRef).to.be.null;
        expect(qw.newTaskListener).to.be.null;
        expect(qw.expiryTimeouts).to.deep.equal({});
      });
    });

    it('should reset the worker when called with an invalid task spec after a valid task spec with everythin', () => {
      ['', 'foo', NaN, Infinity, true, false, null, undefined, 0, -1, 10, ['foo', 'bar'], { foo: 'bar' }, { foo: 'bar' }, { foo: { bar: { baz: true } } }, _.noop].forEach(invalidTaskSpec => {
        qw = new th.QueueWorkerWithoutProcessingOrTimeouts(tasksRef, '0', true, false, _.noop);
        qw.setTaskSpec(th.validTaskSpecWithEverything);
        let oldTaskNumber = qw.taskNumber;
        qw.setTaskSpec(invalidTaskSpec);
        expect(qw.taskNumber).to.not.equal(oldTaskNumber);
        expect(qw.startState).to.be.null;
        expect(qw.inProgressState).to.be.null;
        expect(qw.finishedState).to.be.null;
        expect(qw.taskTimeout).to.be.null;
        expect(qw.newTaskRef).to.be.null;
        expect(qw.newTaskListener).to.be.null;
        expect(qw.expiryTimeouts).to.deep.equal({});
      });
    });

    it('should reset a worker when called with a basic valid task spec', () => {
      qw = new th.QueueWorkerWithoutProcessingOrTimeouts(tasksRef, '0', true, false, _.noop);
      let oldTaskNumber = qw.taskNumber;
      qw.setTaskSpec(th.validBasicTaskSpec);
      expect(qw.taskNumber).to.not.equal(oldTaskNumber);
      expect(qw.startState).to.be.null;
      expect(qw.inProgressState).to.equal(th.validBasicTaskSpec.inProgressState);
      expect(qw.finishedState).to.be.null;
      expect(qw.taskTimeout).to.be.null;
      expect(qw.newTaskRef).to.have.property('on').and.be.a('function');
      expect(qw.newTaskListener).to.be.a('function');
      expect(qw.expiryTimeouts).to.deep.equal({});
    });

    it('should reset a worker when called with a valid task spec with a startState', () => {
      qw = new th.QueueWorkerWithoutProcessingOrTimeouts(tasksRef, '0', true, false, _.noop);
      let oldTaskNumber = qw.taskNumber;
      qw.setTaskSpec(th.validTaskSpecWithStartState);
      expect(qw.taskNumber).to.not.equal(oldTaskNumber);
      expect(qw.startState).to.equal(th.validTaskSpecWithStartState.startState);
      expect(qw.inProgressState).to.equal(th.validTaskSpecWithStartState.inProgressState);
      expect(qw.finishedState).to.be.null;
      expect(qw.taskTimeout).to.be.null;
      expect(qw.newTaskRef).to.have.property('on').and.be.a('function');
      expect(qw.newTaskListener).to.be.a('function');
      expect(qw.expiryTimeouts).to.deep.equal({});
    });

    it('should reset a worker when called with a valid task spec with a finishedState', () => {
      qw = new th.QueueWorkerWithoutProcessingOrTimeouts(tasksRef, '0', true, false, _.noop);
      let oldTaskNumber = qw.taskNumber;
      qw.setTaskSpec(th.validTaskSpecWithFinishedState);
      expect(qw.taskNumber).to.not.equal(oldTaskNumber);
      expect(qw.startState).to.be.null;
      expect(qw.inProgressState).to.equal(th.validTaskSpecWithFinishedState.inProgressState);
      expect(qw.finishedState).to.equal(th.validTaskSpecWithFinishedState.finishedState);
      expect(qw.taskTimeout).to.be.null;
      expect(qw.newTaskRef).to.have.property('on').and.be.a('function');
      expect(qw.newTaskListener).to.be.a('function');
      expect(qw.expiryTimeouts).to.deep.equal({});
    });

    it('should reset a worker when called with a valid task spec with a timeout', () => {
      qw = new th.QueueWorkerWithoutProcessingOrTimeouts(tasksRef, '0', true, false, _.noop);
      let oldTaskNumber = qw.taskNumber;
      qw.setTaskSpec(th.validTaskSpecWithTimeout);
      expect(qw.taskNumber).to.not.equal(oldTaskNumber);
      expect(qw.startState).to.be.null;
      expect(qw.inProgressState).to.equal(th.validTaskSpecWithTimeout.inProgressState);
      expect(qw.finishedState).to.be.null;
      expect(qw.taskTimeout).to.equal(th.validTaskSpecWithTimeout.timeout);
      expect(qw.newTaskRef).to.have.property('on').and.be.a('function');
      expect(qw.newTaskListener).to.be.a('function');
      expect(qw.expiryTimeouts).to.deep.equal({});
    });

    it('should reset a worker when called with a valid task spec with everything', () => {
      qw = new th.QueueWorkerWithoutProcessingOrTimeouts(tasksRef, '0', true, false, _.noop);
      let oldTaskNumber = qw.taskNumber;
      qw.setTaskSpec(th.validTaskSpecWithEverything);
      expect(qw.taskNumber).to.not.equal(oldTaskNumber);
      expect(qw.startState).to.equal(th.validTaskSpecWithEverything.startState);
      expect(qw.inProgressState).to.equal(th.validTaskSpecWithEverything.inProgressState);
      expect(qw.finishedState).to.equal(th.validTaskSpecWithEverything.finishedState);
      expect(qw.taskTimeout).to.equal(th.validTaskSpecWithEverything.timeout);
      expect(qw.newTaskRef).to.have.property('on').and.be.a('function');
      expect(qw.newTaskListener).to.be.a('function');
      expect(qw.expiryTimeouts).to.deep.equal({});
    });

    it('should not pick up tasks on the queue not for the current task', done => {
      qw = new th.QueueWorkerWithoutProcessingOrTimeouts(tasksRef, '0', true, false, _.noop);
      qw.setTaskSpec(th.validBasicTaskSpec);
      let spy = sinon.spy(qw, 'tryToProcess');
      tasksRef.once('child_added', () => {
        try {
          expect(qw.tryToProcess).to.not.have.been.called;
          spy.restore();
          done();
        } catch (error) {
          spy.restore();
          done(error);
        }
      });
      tasksRef.push({ '_state': 'other' }, error => {
        if (error) {
          return done(error);
        }
        return undefined;
      });
    });

    it('should pick up tasks on the queue with no "_state" when a task is specified without a startState', done => {
      qw = new th.QueueWorkerWithoutProcessingOrTimeouts(tasksRef, '0', true, false, _.noop);
      qw.setTaskSpec(th.validBasicTaskSpec);
      let spy = sinon.spy(qw, 'tryToProcess');
      let ref = tasksRef.push();
      tasksRef.once('child_added', () => {
        try {
          expect(qw.tryToProcess).to.have.been.calledOnce;
          spy.restore();
          done();
        } catch (error) {
          spy.restore();
          done(error);
        }
      });
      ref.set({ 'foo': 'bar' });
    });

    it('should pick up tasks on the queue with the corresponding "_state" when a task is specifies a startState', done => {
      qw = new th.QueueWorkerWithoutProcessingOrTimeouts(tasksRef, '0', true, false, _.noop);
      qw.setTaskSpec(th.validTaskSpecWithStartState);
      let spy = sinon.spy(qw, 'tryToProcess');
      let ref = tasksRef.push();
      tasksRef.once('child_added', () => {
        try {
          expect(qw.tryToProcess).to.have.been.calledOnce;
          spy.restore();
          done();
        } catch (error) {
          spy.restore();
          done(error);
        }
      });
      ref.set({ '_state': th.validTaskSpecWithStartState.startState });
    });
  });

  describe('#shutdown', () => {
    let qw;
    let callbackStarted;
    let callbackComplete;

    beforeEach(() => {
      callbackStarted = false;
      callbackComplete = false;
      qw = new th.QueueWorker(tasksRef, '0', true, false, (data, progress, resolve) => {
        callbackStarted = true;
        setTimeout(() => {
          callbackComplete = true;
          resolve();
        }, 500);
      });
    });

    afterEach(() => {
      qw.setTaskSpec();
    });

    it('should shutdown a worker not processing any tasks', () => {
      return qw.shutdown().should.eventually.be.fulfilled;
    });

    it('should shutdown a worker after the current task has finished', done => {
      qw.setTaskSpec(th.validBasicTaskSpec);
      tasksRef.push({
        foo: 'bar'
      }, errorA => {
        if (errorA) {
          return done(errorA);
        }
        return setTimeout(() => {
          try {
            expect(callbackStarted).to.be.true;
            expect(callbackComplete).to.be.false;
            qw.shutdown().then(() => {
              expect(callbackComplete).to.be.true;
            }).should.eventually.be.fulfilled.notify(done);
          } catch (errorB) {
            done(errorB);
          }
        }, 500);
      });
    });

    it('should return the same shutdown promise if shutdown is called twice', done => {
      qw.setTaskSpec(th.validBasicTaskSpec);
      tasksRef.push({
        foo: 'bar'
      }, errorA => {
        if (errorA) {
          return done(errorA);
        }
        try {
          let firstPromise = qw.shutdown();
          let secondPromise = qw.shutdown();
          expect(firstPromise).to.deep.equal(secondPromise);
          return done();
        } catch (errorB) {
          return done(errorB);
        }
      });
    });
  });
});
