import logger from 'winston';
import uuid from 'uuid';
import _ from 'lodash';
import RSVP from 'rsvp';

const MAX_TRANSACTION_ATTEMPTS = 10;
const DEFAULT_ERROR_STATE = 'error';
const DEFAULT_RETRIES = 0;

const SERVER_TIMESTAMP = {
  '.sv': 'timestamp',
};

const getKey = snapshot => _.isFunction(snapshot.key) ? snapshot.key() : snapshot.key;
const getRef = snapshot => _.isFunction(snapshot.ref) ? snapshot.ref() : snapshot.ref;

/**
 * @param {firebase.database.Reference} tasksRef the Firebase Realtime Database
 *   reference for queue tasks.
 * @param {String} processId the ID of the current worker process.
 * @param {Function} processingFunction the function to be called each time a
 *   task is claimed.
 * @return {Object}
 */
export default class Worker {
  constructor(tasksRef, queueId, sanitize, suppressStack, processingFunction) {
    if (_.isUndefined(tasksRef)) {
      Worker.throwWorkerError('No tasks reference provided.');
    }
    if (!_.isString(queueId)) {
      Worker.throwWorkerError('Invalid process ID provided.');
    }
    if (!_.isBoolean(sanitize)) {
      Worker.throwWorkerError('Invalid sanitize option.');
    }
    if (!_.isBoolean(suppressStack)) {
      Worker.throwWorkerError('Invalid suppressStack option.');
    }
    if (!_.isFunction(processingFunction)) {
      Worker.throwWorkerError('No processing function provided.');
    }
    this.queueId = queueId;
    this.workerId = this.getWorkerId();
    this.shutdownDeferred = null;

    this.processingFunction = processingFunction;
    this.expiryTimeouts = {};
    this.owners = {};

    this.tasksRef = tasksRef;
    this.processingTasksRef = null;
    this.currentTaskRef = null;
    this.newTaskRef = null;

    this.currentTaskListener = null;
    this.newTaskListener = null;
    this.processingTaskAddedListener = null;
    this.processingTaskRemovedListener = null;

    this.busy = false;
    this.taskNumber = 0;
    this.errorState = DEFAULT_ERROR_STATE;
    this.sanitize = sanitize;
    this.suppressStack = suppressStack;
  }

  static throwWorkerError(error) {
    logger.debug(`QueueWorker(): ${error}`);
    throw new Error(error);
  }

  getWorkerId() {
    return `${this.queueId}:${uuid.v4()}`;
  }

  getProcessId(taskNumber) {
    return `${this.workerId}:${typeof taskNumber !== 'undefined' ? taskNumber : this.taskNumber}`;
  }

  /**
   * Logs an info message with a worker-specific prefix.
   * @param {String} message The message to log.
   */
  getLogEntry(message) {
    return `QueueWorker ${this.workerId} ${message}`;
  };

  updateWfStatus(task, status) {
    task.__wfstatus__ = status;
    if (!_.isUndefined(task.__index__)) {
      task.__index_wfstatus__ = `${task.__index__}:${task.__wfstatus__}`;
    }
    return task;
  }

  /**
   * Returns the state of a task to the start state.
   * @param {firebase.database.Reference} taskRef Firebase Realtime Database
   *   reference to the Firebase location of the task that's timed out.
   * @param {Boolean} immediate Whether this is an immediate update to a task we
   *   expect this worker to own, or whether it's a timeout reset that we don't
   *   necessarily expect this worker to own.
   * @returns {Promise} Whether the task was able to be reset.
   */
  resetTask(taskRef, immediate, deferred) {
    let retries = 0;

    /* istanbul ignore else */
    if (_.isUndefined(deferred)) {
      deferred = RSVP.defer();
    }

    taskRef.transaction(task => {
      /* istanbul ignore if */
      if (_.isNull(task)) {
        return task;
      }
      const id = this.getProcessId();
      const correctState = (task._state === this.inProgressState);
      const correctOwner = (task._owner === id || !immediate);
      const timeSinceUpdate = Date.now() - _.get(task, '_state_changed', 0);
      const timedOut = ((this.taskTimeout && timeSinceUpdate > this.taskTimeout) || immediate);
      if (correctState && correctOwner && timedOut) {
        task._state = this.startState;
        if (this.isWorkflowTask && this.isFirstTask) {
          task = this.updateWfStatus(task, 0);
        }
        task._state_changed = SERVER_TIMESTAMP;
        task._owner = null;
        task._progress = null;
        task._error_details = null;
        return task;
      }
      return undefined;
    }, (error, committed, snapshot) => {
      /* istanbul ignore if */
      if (error) {
        if (++retries < MAX_TRANSACTION_ATTEMPTS) {
          logger.debug(this.getLogEntry('reset task errored, retrying'), error);
          setImmediate(::this.resetTask, taskRef, immediate, deferred);
        } else {
          const errorMsg = 'reset task errored too many times, no longer retrying';
          logger.debug(this.getLogEntry(errorMsg), error);
          deferred.reject(new Error(errorMsg));
        }
      } else {
        if (committed && snapshot.exists()) {
          logger.debug(this.getLogEntry('reset ' + getKey(snapshot)));
        }
        deferred.resolve();
      }
    }, false);

    return deferred.promise;
  }

  /**
   * Creates a resolve callback function, storing the current task number.
   * @param {Number} taskNumber the current task number
   * @returns {Function} the resolve callback function.
   */
  resolve(taskNumber) {
    let retries = 0;
    const deferred = RSVP.defer();

    /*
     * Resolves the current task and changes the state to the finished state.
     * @param {Object} newTask The new data to be stored at the location.
     * @returns {RSVP.Promise} Whether the task was able to be resolved.
     */
    const _resolve = newTask => {
      if ((taskNumber !== this.taskNumber) || _.isNull(this.currentTaskRef)) {
        if (_.isNull(this.currentTaskRef)) {
          logger.debug(this.getLogEntry('Can\'t resolve task - no task ' +
            'currently being processed'));
        } else {
          logger.debug(this.getLogEntry('Can\'t resolve task - no longer ' +
            'processing current task'));
        }
        deferred.resolve();
        this.busy = false;
        this.tryToProcess();
      } else {
        let existedBefore;
        this.currentTaskRef.transaction(task => {
          existedBefore = true;
          if (_.isNull(task)) {
            existedBefore = false;
            return task;
          }
          let id = this.getProcessId();
          if (task._state === this.inProgressState && task._owner === id) {
            let outputTask = _.clone(newTask);
            if (!_.isPlainObject(outputTask)) {
              outputTask = {};
            }
            outputTask._state = _.get(outputTask, '_new_state');
            delete outputTask._new_state;
            if (!_.isNull(outputTask._state) && !_.isString(outputTask._state)) {
              if (_.isNull(this.finishedState) || outputTask._state === false) {
                // Remove the item if no `finished_state` set in the spec or
                // _new_state is explicitly set to `false`.
                return null;
              }
              outputTask._state = this.finishedState;
            }
            outputTask._state_changed = SERVER_TIMESTAMP;
            outputTask._owner = null;
            outputTask._progress = 100;
            outputTask._error_details = null;
            if (this.isWorkflowTask && this.isLastTask) {
              outputTask = this.updateWfStatus(outputTask, 1);
            }
            return outputTask;
          }
          return undefined;
        }, (error, committed, snapshot) => {
          /* istanbul ignore if */
          if (error) {
            if (++retries < MAX_TRANSACTION_ATTEMPTS) {
              logger.debug(this.getLogEntry('resolve task errored, retrying'),
                error);
              setImmediate(_resolve, newTask);
            } else {
              let errorMsg = 'resolve task errored too many times, no longer ' +
                'retrying';
              logger.debug(this.getLogEntry(errorMsg), error);
              deferred.reject(new Error(errorMsg));
            }
          } else {
            if (committed && existedBefore) {
              logger.debug(this.getLogEntry('completed ' + getKey(snapshot)));
            } else {
              logger.debug(this.getLogEntry('Can\'t resolve task - current ' +
                'task no longer owned by this process'));
            }
            deferred.resolve();
            this.busy = false;
            this.tryToProcess();
          }
        }, false);
      }

      return deferred.promise;
    };
    return _resolve;
  }

  /**
   * Creates a reject callback function, storing the current task number.
   * @param {Number} taskNumber the current task number
   * @returns {Function} the reject callback function.
   */
  reject(taskNumber) {
    let retries = 0;
    let errorString = null;
    let errorStack = null;
    const deferred = RSVP.defer();

    /**
     * Rejects the current task and changes the state to this.errorState,
     * adding additional data to the '_error_details' sub key.
     * @param {Object} error The error message or object to be logged.
     * @returns {RSVP.Promise} Whether the task was able to be rejected.
     */
    const _reject = error => {
      if ((taskNumber !== this.taskNumber) || _.isNull(this.currentTaskRef)) {
        if (_.isNull(this.currentTaskRef)) {
          logger.debug(this.getLogEntry('Can\'t reject task - no task ' +
            'currently being processed'));
        } else {
          logger.debug(this.getLogEntry('Can\'t reject task - no longer ' +
            'processing current task'));
        }
        deferred.resolve();
        this.busy = false;
        this.tryToProcess();
      } else {
        if (_.isError(error)) {
          errorString = error.message;
        } else if (_.isString(error)) {
          errorString = error;
        } else if (!_.isUndefined(error) && !_.isNull(error)) {
          errorString = error.toString();
        }

        if (!this.suppressStack) {
          errorStack = _.get(error, 'stack', null);
        }

        let existedBefore;
        this.currentTaskRef.transaction(task => {
          existedBefore = true;
          if (_.isNull(task)) {
            existedBefore = false;
            return task;
          }
          const id = this.getProcessId();
          if (task._state === this.inProgressState &&
            task._owner === id) {
            let attempts = 0;
            const currentAttempts = _.get(task, '_error_details.attempts', 0);
            const currentPrevState = _.get(task, '_error_details.previous_state');
            if (currentAttempts > 0 &&
              currentPrevState === this.inProgressState) {
              attempts = currentAttempts;
            }
            if (attempts >= this.taskRetries) {
              task._state = this.errorState;
              if (this.isWorkflowTask) {
                task = this.updateWfStatus(task, -1);
              }
            } else {
              task._state = this.startState;
              if (this.isWorkflowTask && this.isFirstTask) {
                task = this.updateWfStatus(task, 0);
              }
            }
            task._state_changed = SERVER_TIMESTAMP;
            task._owner = null;
            task._error_details = {
              previous_state: this.inProgressState,
              error: errorString,
              error_stack: errorStack,
              attempts: attempts + 1
            };
            return task;
          }
          return undefined;
        }, (transactionError, committed, snapshot) => {
          /* istanbul ignore if */
          if (transactionError) {
            if (++retries < MAX_TRANSACTION_ATTEMPTS) {
              logger.debug(this.getLogEntry('reject task errored, retrying'),
                transactionError);
              setImmediate(_reject, error);
            } else {
              const errorMsg = 'reject task errored too many times, no longer ' +
                'retrying';
              logger.debug(this.getLogEntry(errorMsg), transactionError);
              deferred.reject(new Error(errorMsg));
            }
          } else {
            if (committed && existedBefore) {
              logger.debug(this.getLogEntry('errored while attempting to ' +
                'complete ' + getKey(snapshot)));
            } else {
              logger.debug(this.getLogEntry('Can\'t reject task - current task' +
                ' no longer owned by this process'));
            }
            deferred.resolve();
            this.busy = false;
            this.tryToProcess();
          }
        }, false);
      }
      return deferred.promise;
    };

    return _reject;
  }

  /**
   * Creates an update callback function, storing the current task number.
   * @param {Number} taskNumber the current task number
   * @returns {Function} the update callback function.
   */
  updateProgress(taskNumber) {
    let errorMsg;

    /**
     * Updates the progress state of the task.
     * @param {Number} progress The progress to report.
     * @returns {RSVP.Promise} Whether the progress was updated.
     */
    return progress => {
      if (!_.isNumber(progress) || _.isNaN(progress) || progress < 0 || progress > 100) {
        return RSVP.reject(new Error('Invalid progress'));
      }
      if ((taskNumber !== this.taskNumber) || _.isNull(this.currentTaskRef)) {
        errorMsg = 'Can\'t update progress - no task currently being processed';
        logger.debug(this.getLogEntry(errorMsg));
        return RSVP.reject(new Error(errorMsg));
      }
      return new RSVP.Promise((resolve, reject) => {
        this.currentTaskRef.transaction(task => {
          /* istanbul ignore if */
          if (_.isNull(task)) {
            return task;
          }
          const id = this.getProcessId();
          if (task._state === this.inProgressState &&
            task._owner === id) {
            task._progress = progress;
            return task;
          }
          return undefined;
        }, (transactionError, committed, snapshot) => {
          /* istanbul ignore if */
          if (transactionError) {
            errorMsg = 'errored while attempting to update progress';
            logger.debug(this.getLogEntry(errorMsg), transactionError);
            return reject(new Error(errorMsg));
          }
          if (committed && snapshot.exists()) {
            return resolve();
          }
          errorMsg = 'Can\'t update progress - current task no longer owned ' +
            'by this process';
          logger.debug(this.getLogEntry(errorMsg));
          return reject(new Error(errorMsg));
        }, false);
      });
    };
  }

  claimWfTaskToProcess(deferred, progress, resolve, reject) {
    let retries = 0;

    const claimWf = () => {
      this.currentTaskRef.transaction(task => {
        if (_.isNull(task)) {
          return task;
        }
        task = this.updateWfStatus(task, 10);
        return task;
      }, (error, committed, snapshot) => {
        if (error) {
          if (++retries < MAX_TRANSACTION_ATTEMPTS) {
            logger.debug(this.getLogEntry('errored while attempting to claim a new WF task, retrying'), error);
            return setImmediate(claimWf);
          }
          const errorMsg = 'errored while attempting to claim a new WF task too many times, no longer retrying';
          logger.debug(this.getLogEntry(errorMsg), error);
          return deferred.reject(new Error(errorMsg));
        } else if (committed && snapshot.exists()) {
          setImmediate(() => {
            try {
              this.processingFunction.call(null, snapshot.val(), progress, resolve, reject);
            } catch (err) {
              reject(err);
            }
          });
        }
      }, false);
    };

    claimWf();
  }

  tryToProcess(deferred) {
    let retries = 0;
    let malformed = false;

    /* istanbul ignore else */
    if (_.isUndefined(deferred)) {
      deferred = RSVP.defer();
    }

    if (!this.busy) {
      if (!_.isNull(this.shutdownDeferred)) {
        deferred.reject(new Error('Shutting down - can no longer process new ' +
          'tasks'));
        this.setTaskSpec(null);
        logger.debug(this.getLogEntry('finished shutdown'));
        this.shutdownDeferred.resolve();
      } else {
        if (!this.newTaskRef) {
          deferred.resolve();
        } else {
          this.newTaskRef.once('value', taskSnap => {
            if (!taskSnap.exists()) {
              return deferred.resolve();
            }
            let nextTaskRef;
            taskSnap.forEach(childSnap => {
              nextTaskRef = getRef(childSnap);
            });
            return nextTaskRef.transaction(task => {
              /* istanbul ignore if */
              if (_.isNull(task)) {
                return task;
              }
              if (!_.isPlainObject(task)) {
                malformed = true;
                const error = new Error('Task was malformed');
                let errorStack = null;
                if (!this.suppressStack) {
                  errorStack = error.stack;
                }
                return {
                  _state: this.errorState,
                  _state_changed: SERVER_TIMESTAMP,
                  _error_details: {
                    error: error.message,
                    original_task: task,
                    error_stack: errorStack
                  }
                };
              }
              if (_.isUndefined(task._state)) {
                task._state = null;
              }
              if (task._state === this.startState) {
                task._state = this.inProgressState;
                task._state_changed = SERVER_TIMESTAMP;
                task._owner = this.getProcessId(this.taskNumber + 1);
                task._progress = 0;
                return task;
              }
              logger.debug(this.getLogEntry(`task no longer in correct state: expected ${this.startState}, got ${task._state}`));
              return undefined;
            }, (error, committed, snapshot) => {
              /* istanbul ignore if */
              if (error) {
                if (++retries < MAX_TRANSACTION_ATTEMPTS) {
                  logger.debug(this.getLogEntry('errored while attempting to ' +
                    'claim a new task, retrying'), error);
                  return setImmediate(::this.tryToProcess, deferred);
                }
                const errorMsg = 'errored while attempting to claim a new task ' +
                  'too many times, no longer retrying';
                logger.debug(this.getLogEntry(errorMsg), error);
                return deferred.reject(new Error(errorMsg));
              } else if (committed && snapshot.exists()) {
                if (malformed) {
                  logger.debug(this.getLogEntry('found malformed entry ' +
                    getKey(snapshot)));
                } else {
                  /* istanbul ignore if */
                  if (this.busy) {
                    // Worker has become busy while the transaction was processing
                    // so give up the task for now so another worker can claim it
                    this.resetTask(nextTaskRef, true);
                  } else {
                    this.busy = true;
                    this.taskNumber += 1;
                    logger.debug(this.getLogEntry('claimed ' + getKey(snapshot)));
                    this.currentTaskRef = getRef(snapshot);
                    this.currentTaskListener = this.currentTaskRef
                      .child('_owner')
                      .on('value', ownerSnapshot => {
                        const id = this.getProcessId(this.taskNumber);
                        /* istanbul ignore else */
                        if (ownerSnapshot.val() !== id &&
                          !_.isNull(this.currentTaskRef) &&
                          !_.isNull(this.currentTaskListener)) {
                          this.currentTaskRef.child('_owner').off(
                            'value',
                            this.currentTaskListener);
                          this.currentTaskRef = null;
                          this.currentTaskListener = null;
                        }
                      });
                    const data = snapshot.val();
                    if (this.sanitize) {
                      [
                        '_state',
                        '_state_changed',
                        '_owner',
                        '_progress',
                        '_error_details'
                      ].forEach(reserved => {
                        if (snapshot.hasChild(reserved)) {
                          delete data[reserved];
                        }
                      });
                    } else {
                      data._id = getKey(snapshot);
                    }
                    const progress = this.updateProgress(this.taskNumber);
                    const resolve = this.resolve(this.taskNumber);
                    const reject = this.reject(this.taskNumber);
                    if (!this.isWorkflowTask) {
                      setImmediate(() => {
                        try {
                          this.processingFunction.call(null, data, progress, resolve, reject);
                        } catch (err) {
                          reject(err);
                        }
                      });
                    } else {
                      this.claimWfTaskToProcess(deferred, progress, resolve, reject);
                    }
                  }
                }
              }
              return deferred.resolve();
            }, false);
          });
        }
      }
    } else {
      deferred.resolve();
    }

    return deferred.promise;
  }

  setupTimeouts() {
    if (!_.isNull(this.processingTaskAddedListener)) {
      this.processingTasksRef.off(
        'child_added',
        this.processingTaskAddedListener);
      this.processingTaskAddedListener = null;
    }
    if (!_.isNull(this.processingTaskRemovedListener)) {
      this.processingTasksRef.off(
        'child_removed',
        this.processingTaskRemovedListener);
      this.processingTaskRemovedListener = null;
    }

    _.forEach(this.expiryTimeouts, expiryTimeout => {
      clearTimeout(expiryTimeout);
    });
    this.expiryTimeouts = {};
    this.owners = {};

    if (this.taskTimeout) {
      this.processingTasksRef = this.tasksRef.orderByChild('_state')
        .equalTo(this.inProgressState);

      const setUpTimeout = snapshot => {
        const taskName = getKey(snapshot);
        const now = new Date().getTime();
        const startTime = (snapshot.child('_state_changed').val() || now);
        const expires = Math.max(0, startTime - now + this.taskTimeout);
        const ref = getRef(snapshot);
        this.owners[taskName] = snapshot.child('_owner').val();
        this.expiryTimeouts[taskName] = setTimeout(
          ::this.resetTask,
          expires,
          ref, false);
      };

      this.processingTaskAddedListener = this.processingTasksRef.on('child_added',
        setUpTimeout,
        /* istanbul ignore next */ error => {
          logger.debug(this.getLogEntry('errored listening to Firebase'), error);
        });
      this.processingTaskRemovedListener = this.processingTasksRef.on(
        'child_removed',
        snapshot => {
          const taskName = getKey(snapshot);
          clearTimeout(this.expiryTimeouts[taskName]);
          delete this.expiryTimeouts[taskName];
          delete this.owners[taskName];
        }, /* istanbul ignore next */ error => {
          logger.debug(this.getLogEntry('errored listening to Firebase'), error);
        });
      this.processingTasksRef.on('child_changed', snapshot => {
        // This catches de-duped events from the server - if the task was removed
        // and added in quick succession, the server may squash them into a
        // single update
        const taskName = getKey(snapshot);
        if (snapshot.child('_owner').val() !== this.owners[taskName]) {
          setUpTimeout(snapshot);
        }
      }, /* istanbul ignore next */ error => {
        logger.debug(this.getLogEntry('errored listening to Firebase'), error);
      });
    } else {
      this.processingTasksRef = null;
    }
  }

  static isValidTaskSpec(taskSpec) {
    if (!_.isPlainObject(taskSpec)) {
      return false;
    }
    if (!_.isString(taskSpec.inProgressState)) {
      return false;
    }
    if (!_.isUndefined(taskSpec.startState) &&
      !_.isNull(taskSpec.startState) &&
      (
        !_.isString(taskSpec.startState) ||
        taskSpec.startState === taskSpec.inProgressState
      )) {
      return false;
    }
    if (!_.isUndefined(taskSpec.finishedState) &&
      !_.isNull(taskSpec.finishedState) &&
      (
        !_.isString(taskSpec.finishedState) ||
        taskSpec.finishedState === taskSpec.inProgressState ||
        taskSpec.finishedState === taskSpec.startState
      )) {
      return false;
    }
    if (!_.isUndefined(taskSpec.errorState) &&
      !_.isNull(taskSpec.errorState) &&
      (
        !_.isString(taskSpec.errorState) ||
        taskSpec.errorState === taskSpec.inProgressState
      )) {
      return false;
    }
    if (!_.isUndefined(taskSpec.timeout) &&
      !_.isNull(taskSpec.timeout) &&
      (
        !_.isNumber(taskSpec.timeout) ||
        taskSpec.timeout <= 0 ||
        taskSpec.timeout % 1 !== 0
      )) {
      return false;
    }
    return !(!_.isUndefined(taskSpec.retries) &&
      !_.isNull(taskSpec.retries) &&
      (
        !_.isNumber(taskSpec.retries) ||
        taskSpec.retries < 0 ||
        taskSpec.retries % 1 !== 0
      ));
  }

  setTaskSpec(taskSpec) {
    // Increment the taskNumber so that a task being processed before the change
    // doesn't continue to use incorrect data
    this.taskNumber += 1;

    if (!_.isNull(this.newTaskListener)) {
      this.newTaskRef.off('child_added', this.newTaskListener);
    }

    if (!_.isNull(this.currentTaskListener)) {
      this.currentTaskRef.child('_owner').off(
        'value',
        this.currentTaskListener);
      this.resetTask(this.currentTaskRef, true);
      this.currentTaskRef = null;
      this.currentTaskListener = null;
    }

    if (Worker.isValidTaskSpec(taskSpec)) {
      const { startState = null, inProgressState, finishedState = null, errorState = DEFAULT_ERROR_STATE, timeout = null, retries = DEFAULT_RETRIES, isWorkflowTask = false, isFirstTask = false, isLastTask = false } = taskSpec;
      this.isFirstTask = isFirstTask;
      this.isLastTask = isLastTask;
      this.isWorkflowTask = isWorkflowTask;
      this.startState = startState;
      this.inProgressState = inProgressState;
      this.finishedState = finishedState;
      this.errorState = errorState;
      this.taskTimeout = timeout;
      this.taskRetries = retries;
      this.newTaskRef = this.tasksRef
        .orderByChild('_state')
        .equalTo(this.startState)
        .limitToFirst(1);
      logger.debug(this.getLogEntry('listening'));
      this.newTaskListener = this.newTaskRef.on('child_added', () => this.tryToProcess(),
        /* istanbul ignore next */ error => logger.debug(this.getLogEntry('errored listening to Firebase'), error));
    } else {
      logger.debug(this.getLogEntry('invalid task spec, not listening for new tasks'));
      this.isFirstTask = false;
      this.isLastTask = false;
      this.isWorkflowTask = false;
      this.startState = null;
      this.inProgressState = null;
      this.finishedState = null;
      this.errorState = DEFAULT_ERROR_STATE;
      this.taskTimeout = null;
      this.taskRetries = DEFAULT_RETRIES;
      this.newTaskRef = null;
      this.newTaskListener = null;
    }
    this.setupTimeouts();
  }

  shutdown() {
    if (!_.isNull(this.shutdownDeferred)) {
      return this.shutdownDeferred.promise;
    }

    logger.debug(this.getLogEntry('shutting down'));

    // Set the global shutdown deferred promise, which signals we're shutting down
    this.shutdownDeferred = RSVP.defer();

    // We can report success immediately if we're not busy
    if (!this.busy) {
      this.setTaskSpec(null);
      logger.debug(this.getLogEntry('finished shutdown'));
      this.shutdownDeferred.resolve();
    }

    return this.shutdownDeferred.promise;
  }
}