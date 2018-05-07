import _ from 'lodash';
import logger from './utils/logger';
import QueueWorker from './Worker';

const DEFAULT_NUM_WORKERS = 1;
const DEFAULT_SANITIZE = true;
const DEFAULT_SUPPRESS_STACK = false;
const DEFAULT_TASK_SPEC = {
  inProgressState: 'in_progress',
  timeout: 300000 // 5 minutes
};


/**
 * @constructor
 * @param {firebase.database.Reference|Object} ref A Firebase Realtime Database
 *  reference to the queue or an object containing both keys:
 *     - tasksRef: {firebase.database.Reference} A Firebase Realtime Database
 *         reference to the queue tasks location.
 *     - specsRef: {firebase.database.Reference} A Firebase Realtime Database
 *         reference to the queue specs location.
 * @param {Object} options (optional) Object containing possible keys:
 *     - specId: {String} the task specification ID for the workers.
 *     - numWorkers: {Number} The number of workers to create for this task.
 *     - sanitize: {Boolean} Whether to sanitize the 'data' passed to the
 *         processing function of internal queue keys.
 * @param {Function} processingFunction A function that is called each time to
 *   process a task. This function is passed four parameters:
 *     - data {Object} The current data at the location.
 *     - progress {Function} A function to update the progress percent of the
 *         task for informational purposes. Pass it a number between 0 and 100.
 *         Returns a promise of whether the operation was completed
 *         successfully.
 *     - resolve {Function} An asychronous callback function - call this
 *         function when the processingFunction completes successfully. This
 *         takes an optional Object parameter that, if passed, will overwrite
 *         the data at the task location, and returns a promise of whether the
 *         operation was successful.
 *     - reject {Function} An asynchronous callback function - call this
 *         function if the processingFunction encounters an error. This takes
 *         an optional String or Object parameter that will be stored in the
 *         '_error_details/error' location in the task and returns a promise
 *         of whether the operation was successful.
 * @returns {Object} The new Queue object.
 */

export default class Queue {
  constructor() {
    const constructorArguments = arguments;

    this.numWorkers = DEFAULT_NUM_WORKERS;
    this.sanitize = DEFAULT_SANITIZE;
    this.suppressStack = DEFAULT_SUPPRESS_STACK;
    this.initialized = false;
    this.shuttingDown = false;
    this.specChangeListener = null;

    if (constructorArguments.length < 2) {
      Queue.throwInitializeError('Queue must at least have the queueRef and processingFunction arguments.');
    } else if (constructorArguments.length === 2) {
      this.processingFunction = constructorArguments[1];
    } else if (constructorArguments.length === 3) {
      const options = constructorArguments[1];
      if (!_.isPlainObject(options)) {
        Queue.throwInitializeError('Options parameter must be a plain object.');
      }
      this.backoffConf = options.backoff;
      this.setSpecId(options.specId);
      this.setNumWorkers(options.numWorkers);
      this.setSanitize(options.sanitize);
      this.setSuppressStack(options.suppressStack);
      this.processingFunction = constructorArguments[2];
    } else {
      Queue.throwInitializeError('Queue can only take at most three arguments - queueRef, options (optional), and processingFunction.');
    }

    this.setRefs(constructorArguments[0]);
    this.setWorkers();
    this.setSpecListener();
  }

  static throwInitializeError(error) {
    logger.debug('Queue(): Error during initialization', error);
    throw new Error(error);
  }

  setRefs(ref) {
    if (_.has(ref, 'tasksRef') && _.has(ref, 'specsRef')) {
      this.tasksRef = ref.tasksRef;
      this.specsRef = ref.specsRef;
    } else if (_.isPlainObject(ref)) {
      Queue.throwInitializeError('When ref is an object it must contain both keys \'tasksRef\' and \'specsRef\'');
    } else {
      this.tasksRef = ref.child('tasks');
      this.specsRef = ref.child('specs');
    }
  }

  setSpecId(specId) {
    if (!_.isUndefined(specId)) {
      if (_.isString(specId)) {
        this.specId = specId;
      } else {
        Queue.throwInitializeError('options.specId must be a String.');
      }
    }
  }

  setNumWorkers(numWorkers) {
    if (!_.isUndefined(numWorkers)) {
      if (_.isNumber(numWorkers) && numWorkers > 0 && numWorkers % 1 === 0) {
        this.numWorkers = numWorkers;
      } else {
        Queue.throwInitializeError('options.numWorkers must be a positive integer.');
      }
    }
  }

  setSanitize(sanitize) {
    if (!_.isUndefined(sanitize)) {
      if (_.isBoolean(sanitize)) {
        this.sanitize = sanitize;
      } else {
        Queue.throwInitializeError('options.sanitize must be a boolean.');
      }
    }
  }

  setSuppressStack(suppressStack) {
    if (!_.isUndefined(suppressStack)) {
      if (_.isBoolean(suppressStack)) {
        this.suppressStack = suppressStack;
      } else {
        Queue.throwInitializeError('options.suppressStack must be a boolean.');
      }
    }
  }

  getQueueId() {
    return `${this.specId ? `${this.specId}:` : ''}${this.workers.length}`;
  }

  setWorkers() {
    this.workers = [];
    for (let i = 0; i < this.numWorkers; i++) {
      this.workers.push(new QueueWorker(
        this.tasksRef,
        this.getQueueId(),
        this.sanitize,
        this.suppressStack,
        this.processingFunction,
        this.backoffConf,
      ));
    }
  }

  setSpecListener() {
    if (_.isUndefined(this.specId)) {
      this.setWorkersTaskSpec(DEFAULT_TASK_SPEC);
      this.initialized = true;
    } else {
      this.specChangeListener = this.specsRef
        .child(this.specId)
        .on('value', taskSpecSnap => {
          const taskSpec = {
            startState: taskSpecSnap.child('start_state').val(),
            inProgressState: taskSpecSnap.child('in_progress_state').val(),
            finishedState: taskSpecSnap.child('finished_state').val(),
            errorState: taskSpecSnap.child('error_state').val(),
            timeout: taskSpecSnap.child('timeout').val(),
            retries: taskSpecSnap.child('retries').val()
          };
          this.setWorkersTaskSpec(taskSpec);
          this.currentTaskSpec = taskSpec;
          this.initialized = true;
        }, /* istanbul ignore next */ err => {
          logger.debug('Queue(): Error connecting to Firebase reference',
            err.message);
        });
    }
  }

  setWorkersTaskSpec(taskSpec) {
    this.workers.forEach(worker => worker.setTaskSpec(taskSpec));
  }

  /**
   * Gracefully shuts down a queue.
   * @returns {Promise} A promise fulfilled when all the worker processes
   *   have finished their current tasks and are no longer listening for new ones.
   */
  shutdown() {
    this.shuttingDown = true;
    logger.debug('Queue: Shutting down');
    if (!_.isNull(this.specChangeListener)) {
      this.specsRef.child(this.specId).off('value', this.specChangeListener);
      this.specChangeListener = null;
    }
    return Promise.all(this.workers.map(worker => worker.shutdown()));
  }

  /**
   * Gets queue worker count.
   * @returns {Number} Total number of workers for this queue.
   */
  getWorkerCount() {
    return this.workers.length;
  };

  /**
   * Adds a queue worker.
   * @returns {QueueWorker} the worker created.
   */
  addWorker() {
    if (this.shuttingDown) {
      throw new Error('Cannot add worker while queue is shutting down');
    }

    logger.debug('Queue: adding worker');
    const worker = new QueueWorker(
      this.tasksRef,
      this.getQueueId(this.workers.length),
      this.sanitize,
      this.suppressStack,
      this.processingFunction,
      this.backoffConf
    );
    this.workers.push(worker);

    if (_.isUndefined(this.specId)) {
      worker.setTaskSpec(DEFAULT_TASK_SPEC);
      // if the currentTaskSpec is not yet set it will be called once it's fetched by the specChangeListener
    } else if (!_.isUndefined(this.currentTaskSpec)) {
      worker.setTaskSpec(this.currentTaskSpec);
    }

    return worker;
  }

  /**
   * Shutdowns a queue worker if one exists.
   * @returns {Promise} A promise fulfilled once the worker is shutdown
   *   or rejected if there are no workers left to shutdown.
   */
  shutdownWorker() {
    const worker = this.workers.pop();

    if (_.isUndefined(worker)) {
      return Promise.reject(new Error('No workers to shutdown'));
    } else {
      logger.debug('Queue: shutting down worker');
      return worker.shutdown();
    }
  }
}