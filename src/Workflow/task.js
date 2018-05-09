import QueueDB from "./db";
import logger from 'winston';

const patchData = (data, patch = {}) => {
  if (Array.isArray(patch)) {
    return [
      ...(data || []),
      ...patch,
    ];
  } else {
    return {
      ...(data || {}),
      ...patch,
    };
  }
};

const resolvedTaskHandler = async (task, jobData, resolve, taskResults) => {
  const outputData = task.getOutputData(jobData, taskResults);
  if (typeof outputData !== 'undefined') {
    jobData = {
      ...jobData,
      __display__: {
        ...jobData.__display__,
        output: patchData(jobData.__display__.output, outputData),
      },
    };
  }
  resolve({
    ...jobData,
    ...taskResults,
  });
};

const taskHandler = (async (task, handler, jobData, progress, resolve, reject) => {
  try {
    handler(jobData, progress, resolvedTaskHandler.bind(null, task, jobData, resolve), async err => {
      reject(err);
    });
  } catch (ex) {
    logger.error(ex);
    reject(ex);
  }
});

export default class Task {
  constructor({ previousState, jobType, retries, timeout, id, name, outputData, numWorkers }) {
    this.jobType = jobType;
    this.retries = retries;
    this.timeout = timeout;
    this.numWorkers = numWorkers;
    this.id = id;
    this.name = name;
    this.outputData = outputData;
    this.previousState = previousState;
    this.create();
  }

  static createTasks(jobType, tasks = [], defaults = {}) {
    const { timeout: defaultTimeout, retries: defaultRetries, numWorkers: defaultNumWorkers = 1 } = defaults;
    let previousState;
    return tasks.reduce((acc, { retries, timeout, numWorkers, id, ...rest }) => {
      acc[id] = previousState = new Task({
        ...rest,
        id,
        previousState,
        jobType,
        retries: retries || defaultRetries,
        timeout: timeout || defaultTimeout,
        numWorkers: numWorkers || defaultNumWorkers,
      });
      return acc;
    }, {});
  }

  getStateName(action) {
    return `${this.getSpecName()}_${action}`;
  }

  getWorkerCount() {
    return this.numWorkers;
  }

  getSpecName() {
    return `${this.jobType}_${this.id}`;
  }

  getStartState() {
    return this.getStateName('started');
  }

  getInProgressState() {
    return this.getStateName('in_progress');
  }

  getErredState() {
    return this.getStateName('erred');
  }

  getFinishedState() {
    return this.getStateName('finished');
  }

  getHandler(handler) {
    return taskHandler.bind(null, this, handler);
  }

  getOutputData(jobData, taskResults) {
    return typeof this.outputData === 'function' ? this.outputData(jobData, taskResults) : this.outputData;
  }

  create() {
    const startState = typeof this.previousState !== 'undefined' ? this.previousState.getFinishedState() : this.getStartState();
    const specData = {
      start_state: startState,
      in_progress_state: this.getInProgressState(),
      finished_state: this.getFinishedState(),
      error_state: this.getErredState(),
    };
    if (typeof this.retries !== 'undefined') {
      specData.retries = this.retries;
    }
    if (typeof this.timeout !== 'undefined') {
      specData.timeout = this.timeout;
    }
    return QueueDB.getSpecRef(this.getSpecName()).set(specData);
  }

};