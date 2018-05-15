import QueueDB from "./QueueDB";
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
  let jobDataPatch = {
    ...taskResults,
  };
  if (typeof outputData !== 'undefined') {
    jobDataPatch = {
      ...jobDataPatch,
      __display__: {
        ...jobData.__display__,
        output: patchData(jobData.__display__.output, outputData),
      },
    };
  }
  return resolve(jobDataPatch, true);
};

const taskHandler = async (task, handler, jobData, progress, resolve, reject) => {
  try {
    return handler(jobData, progress, resolvedTaskHandler.bind(null, task, jobData, resolve), async err => {
      reject(err);
    });
  } catch (ex) {
    logger.error(ex);
    reject(ex);
  }
};

export default class Task {
  constructor({ app, previousState, isFirstTask, isLastTask, jobType, retries, timeout, id, name, outputData, numWorkers }) {
    this.jobType = jobType;
    this.retries = retries;
    this.timeout = timeout;
    this.numWorkers = numWorkers;
    this.id = id;
    this.name = name;
    this.app = app;
    this.outputData = outputData;
    this.previousState = previousState;
    this.isFirstTask = isFirstTask;
    this.isLastTask = isLastTask;
    this.create();
  }

  static createTasks(app, jobType, tasks = [], defaults = {}) {
    const { timeout: defaultTimeout, retries: defaultRetries, numWorkers: defaultNumWorkers = 1 } = defaults;
    let previousState;
    return tasks.reduce((acc, { retries, timeout, numWorkers, id, ...rest }, index) => {
      acc[id] = previousState = new Task({
        ...rest,
        id,
        previousState,
        app,
        jobType,
        retries: retries || defaultRetries,
        timeout: timeout || defaultTimeout,
        numWorkers: numWorkers || defaultNumWorkers,
        isLastTask: index === (tasks.length - 1),
        isFirstTask: index === 0,
      });
      return acc;
    }, {});
  }

  getStateName(action) {
    return `${this.id}_${action}`;
  }

  getWorkerCount() {
    return this.numWorkers;
  }

  getId() {
    return this.id;
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
      isWorkflowTask: true,
      isFirstTask: this.isFirstTask,
      isLastTask: this.isLastTask,
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
    return QueueDB.getSpecRef(this.app, this.jobType, this.id).set(specData);
  }

};