import Queue from '../Queue';
import QueueDB from './db';
import getRoutes from './routes';
import Task from "./task";
import logger from '../utils/logger';

export default class Job {
  static initialized = false;
  static queues = {};

  constructor({ app, type = '', tasks = [], timeout, retries, numWorkers = 1, inputData, backoff }) {
    if (typeof app !== 'string' || app.length === 0) {
      throw new Error('Please provide a valid app');
    }
    if (typeof type !== 'string' || type.length === 0) {
      throw new Error('Please provide a job type');
    }
    if (tasks.length === 0) {
      tasks = [{
        id: type,
      }];
    }
    if (!Array.isArray(tasks)) {
      tasks = [tasks];
    }
    this.type = type;
    this.app = app;
    this.backoff = backoff;
    this.tasks = Task.createTasks(this.type, tasks, {
      timeout,
      retries,
      numWorkers,
    });
    this.startTask = this.tasks[tasks[0].id];
    this.inputData = inputData;
  }

  static initialize({ firebase }) {
    try {
      QueueDB.initialize(firebase);
      Job.initialized = true;
    } catch (ex) {
      throw ex;
    }
  }

  static getTaskCount(state) {
    if (!Job.initialized) {
      throw new Error('Not initialized. Call static function `initialize` with firebase config.');
    } else {
      return new Promise(resolve => {
        QueueDB.getRefForState(state)
          .once('value', snapshot => resolve(snapshot.numChildren()));
      });
    }
  }

  static getJobRoutes(apps, wares) {
    if (!Job.initialized) {
      throw new Error('Not initialized. Call static function `initialize` with firebase config.');
    }
    return getRoutes(apps, QueueDB, wares);
  }

  static shutdown(app) {
    if (!app) {
      throw new Error("Missing 'app' parameter");
    }
    const appQueues = Job.queues[app] || [];
    if (appQueues.length > 0) {
      const promises = appQueues.map(queue => queue.shutdown());
      Promise.all(promises)
        .then(res => logger.info(`${appQueues.length} queues shut down for app ${app}`))
        .catch(err => logger.error(err));
    } else {
      logger.info('No queues to shut down');
    }
  }

  on(taskName, handler) {
    const task = this.tasks[taskName];
    if (!task) {
      throw new Error(`'${taskName}' is not a registered task for this job.`);
    }
    let appQueues = Job.queues[this.app];
    if (!appQueues) {
      appQueues = [];
    }
    appQueues.push(new Queue(QueueDB.getQueueRef(), {
      specId: task.getSpecName(),
      numWorkers: task.getWorkerCount(),
      backoff: this.backoff,
    }, task.getHandler(handler)));
    Job.queues[this.app] = appQueues;
  }

  getInputData(jobData) {
    if (typeof this.inputData === 'function') {
      return {
        input: this.inputData(jobData),
      };
    } else {
      return {};
    }
  }

  add(jobData) {
    QueueDB.getTasksRef().push({
      ...jobData,
      __display__: this.getInputData(jobData),
      _state: this.startTask.getStartState(),
      __type__: this.type,
      __app__: this.app,
    });
  }
}