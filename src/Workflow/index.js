import Queue from '../Queue';
import QueueDB from './db';
import getRoutes from './routes';
import Task from "./task";
import logger from 'winston';
import _ from 'lodash';

const statKeyMap = {
  0: 'pending',
  1: 'completed',
  10: 'progress',
  '-1': 'erred',
};

export default class Job {
  static initialized = false;
  static queues = {};

  constructor({ app, type = '', tasks = [], timeout, retries, numWorkers = 1, inputData, eventHandlers = {} }) {
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
    this.tasks = Task.createTasks(this.type, tasks, {
      timeout,
      retries,
      numWorkers,
    });
    this.startTask = this.tasks[tasks[0].id];
    this.inputData = inputData;
    this.eventHandlers = eventHandlers;
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
        .then(res => logger.debug(`${appQueues.length} queues shut down for app ${app}`))
        .catch(err => logger.error(err));
    } else {
      logger.debug('No queues to shut down');
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

  static updateStatsFor(taskRef, statKey, wfStatus, ref) {
    return ref.transaction(statsObject => {
      logger.debug("Status", wfStatus);
      logger.debug("Status", taskRef.key);
      logger.debug("Incoming", statsObject);
      const pendingKey = statKeyMap[0];
      const progressKey = statKeyMap[10];
      if (_.isNull(statsObject)) {
        if (wfStatus === 0) {
          logger.debug("Returning pending 0");
          statsObject = {
            pending: 0,
          };
        } else {
          logger.debug("Returning without update");
          return statsObject;
        }
      }
      const currentStat = statsObject[statKey] || 0;
      statsObject[statKey] = currentStat + 1;
      if (wfStatus === 10) {
        const pendingStat = statsObject[pendingKey] || 0;
        statsObject[pendingKey] = pendingStat > 0 ? (pendingStat - 1) : 0;
      } else if (wfStatus === 1 || wfStatus === -1) {
        const progressStat = statsObject[progressKey] || 0;
        statsObject[progressKey] = progressStat > 0 ? (progressStat - 1) : 0;
      }
      logger.debug("Outgoing", statsObject);
      return statsObject;
    }, undefined, false);
  }

  static updateStats(taskRef, statsRefs, index, statKey, wfStatus) {
    if (index < statsRefs.length) {
      return Job.updateStatsFor(taskRef, statKey, wfStatus, statsRefs[index])
        .then(({ committed, snapshot }) => {
          logger.debug('Commmitted', committed);
          logger.debug('Snap', snapshot.val());
          return Job.updateStats(taskRef, statsRefs, ++index, statKey, wfStatus)
        });
    } else {
      return Promise.resolve();
    }
  }

  static statusListener(taskRef, statusChangeHandler, switchOffListener, wfSnap) {
    const wfStatus = wfSnap.val();
    if (statusChangeHandler) {
      setImmediate(statusChangeHandler, wfStatus, taskRef);
    }
    if (wfStatus === 1 || wfStatus === -1) {
      switchOffListener();
    }
    return taskRef.once('value', taskSnap => {
      const taskVal = taskSnap.val();
      if (!_.isNull(taskVal)) {
        const { __app__: appName, __type__: jobType, __index__: indexValue } = taskVal;
        if (!_.isUndefined(appName) && !_.isUndefined(jobType)) {
          const statKey = statKeyMap[wfStatus];
          const statsRefs = QueueDB.getStatsRefsFor(appName, jobType, indexValue);
          Job.updateStats(taskRef, statsRefs, 0, statKey, wfStatus)
            .catch(ex => {
              logger.error('Erred while setting up stats listener');
              logger.error(ex);
            });
        }
      }
    });
  }

  static setupStatListeners(taskRef, statusChangeHandler) {
    const boundListener = Job.statusListener.bind(Job, taskRef, statusChangeHandler, () => {
      taskRef.child('__wfstatus__').off('value', boundListener);
    });
    taskRef.child('__wfstatus__').on('value', boundListener);
  }

  setStatsListener(handler, indexId) {
    return QueueDB.getStatsRefsFor(this.app, this.type, indexId)
      .pop()
      .on('value', statsSnap => setImmediate(handler, statsSnap.val()));
  }

  add(jobData, { indexId, eventHandlers = {} } = {}) {
    const statusChangeHandler = this.eventHandlers['status'] || eventHandlers['status'];
    const taskData = {
      ...jobData,
      __display__: this.getInputData(jobData),
      __wfstatus__: 0,
      _state: this.startTask.getStartState(),
      __type__: this.type,
      __app__: this.app,
    };
    if (typeof indexId !== 'undefined') {
      taskData.__index__ = indexId;
    }
    const taskRef = QueueDB.getTasksRef().push();
    taskRef.transaction(() => taskData, () => Job.setupStatListeners(taskRef, statusChangeHandler));
    return taskRef;
  }
}