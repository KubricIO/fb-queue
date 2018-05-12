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

  static async initialize({ firebase }) {
    try {
      QueueDB.initialize(firebase);
      return QueueDB.getStatsRef()
        .remove()
        .then(() => Job.setupTasksListeners())
        .then(() => (Job.initialized = true));
    } catch (ex) {
      throw ex;
    }
  }

  static updateStatsForTask(taskSnap, statTransformer) {
    const taskVal = taskSnap.val();
    if (!_.isNull(taskVal)) {
      const { __app__: appName, __type__: jobType, __index__: indexValue, __wfstatus__: wfStatus } = taskVal;
      if (!_.isUndefined(appName) && !_.isUndefined(jobType)) {
        const statsRefs = QueueDB.getStatsRefsFor(appName, jobType, indexValue);
        return Job.updateStats(statsRefs, 0, wfStatus, statTransformer)
          .catch(ex => {
            logger.error('Erred while setting up stats listener');
            logger.error(ex);
          });
      }
    }
    return Promise.resolve();
  }

  static startupStat(incrementBy, wfStatus, statsObj) {
    const statKey = statKeyMap[wfStatus];
    if (_.isNull(statsObj)) {
      return {
        [statKey]: 1,
      }
    } else {
      statsObj[statKey] = (statsObj[statKey] || 0) + incrementBy;
      return statsObj;
    }
  }

  static setupTasksListeners() {
    const tasksRef = QueueDB.getTasksRef();
    tasksRef.on('child_removed', oldTaskSnap => {
      if (!_.isNull(oldTaskSnap.val())) {
        Job.updateStatsForTask(oldTaskSnap, Job.startupStat.bind(Job, -1));
      }
    });
    return new Promise((resolve, reject) => {
      tasksRef.once('value', tasksSnap => {
        const promises = [];
        tasksSnap.forEach(taskSnap => {
          const taskVal = taskSnap.val();
          if (!_.isNull(taskVal)) {
            const { __app__: appName, __type__: jobType, __index__: indexValue, __wfstatus__: wfStatus } = taskVal;
            if (!_.isUndefined(appName) && !_.isUndefined(jobType) && (wfStatus === 0 || wfStatus === 10)) {
              Job.setupStatListeners(taskSnap.ref);
            }
            promises.push(Job.updateStatsForTask(taskSnap, Job.startupStat.bind(Job, 1)));
          }
        });
        Promise.all(promises)
          .then(resolve)
          .catch(reject);
      });
    });
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
      QueueDB.getTasksRef().off(); //Removes all callbacks for all events on that node
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

  static updateStatsFor(wfStatus, statRef, statTransformer) {
    const statKey = statKeyMap[wfStatus];
    return statRef.transaction(statsObject => {
      if (statTransformer) {
        return statTransformer(wfStatus, statsObject);
      } else {
        logger.debug("Status", wfStatus);
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
      }
    }, undefined, false);
  }

  static updateStats(statsRefs, index, wfStatus, statTransformer) {
    if (index < statsRefs.length) {
      return Job.updateStatsFor(wfStatus, statsRefs[index], statTransformer)
        .then(({ committed, snapshot }) => {
          logger.debug('Commmitted', committed);
          logger.debug('Snap', snapshot.val());
          return Job.updateStats(statsRefs, ++index, wfStatus, statTransformer)
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
          const statsRefs = QueueDB.getStatsRefsFor(appName, jobType, indexValue);
          Job.updateStats(statsRefs, 0, wfStatus)
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

  addStatsListener(handler, indexId) {
    const ref = QueueDB.getStatsRefsFor(this.app, this.type, indexId).pop();
    const listener = ref.on('value', statsSnap => {
      setImmediate(handler, statsSnap.val())
    });
    return () => ref.off('value', listener);
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