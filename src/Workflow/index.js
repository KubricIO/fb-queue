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

  constructor({ app, type = '', tasks = [], timeout, retries, numWorkers = 1, inputData }) {
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

  static updateStatsFor(statKey, wfStatus, switchOffListener, ref) {
    return ref.transaction(statsObject => {
      logger.info("Wfstats", wfStatus);
      logger.info("Incoming", statsObject);
      const pendingKey = statKeyMap[0];
      const progressKey = statKeyMap[10];
      if (_.isNull(statsObject)) {
        if (wfStatus === 0) {
          statsObject = {}
        } else {
          logger.info("Returning without update");
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
        switchOffListener();
      }
      logger.info("Outgoing", statsObject);
      return statsObject;
    }, undefined, false);
  }

  static updateStats(statsRefs, index, statKey, wfStatus, switchOffListener) {
    if (index < statsRefs.length) {
      return Job.updateStatsFor(statKey, wfStatus, switchOffListener, statsRefs[index])
        .then(() => Job.updateStats(statsRefs, ++index, statKey, wfStatus, switchOffListener));
    } else {
      return Promise.resolve();
    }
  }

  static statusListener(taskRef, switchOffListener, wfStatus) {
    logger.info("Status changed");
    logger.info("Status", wfStatus.val());
    logger.info("Status", taskRef.key);

    // taskRef.once('value', taskSnap => {
    //   const taskVal = taskSnap.val();
    //   if (!_.isNull(taskVal)) {
    //     const { __app__: appName, __type__: jobType, __index__: indexValue, __wfstatus__: wfStatus } = taskSnap.val();
    //     const statKey = statKeyMap[wfStatus];
    //     const statsRefs = QueueDB.getStatsRefsFor(appName, jobType, wfStatus, indexValue);
    //     // return Job.updateStatsFor(statKey, wfStatus, switchOffListener, QueueDB.getStatsRef());
    //     return Job.updateStats(statsRefs, 0, statKey, wfStatus, switchOffListener)
    //       .catch(ex => {
    //         logger.error('Erred while setting up stats listener');
    //         logger.error(ex);
    //       });
    //   } else {
    //     return Promise.resolve();
    //   }
    // });
  }

  static setupStatListeners(taskRef) {
    const boundListener = Job.statusListener.bind(Job, taskRef, () => {
      taskRef.child('__wfstatus__').off('value', boundListener);
    });
    taskRef.child('__wfstatus__').on('value', boundListener);
  }

  add(jobData, { indexId } = {}) {
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
    const taskRef = QueueDB.getTasksRef().push(taskData, () => {
      Job.setupStatListeners(taskRef)
    });
    return taskRef;
  }
}