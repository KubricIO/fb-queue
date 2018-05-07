import conf from '../../../key';
import { Workflow } from "../../index";
// import { devLogger as logger } from '../../utils/logger';
import logger from '../../utils/logger';

Workflow.initialize({
  firebase: conf,
});

const job = new Workflow({
  app: 'server-utils',
  type: 'TestJob',
  numWorkers: 5,
  retries: 5,
  backoff: {
    strategy: 'fibonacci',
    conf: {
      initialDelay: 1000,
      maxDelay: 15000
    },
  },
  inputData({ title }) {
    return [{
      name: "Job title",
      value: title,
    }];
  },
  tasks: [{
    id: 'state_1',
    name: 'State 1',
    outputData() {
      return [{
        field: 'State 1'
      }];
    }
  }, {
    id: 'state_2',
    name: 'State 2',
    numWorkers: 20,
    outputData() {
      return [{
        field1: 'State 2'
      }];
    }
  }],
});

let rejectCount = 0;
job.on('state_1', (data, progress, resolve, reject) => {
  logger.info({
    message: `${data.title} state_1 resolved`,
    time: new Date(),
  });
  if (++rejectCount < 3) {
    reject(new Error('Rejecting state 1'));
  } else {
    resolve({
      progress: 50,
    });
  }
});

job.on('state_2', (data, progress, resolve, reject) => {
  setTimeout(() => {
    logger.info({
      message: `${data.title} state_2 resolved`,
      time: new Date(),
    });
    resolve({
      progress: 100,
    });
  }, 5000);
});

job.add({
  title: 'job1',
});