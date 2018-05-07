import conf from '../../../key';
import { Workflow } from "../../index";
import logger from '../../utils/logger';

Workflow.initialize({
  firebase: conf,
});

const job = new Workflow({
  app: 'server-utils',
  type: 'TestJob',
  numWorkers: 5,
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
  }, {
    id: 'state_3',
    name: 'State 3',
    outputData() {
      return [{
        field2: 'State 3'
      }];
    }
  }, {
    id: 'state_4',
    name: 'State 4',
    outputData() {
      return [{
        field3: 'State 4'
      }];
    }
  }, {
    id: 'end',
    name: 'End Task',
    outputData() {
      return [{
        field4: 'State End'
      }];
    }
  }],
});

job.on('state_1', (data, progress, resolve, reject) => {
  logger.info({
    message: `${data.title} state_1 resolved`,
    time: new Date(),
  });
  resolve({
    progress: 20,
  });
});

job.on('state_2', (data, progress, resolve, reject) => {
  setTimeout(() => {
    logger.info({
      message: `${data.title} state_2 resolved`,
      time: new Date(),
    });
    resolve({
      progress: 40,
    });
  }, 5000);
});

job.on('state_3', (data, progress, resolve, reject) => {
  setTimeout(() => {
    resolve({
      progress: 60,
    });
    logger.info({
      message: `${data.title} state_3 resolved`,
      time: new Date(),
    });

  }, 10000);
});

job.on('state_4', (data, progress, resolve, reject) => {
  setTimeout(() => {
    resolve({
      progress: 80,
    });
    logger.info({
      message: `${data.title} state_4 resolved`,
      time: new Date(),
    });

  }, 15000);
});


job.on('end', (data, progress, resolve, reject) => {
  logger.info({
    message: `${data.title} end resolved`,
    time: new Date(),
  });
  resolve({
    progress: 100,
  });
});

job.add({
  title: 'job1',
});

job.add({
  title: 'job2',
});

job.add({
  title: 'job3',
});

job.add({
  title: 'job4',
});
job.add({
  title: 'job11',
});

job.add({
  title: 'job12',
});

job.add({
  title: 'job13',
});

job.add({
  title: 'job14',
});
job.add({
  title: 'job21',
});

job.add({
  title: 'job22',
});

job.add({
  title: 'job23',
});

job.add({
  title: 'job24',
});
job.add({
  title: 'job31',
});

job.add({
  title: 'job32',
});

job.add({
  title: 'job33',
});

job.add({
  title: 'job34',
});
job.add({
  title: 'job41',
});

job.add({
  title: 'job42',
});

job.add({
  title: 'job43',
});

job.add({
  title: 'job44',
});