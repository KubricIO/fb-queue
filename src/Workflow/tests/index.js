import conf from '../../../key';
import { Workflow } from "../../index";
import logger from 'winston';

Workflow.initialize({
  firebase: conf,
});

const config = {
  numWorkers: 1,
  eventHandlers: {
    status(wfStatus) {
      // logger.info(wfStatus);
    }
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
    retries: 3,
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
};

const setupQueue = job => {
  job.on('state_1', (data, progress, resolve, reject) => {
    setTimeout(() => {
      resolve({
        progress: 20,
      });
    }, 2000);
  });

  job.on('state_2', (data, progress, resolve, reject) => {
    setTimeout(() => {
      resolve({
        progress: 40,
      });
    }, 2000);
  });

  job.on('state_3', (data, progress, resolve, reject) => {
    setTimeout(() => {
      resolve({
        progress: 60,
      });
    }, 2000);
  });

  job.on('state_4', (data, progress, resolve, reject) => {
    setTimeout(() => {
      resolve({
        progress: 80,
      });
    }, 2000);
  });


  job.on('end', (data, progress, resolve, reject) => {
    setTimeout(() => {
      resolve({
        progress: 100,
      });
    }, 2000);
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
  }, {
    indexId: 'abc',
  });

  job.add({
    title: 'job43',
  }, {
    indexId: 'abc',
  });

  job.add({
    title: 'job44',
  }, {
    indexId: 'abc',
  });
};

const job1 = new Workflow({
  ...config,
  app: 'server-utils1',
  type: 'TestJob1',
});
setupQueue(job1);

job1.setStatsListener(logger.info.bind(logger, "TestJob1 stats"));
job1.setStatsListener(logger.info.bind(logger, "TestJob1/abc stats"), 'abc');
// const job2 = new Workflow({
//   ...config,
//   app: 'server-utils1',
//   type: `TestJob2`
// });
// setupQueue(job2);
//
// const job3 = new Workflow({
//   ...config,
//   app: 'server-utils2',
//   type: 'TestJob2',
// });
// setupQueue(job3);
//
// const job4 = new Workflow({
//   ...config,
//   app: 'server-utils2',
//   type: 'TestJob3',
// });
// setupQueue(job4);
