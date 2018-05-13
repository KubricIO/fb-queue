import conf from '../../../key';
import { Workflow } from "../../index";
import logger from 'winston';
import _ from 'lodash';

const config = {
  numWorkers: 1,
  eventHandlers: {
    status(wfStatus, jobData) {
      logger.info(wfStatus);
      logger.info(jobData);
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
};

const addJobs = (job, user) => {
  job.add({
    title: 'job1',
    user,
  }, {
    indexField: 'ad',
    indexId: "test",
  });

  job.add({
    title: 'job2',
    user,
  });

  job.add({
    title: 'job3',
    user,
  });

  job.add({
    title: 'job4',
    user,
  });

  job.add({
    title: 'job11',
    user,
  });

  job.add({
    title: 'job12',
    user,
  });

  job.add({
    title: 'job13',
    user,
  });

  job.add({
    title: 'job14',
    user,
  });
  job.add({
    title: 'job21',
    user,
  });

  job.add({
    title: 'job22',
    user,
  });

  job.add({
    title: 'job23',
    user,
  });

  job.add({
    title: 'job24',
    user,
  });
  job.add({
    title: 'job31',
    user,
  });

  job.add({
    title: 'job32',
    user,
  });

  job.add({
    title: 'job33',
    user,
  });

  job.add({
    title: 'job34',
    user,
  });
  job.add({
    title: 'job41',
    user,
  });

  job.add({
    title: 'job42',
    user,
  }, {
    indexField: 'campaign',
    indexId: 'abc',
  });

  job.add({
    title: 'job43',
    user,
  }, {
    indexField: 'campaign',
    indexId: 'abc',
  });

  job.add({
    title: 'job44',
    user,
  }, {
    indexField: 'campaign',
    indexId: 'abc',
  });
};

const QueueDB = Workflow.initialize({
  firebase: conf,
});

// QueueDB.getSpecsRef('server-utils1', 'TestJob2')
//   .orderByChild('status')
//   .startAt(`abc:-1`)
//   .endAt(`abc:1`)
//   .limitToFirst(1)
//   .on('value', snapshot => {
//     const val = snapshot.val();
//     if (_.isNull(val)) {
//       logger.info('done');
//     } else {
//       logger.info(val);
//     }
//   });
const job1 = new Workflow({
  ...config,
  app: 'server-utils1',
  type: 'TestJob1',
});
setupQueue(job1);
addJobs(job1);
//
// const job2 = new Workflow({
//   ...config,
//   app: 'server-utils1',
//   type: `TestJob2`
// });
// setupQueue(job2);
// addJobs(job2, "jophin2u@gmail.com");
//
// const job3 = new Workflow({
//   ...config,
//   app: 'server-utils2',
//   type: 'TestJob2',
// });
// setupQueue(job3);
// addJobs(job3, "jophin3u@gmail.com");
//
// const job4 = new Workflow({
//   ...config,
//   app: 'server-utils2',
//   type: 'TestJob3',
// });
// setupQueue(job4);
// addJobs(job4, "jophin4u@gmail.com");
