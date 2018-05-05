import { Workflow } from '../../index';
import conf from '../../../key';

Workflow.initialize({
  firebase: conf,
});

console.log('state_1', 'state_2', 'state_3', 'state_4', 'end', 'time');

setInterval(() => {
  Promise.all([
      Workflow.getTaskCount('TestJob_state_1_in_progress'),
      Workflow.getTaskCount('TestJob_state_2_in_progress'),
      Workflow.getTaskCount('TestJob_state_3_in_progress'),
      Workflow.getTaskCount('TestJob_state_4_in_progress'),
      Workflow.getTaskCount('TestJob_end_in_progress')
    ])
    .then((results = []) => {
      console.log(`${results[0]}      `, `${results[1]}      `, `${results[2]}      `, `${results[3]}      `, `${results[4]}  `, `${+new Date()}`);
    })
}, 1000);
