import QueueWorker from '../src/Worker';
import _ from 'lodash';

export class QueueWorkerWithoutProcessing extends QueueWorker {
  constructor(...args) {
    super(...args);
  }

  tryToProcess(...args) {
    return _.noop(...args);
  }
}

export class QueueWorkerWithoutProcessingOrTimeouts extends QueueWorkerWithoutProcessing {
  constructor(...args) {
    super(...args);
  }

  setupTimeouts(...args) {
    return _.noop(...args);
  }
}