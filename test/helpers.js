'use strict';

import _ from 'lodash';
import admin from 'firebase-admin';
import { QueueWorkerWithoutProcessing, QueueWorkerWithoutProcessingOrTimeouts } from "./QueueWorkerWithoutProcessing";
import fbConf from '../key.json';

admin.initializeApp({
  credential: admin.credential.cert(fbConf.serviceAccount),
  databaseURL: fbConf.config.databaseURL,
});

export default function () {
  const self = this;

  this.testRef = admin.database().ref(_.random(1, 2 << 29));
  this.offset = 0;
  self.testRef.root.child('.info/serverTimeOffset').on('value', function (snapshot) {
    self.offset = snapshot.val();
  });
  this.Queue = require('../src/Queue.js');
  this.QueueWorker = require('../src/Worker.js');

  this.QueueWorkerWithoutProcessingOrTimeouts = QueueWorkerWithoutProcessingOrTimeouts;
  this.QueueWorkerWithoutProcessing = QueueWorkerWithoutProcessing;

  this.validBasicTaskSpec = {
    inProgressState: 'in_progress'
  };
  this.validTaskSpecWithStartState = {
    inProgressState: 'in_progress',
    startState: 'start_state'
  };
  this.validTaskSpecWithFinishedState = {
    inProgressState: 'in_progress',
    finishedState: 'finished_state'
  };
  this.validTaskSpecWithErrorState = {
    inProgressState: 'in_progress',
    errorState: 'error_state'
  };
  this.validTaskSpecWithTimeout = {
    inProgressState: 'in_progress',
    timeout: 10
  };
  this.validTaskSpecWithRetries = {
    inProgressState: 'in_progress',
    retries: 4
  };
  this.validTaskSpecWithEverything = {
    inProgressState: 'in_progress',
    startState: 'start_state',
    finishedState: 'finished_state',
    errorState: 'error_state',
    timeout: 10,
    retries: 4
  };

  return this;
};
