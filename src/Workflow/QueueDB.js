import firebase from 'firebase';
import firebaseAdmin from 'firebase-admin'

export default class QueueDB {
  static dbRoot;

  static initialize(fbConf) {
    if (typeof fbConf === 'undefined') {
      return new Error('Firebase config should be provided in the property "firebase"');
    }
    const { config, serviceAccount, dbRoot = '/queue' } = fbConf;
    if (typeof config === 'undefined') {
      return new Error('Provided firebase conf should have a property "config" with the "databaseURL"');
    }
    if (typeof serviceAccount === 'undefined') {
      return new Error('Provided firebase conf should have a property "serviceAccount" with the service account details');
    }
    QueueDB.dbRoot = dbRoot;
    firebaseAdmin.initializeApp({
      credential: firebaseAdmin.credential.cert(serviceAccount),
      databaseURL: config.databaseURL,
      databaseAuthVariableOverride: {
        canAddTasks: true,
        canProcessTasks: true,
        canAddSpecs: true,
      },
    });
  }

  static getQueueRef() {
    return firebaseAdmin.database().ref(QueueDB.dbRoot);
  }

  static getTasksRef(app, jobType) {
    return firebaseAdmin.database().ref(`${QueueDB.dbRoot}/apps/${app}/${jobType}/tasks`);
  }

  static getTaskRef(app, jobType, taskId) {
    return firebaseAdmin.database().ref(`${QueueDB.dbRoot}/apps/${app}/${jobType}/tasks/${taskId}`);
  }

  static getSpecsRef(app, jobType) {
    return firebaseAdmin.database().ref(`${QueueDB.dbRoot}/apps/${app}/${jobType}/specs`);
  }

  static getSpecRef(app, jobType, specId) {
    return firebaseAdmin.database().ref(`${QueueDB.dbRoot}/apps/${app}/${jobType}/specs/${specId}`);
  }

  static getAllTasksRef(user) {
    return firebaseAdmin.database().ref(`${QueueDB.dbRoot}/alltasks/${user}`);
  }

  static getAllTasksRefFor(user, key) {
    return firebaseAdmin.database().ref(`${QueueDB.dbRoot}/alltasks/${user}/${key}`);
  }
}