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

  static getTasksRef() {
    return firebaseAdmin.database().ref(`${QueueDB.dbRoot}/tasks`);
  }

  static getSpecsRef() {
    return firebaseAdmin.database().ref(`${QueueDB.dbRoot}/specs`);
  }

  static getJobtypesRef(type) {
    return firebaseAdmin.database().ref(`${QueueDB.dbRoot}/jobtypes/${type}`);
  }

  static getTaskRef(key) {
    return firebaseAdmin.database().ref(`${QueueDB.dbRoot}/tasks/${key}`);
  }

  static getSpecRef(type) {
    return firebaseAdmin.database().ref(`${QueueDB.dbRoot}/specs/${type}`);
  }

  static getLockRef(name) {
    return firebaseAdmin.database().ref(`${QueueDB.dbRoot}/locks/${name}`);
  }

  static getRefForState(state) {
    return firebaseAdmin.database()
      .ref(`${QueueDB.dbRoot}/tasks`)
      .orderByChild('_state')
      .equalTo(state);
  }

  static getStatsRef() {
    return firebaseAdmin.database().ref(`${QueueDB.dbRoot}/stats`);
  }

  static getStatsRefsFor(appName, jobType, indexValue) {
    const statsPath = `${QueueDB.dbRoot}/stats`;
    const statsRefs = [
      firebaseAdmin.database().ref(`${statsPath}/__stats__`),
      firebaseAdmin.database().ref(`${statsPath}/${appName}/__stats__`),
      firebaseAdmin.database().ref(`${statsPath}/${appName}/${jobType}/__stats__`),
    ];
    if (typeof indexValue !== 'undefined') {
      statsRefs.push(firebaseAdmin.database().ref(`${statsPath}/${appName}/${jobType}/${indexValue}/__stats__`));
    }
    return statsRefs;
  }
}