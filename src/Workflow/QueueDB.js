import { getIndexKey } from "./utils";
import { WFSTATUS_INDEX_KEYNAME } from "./constants";

export default class QueueDB {
  static dbRoot;
  static db;

  static initialize(fbConf = {}) {
    const { dbRoot = '/queue', firebaseInstance } = fbConf;
    if (typeof firebaseInstance === 'undefined') {
      throw new Error("firebaseInstance cannot be undefined");
    }
    QueueDB.dbRoot = dbRoot;
    if (typeof window === 'undefined') {
      const { config, serviceAccount } = fbConf;
      if (typeof config === 'undefined') {
        throw new Error('Provided firebase conf should have a property "config" with the "databaseURL"');
      }
      if (typeof serviceAccount === 'undefined') {
        throw new Error('Provided firebase conf should have a property "serviceAccount" with the service account details');
      }
      firebaseInstance.initializeApp({
        credential: firebaseInstance.credential.cert(serviceAccount),
        databaseURL: config.databaseURL,
        databaseAuthVariableOverride: {
          uid: 'kubric-fbqueue-admin',
        },
      });
    }
    QueueDB.db = firebaseInstance.database();
  }

  static getQueueRef(path) {
    return QueueDB.db.ref(`${QueueDB.dbRoot}${path}`);
  }

  static getTasksRef(app, jobType) {
    return QueueDB.getQueueRef(`/apps/${app}/${jobType}/tasks`);
  }

  static getTaskRef(app, jobType, taskId) {
    return QueueDB.getQueueRef(`/apps/${app}/${jobType}/tasks/${taskId}`);
  }

  static getSpecsRef(app, jobType) {
    return QueueDB.getQueueRef(`/apps/${app}/${jobType}/specs`);
  }

  static getSpecRef(app, jobType, specId) {
    return QueueDB.getQueueRef(`/apps/${app}/${jobType}/specs/${specId}`);
  }

  static getAllTasksRef(user) {
    return QueueDB.getQueueRef(`/alltasks/${user}`);
  }

  static getAllTasksRefFor(user, key) {
    return QueueDB.getQueueRef(`/alltasks/${user}/${key}`);
  }

  static getFilteredTasksRef({ app, jobType, user, indexField, indexId, wfStatus }) {
    const ref = QueueDB.getTasksRef(app, jobType)
      .orderByChild(WFSTATUS_INDEX_KEYNAME);
    if (typeof wfStatus === 'number') {
      return ref.equalTo(getIndexKey(user, wfStatus, indexId, indexField));
    } else {
      const { from, to } = wfStatus;
      const startAtIndexKey = getIndexKey(user, from, indexId, indexField);
      const endAtIndexKey = getIndexKey(user, to, indexId, indexField);
      return ref.startAt(startAtIndexKey)
        .endAt(endAtIndexKey);
    }
  }
}