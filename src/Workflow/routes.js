import Express from 'express';
import { validateFirebaseKey } from "./utils";
import _ from 'lodash';

export default (apps = [], QueueDB, wares = []) => {
  const Router = Express.Router();

  const checkUserId = (req, res, next) => {
    const { userid } = req._sessionData;
    if (!userid || userid.length === 0) {
      res.status(401).send("Unauthorized");
    } else {
      req._userid = userid;
      next();
    }
  };

  const resolveKey = async (req, res, next) => {
    const { key } = req.params;
    const { _userid: userid } = req;
    const allTasksRef = QueueDB.getAllTasksRefFor(userid, key);
    const snapshot = await allTasksRef.once('value');
    const { __app__: appName, __type__: jobType, __ref__ } = snapshot.val();
    req._app = appName;
    req._jobType = jobType;
    req._allTasksRef = allTasksRef;
    req._jobKey = __ref__;
    next();
  };

  Router.get('/', [...wares, checkUserId, async (req, res) => {
    const { _userid: userid } = req;
    const appJobs = {};

    if (typeof apps === 'string') {
      apps = [apps];
    } else if (Array.isArray(apps)) {
      apps = apps.map(appConf => {
        if (typeof appConf.app !== 'undefined') {
          appJobs[appConf.app] = new Set(appConf.jobs || []);
          return appConf.app;
        } else {
          return appConf;
        }
      });
    }
    const appSet = new Set(apps);
    const userKey = validateFirebaseKey(userid);

    const snapshots = await QueueDB.getAllTasksRef(userKey)
      .orderByChild('user')
      .limitToFirst(50)
      .once('value');

    let promises = [];
    snapshots.forEach(allTaskSnap => {
      const { __ref__: taskRefKey, __app__: appName, __type__: jobType } = allTaskSnap.val();
      const jobSet = appJobs[appName];
      if (appSet.has(appName) && (!jobSet || jobSet.has(jobType))) {
        promises.push(QueueDB.getTaskRef(appName, jobType, taskRefKey)
          .once('value')
          .then(taskSnap => {
            const taskData = taskSnap.val();
            return {
              ...taskData,
              key: allTaskSnap.key,
            };
          }));
      }
    });
    const results = await Promise.all(promises);
    res.status(200).send(results);
  }]);

  Router.put('/retry/:key', [...wares, checkUserId, resolveKey, async (req, res, next) => {
    const { _app: appName, _jobType: jobType, _jobKey: key } = req;
    const taskRef = QueueDB.getTaskRef(appName, jobType, key);
    let snapshot = await taskRef.once('value');
    const task = snapshot.val();
    if (task === null) {
      res.status(500).send('Task does not exist');
    } else {
      const currentState = task._state;
      snapshot = await QueueDB.getSpecsRef(appName, jobType)
        .orderByChild('error_state')
        .equalTo(currentState)
        .limitToFirst(1)
        .once('value');
      let erredStateSpec;
      snapshot.forEach(snap => erredStateSpec = snap.val());
      if (erredStateSpec) {
        const startState = erredStateSpec['start_state'];
        await taskRef.child('_state').set(startState);
        res.status(200).send();
      } else {
        res.status(500).send('Some error occured');
      }
    }
  }]);

  Router.delete('/:key', [...wares, checkUserId, resolveKey, async (req, res, next) => {
    const { _app, _jobType, _allTasksRef, _jobKey } = req;
    await _allTasksRef.remove();
    await QueueDB.getTaskRef(_app, _jobType, _jobKey).remove();
    res.status(200).send();
  }]);

  return Router;
};
