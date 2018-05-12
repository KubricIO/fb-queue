import Express from 'express';
import { validateFirebaseKey } from "./utils";
import _ from 'lodash';

export default (apps = [], QueueDB, wares = []) => {
  const Router = Express.Router();

  Router.get('/:email', [...wares, async (req, res, next) => {
    const { email } = req.params;
    if (!email || email.length === 0) {
      res.status(500).send('Please provide a valid email id');
    }
    if (typeof apps === 'string') {
      apps = [apps];
    }
    const appSet = new Set(apps);
    const userKey = validateFirebaseKey(email);

    const snapshots = await QueueDB.getAllTasksRef(userKey)
      .orderByChild('user')
      .once('value');

    let promises = [];
    snapshots.forEach(snapshot => {
      const { taskRefKey, __app__: appName, __type__: jobType } = snapshot.val();
      if (appSet.has(appName)) {
        promises.push(QueueDB.getTaskRef(appName, jobType, taskRefKey)
          .once('value')
          .then(taskSnap => {
            const taskData = taskSnap.val();
            return {
              ...taskData,
              key: taskSnap.key,
            };
          }));
      }
    });
    results = Promise.all(promises);
    res.status(200).send(results);
  }]);

  Router.put('/retry', [...wares, async (req, res, next) => {
    const { key, __type__: jobType, __app__: appName } = req.body;
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

  Router.delete('/:key', [...wares, async (req, res, next) => {
    let email = _.get(req, '_sessionData.email');
    if (typeof email === 'undefined') {
      email = req.params.email;
    }
    if (!email) {
      res.status(500).send('Missing user email');
    }
    const { key } = req.params;
    const allTasksRef = QueueDB.getAllTasksRefFor(email, key);
    const snapshot = await allTasksRef.once('value');
    const { __app__: appName, __type__: jobType } = snapshot.val();
    await allTasksRef.remove();
    await QueueDB.getTaskRef(appName, jobType, key).remove();
    res.status(200).send();
  }]);

  return Router;
};
