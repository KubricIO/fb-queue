import Express from 'express';

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
    const snapshots = await QueueDB.getTasksRef().orderByChild('user').equalTo(email).once('value');

    let results = [];
    snapshots.forEach(snapshot => {
      results.push({
        key: snapshot.key,
        ...snapshot.val()
      })
    });
    results = results.filter(({ __app__ }) => appSet.has(__app__));
    res.status(200).send(results);
  }]);

  Router.put('/retry/:key', [...wares, async (req, res, next) => {
    const { key } = req.params;
    let snapshot = await QueueDB.getTaskRef(key).once('value');
    const task = snapshot.val();
    if (task === null) {
      res.status(500).send('Task does not exist');
    } else {
      const currentState = task._state;
      snapshot = await QueueDB.getSpecsRef().orderByChild('error_state').equalTo(currentState).limitToFirst(1).once('value');
      let erredStateSpec;
      snapshot.forEach(snap => erredStateSpec = snap.val());
      if (erredStateSpec) {
        const startState = erredStateSpec['start_state'];
        const taskStateRef = QueueDB.getTaskRef(`${key}/_state`);
        await taskStateRef.set(startState);
        res.status(200).send();
      } else {
        res.status(500).send('Some error occured');
      }
    }
  }]);

  Router.delete('/:key', [...wares, async (req, res, next) => {
    const { key } = req.params;
    await QueueDB.getTaskRef(key).remove();
    res.status(200).send();
  }]);

  return Router;
};
