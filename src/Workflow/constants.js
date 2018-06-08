export const wfStatus = {
  PENDING: 0,
  ERRED: -1,
  PROGRESS: 1,
  COMPLETED: 10,
};

export const INVALID_KEY_REGEX = /[\.#\$\[\]]/g;
export const APP_JOBTYPE_KEYNAME = `__app_jobtype__`;
export const WFSTATUS_INDEX_KEYNAME = `__index_wfstatus__`;
export const WFSTATUS_KEYNAME = `__wfstatus__`;
export const INDEX_KEYNAME = `__index__`;
export const PROGRESS_STATS_KEYNAME = `_progressStats`;
export const PROGRESS_KEYNAME = `_progress`;