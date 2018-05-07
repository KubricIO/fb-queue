import winston from 'winston';

const logLevels = {
  levels: {
    fatal: 0,
    error: 1,
    warning: 2,
    info: 3,
    debug: 4,
    trace: 5
  },
  colors: {
    fatal: 'red',
    error: 'orange',
    warning: 'yellow',
    info: 'green',
    debug: 'blue',
    trace: 'gray'
  }
};

const logger = new (winston.Logger)(logLevels);

export const devLogger = new (winston.Logger)(logLevels);

logger.add(winston.transports.Console, {
  level: 'info',
  json: false,
  colorize: true
});

devLogger.add(winston.transports.Console, {
  level: 'debug',
  json: false,
  colorize: true
});

export default logger;