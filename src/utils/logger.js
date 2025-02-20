import {
  addColors,
  format as _format,
  transports as _transports,
  createLogger,
} from 'winston';

const levels = {
  error: 0,
  warn: 1,
  info: 2,
  http: 3,
  debug: 4,
};

const level = () => {
  let logLevel = 'info';
  const env = process.env.NODE_ENV || 'development';
  const isDevelopment = env === 'development';

  if (isDevelopment) {
    logLevel = 'debug';
  }

  return logLevel;
};

const colors = {
  error: 'red',
  warn: 'yellow',
  info: 'green',
  http: 'magenta',
  debug: 'white',
};

addColors(colors);

const format = _format.combine(
  _format.timestamp({ format: 'YYYY-MM-DD HH:mm:ss:ms' }),
  _format.colorize({ all: true }),
  _format.printf((info) => `${info.timestamp} ${info.level}: ${info.message}`),
);

const transports = [
  new _transports.Console({
    level: 'error',
    format: _format.combine(_format.colorize(), format),
    stderrLevels: ['error'], // Explicitly log only errors to stderr
  }),
  new _transports.Console({
    level: level(),
    format: _format.combine(_format.colorize(), format),
  }),
];

const logger = createLogger({
  level: level(),
  levels,
  format,
  transports,
});

export default logger;
