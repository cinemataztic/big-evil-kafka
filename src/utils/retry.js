import { backOff } from 'exponential-backoff';

const retryOptions = {
  startingDelay: 1000,
  numOfAttempts: 3,
  timeMultiple: 2,
  retry: (error, attemptNumber) => {
    logger.error(`Attempt ${attemptNumber} failed due to error: ${error}`);
    return true; // Retry on failed attempt
  },
};

export default retryOptions;
