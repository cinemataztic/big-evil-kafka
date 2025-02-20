/**
 * Configuration options for a retry mechanism.
 *
 * @typedef {Object} retryOptions
 * @property {number} startingDelay - Initial delay before the first retry attempt (in milliseconds).
 * @property {number} numOfAttempts - Maximum number of retry attempts.
 * @property {number} timeMultiple - Multiplier for increasing the delay between retries.
 * @property {(error: Error, attemptNumber: number) => boolean} retry - Function determining whether to retry.
 *   It receives the error and the attempt number, and should return `true` to retry or `false` to stop.
 */
const retryOptions = {
  startingDelay: 1000,
  numOfAttempts: 3,
  timeMultiple: 2,
  retry: (error, attemptNumber) => {
    console.error(`Attempt ${attemptNumber} failed due to error: ${error}`);
    return true; // Retry on failed attempt
  },
};

export default retryOptions;
