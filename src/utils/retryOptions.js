/**
 * Configuration options for a retry mechanism.
 *
 * @typedef {Object} retryOptions
 * @property {number} startingDelay - Initial delay before the first retry attempt (in milliseconds).
 * @property {number} numOfAttempts - Maximum number of retry attempts.
 * @property {number} timeMultiple - Multiplier for increasing the delay between retries.
 * @property {(error: any, attemptNumber: number) => boolean} retry - A callback function determining whether to retry after an error occurs.
 */
const retryOptions = {
  startingDelay: 1000,
  numOfAttempts: 5,
  timeMultiple: 2,
  retry: (error, attemptNumber) => {
    console.error(`Attempt ${attemptNumber} failed due to error: ${error}`);
    return true; // Retry on failed attempt
  },
};

module.exports = retryOptions;
