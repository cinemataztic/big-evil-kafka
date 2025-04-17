const { backOff } = require('exponential-backoff');

/**
 * Executes the provided asynchronous function `fn` with exponential backoff retry logic. 
 * This function will call `fn` and, in the event of an error, will retry the connection using the `backOff` method with a starting delay of 1000ms, doubling the delay after each attempt.
 * The process will be repeated for the number of attempts specified by `numOfAttempts`.
 *
 * @async
 * @function retryConnection
 * @param {Function} fn - An asynchronous function to be executed with retry logic.
 * @param {string} entity - An identifier for the producer/consumer, used for logging error messages and debugging.
 * @param {number} numOfAttempts - The maximum number of attempts to execute `fn` before rejecting.
 * @returns {Promise<*>} The resolved value from the function `fn` if one attempt is successful.
 * @throws Will throw an error if all retry attempts fail.
 */

const retry = async (fn, entity, numOfAttempts) => {
  return backOff(fn, {
    startingDelay: 1000,
    numOfAttempts,
    timeMultiple: 2,
    retry: (error, attemptNumber) => {
      console.error(
        `${entity} retry attempt ${attemptNumber} failed due to error: ${error}`,
      );
      return true; // Retry on failed attempt
    },
  });
};

module.exports = retry;
