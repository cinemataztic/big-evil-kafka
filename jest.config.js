module.exports = {
  projects: [
    {
      displayName: 'integration',
      testMatch: ['**/__test__/**/*test.js'],
      globalSetup: '<rootDir>/__test__/utils/globalSetup.js',
      globalTeardown: '<rootDir>/__test__/utils/globalTeardown.js',
      testEnvironment: "node",
    }
  ]
}