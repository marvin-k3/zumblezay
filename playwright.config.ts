import { defineConfig, devices } from '@playwright/test';

const PORT = process.env.PLAYWRIGHT_TEST_PORT ?? '4173';
const HOST = process.env.PLAYWRIGHT_TEST_HOST ?? '127.0.0.1';

export default defineConfig({
  testDir: 'playwright-tests',
  timeout: 60_000,
  expect: {
    timeout: 5_000,
  },
  fullyParallel: true,
  retries: process.env.CI ? 2 : 0,
  forbidOnly: !!process.env.CI,
  use: {
    baseURL: `http://${HOST}:${PORT}`,
    trace: 'retain-on-failure',
    video: 'retain-on-failure',
    screenshot: 'only-on-failure',
  },
  webServer: {
    command: `cargo run --quiet --bin playwright_server -- --host ${HOST} --port ${PORT}`,
    url: `http://${HOST}:${PORT}`,
    reuseExistingServer: !process.env.CI,
    timeout: 120_000,
    stdout: 'pipe',
    stderr: 'pipe',
  },
  projects: [
    {
      name: 'chromium',
      use: { ...devices['Desktop Chrome'] },
    },
  ],
});
