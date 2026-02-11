import { expect, test } from '@playwright/test';

const SUMMARY_DATE = '2024-05-01';
const MODEL_ID = 'test-model';

test.describe('Summary Dashboard', () => {
  test('loads models and shows empty summary state', async ({ page }) => {
    await page.route('**/api/models', async (route) => {
      await route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({
          models: [
            {
              id: MODEL_ID,
              name: 'Test Model',
              provider: 'local',
            },
          ],
        }),
      });
    });

    await page.goto('/summary');

    const modelSelector = page.locator('#model-selector');
    await expect(modelSelector).toContainText('Test Model');
    await expect(modelSelector).toHaveValue(MODEL_ID);

    const datePicker = page.locator('#date-picker');
    await datePicker.fill(SUMMARY_DATE);
    await datePicker.dispatchEvent('change');

    await expect(page.locator('#summaries-list-container')).toContainText(
      'No summaries available for this date.',
    );
    await expect(page.locator('#markdown-summary')).toContainText(
      'No summaries available for this date.',
    );
  });
});
