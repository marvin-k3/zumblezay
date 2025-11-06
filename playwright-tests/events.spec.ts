import { expect, test } from '@playwright/test';

const EVENT_DATE = '2024-05-01';
const TRANSCRIPT_SNIPPET = 'Visitor left a package on the porch.';

test.describe('Events Dashboard', () => {
  test('loads seeded event and displays transcript', async ({ page }) => {
    await page.goto('/events');

    await page.waitForSelector('#camera-filter option[value="camera-1"]', {
      state: 'attached',
    });

    const dateFilter = page.locator('#date-filter');
    await dateFilter.fill(EVENT_DATE);
    await dateFilter.dispatchEvent('change');

    const eventItem = page.locator('.event-item').first();
    await expect(eventItem).toContainText('camera-1');

    await eventItem.click();

    const transcriptSegment = page.locator('.transcript-segment').first();
    await expect(transcriptSegment).toContainText(TRANSCRIPT_SNIPPET);

    await expect(page.locator('#video-title')).toContainText('camera-1');
    await expect(page.locator('body')).toContainText('Transcript');
  });
});
