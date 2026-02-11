import { expect, test } from '@playwright/test';
import type { Page } from '@playwright/test';

const TRANSCRIPT_DATE = '2024-05-01';
const EVENT_ID = 'event-alpha';
const CAMERA_ID = 'camera-1';
const TRANSCRIPT_TEXT = 'Visitor left a package on the porch.';
const SEARCH_TERM = 'package';

async function loadTranscript(page: Page) {
  await page.goto('/transcript');
  const datePicker = page.locator('#date-picker');
  await datePicker.fill(TRANSCRIPT_DATE);
  await page.click('#load-transcript');
  await expect(page.locator('.transcript-entry')).toHaveCount(1);
}

test.describe('Transcript Dashboard', () => {
  test('loads transcript entries and navigates to event', async ({ page }) => {
    await loadTranscript(page);

    const entry = page.locator('.transcript-entry').first();
    await expect(entry).toContainText(CAMERA_ID);
    await expect(entry).toContainText(TRANSCRIPT_TEXT);

    await entry.click();
    await page.waitForURL(new RegExp(`/events/latest\\?video=${EVENT_ID}`));
    await expect(page.locator('#video-player')).toHaveAttribute(
      'src',
      new RegExp(`/video/${EVENT_ID}$`),
    );
  });

  test('search highlights transcript text and navigates results', async ({
    page,
  }) => {
    await loadTranscript(page);

    await page.locator('#search-input').fill(SEARCH_TERM);
    await page.click('#search-button');

    await expect(page.locator('.search-highlight')).toHaveCount(1);
    await expect(page.locator('#search-count')).toContainText('matches');
    await expect(page.locator('#result-counter')).toContainText('1 of 1');

    await page.click('#next-result');
    await expect(page.locator('#result-counter')).toContainText('1 of 1');
  });
});
