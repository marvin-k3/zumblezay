import { expect, test } from '@playwright/test';
import type { Page } from '@playwright/test';
import type { Route } from '@playwright/test';

const EVENT_DATE = '2024-05-01';
const ALT_EVENT_DATE = '2024-05-02';
const TRANSCRIPT_SNIPPET = 'Visitor left a package on the porch.';
const ALT_TRANSCRIPT_SNIPPET =
  'Morning patrol completed without incidents.';
const EVENT_ID = 'event-alpha';
const CAMERA_ID = 'camera-1';
const EVENT_WITHOUT_TRANSCRIPT_ID = 'event-beta';
const CAMERA_WITHOUT_TRANSCRIPT = 'camera-2';
const ALT_EVENT_ID = 'event-gamma';
const ALT_CAMERA_ID = 'camera-3';
const SEARCH_TERM = 'package';

type LoadOptions = {
  dateStart?: string;
  dateEnd?: string;
  cameraId?: string | null;
  query?: string;
};

async function loadSearchPage(
  page: Page,
  {
    dateStart = EVENT_DATE,
    dateEnd = EVENT_DATE,
    cameraId = CAMERA_ID,
    query = '',
  }: LoadOptions = {},
) {
  await page.goto('/events/search');

  await page.waitForSelector('#camera-filter option[value="camera-1"]', {
    state: 'attached',
  });

  await page.locator('#date-start').fill(dateStart);
  await page.locator('#date-end').fill(dateEnd);

  if (query) {
    await page.locator('#search-filter').fill(query);
  } else {
    await page.locator('#search-filter').fill('');
  }

  if (cameraId !== null) {
    await page.selectOption('#camera-filter', cameraId ?? '');
  }

  await page.click('#search-submit');
  await expect(page.locator('#event-list')).not.toContainText('Loading events...');

  const eventList = page.locator('.event-item');

  if (cameraId) {
    const eventItem = page
      .locator('.event-item', { hasText: cameraId })
      .first();
    await expect(eventItem).toBeVisible();
    return { eventItem, eventList };
  }

  return { eventList };
}

async function loadLatestPage(
  page: Page,
  { dateStart = EVENT_DATE, dateEnd = EVENT_DATE }: LoadOptions = {},
) {
  await page.goto('/events/latest');

  await page.waitForSelector('#camera-filter option[value="camera-1"]', {
    state: 'attached',
  });

  await page.selectOption('#range-preset', 'custom');
  await page.locator('#date-start').fill(dateStart);
  await page.locator('#date-end').fill(dateEnd);
  await page.click('#latest-refresh');

  await expect(page.locator('#event-list')).not.toContainText('Loading events...');

  return page.locator('.event-item');
}

function eventItemByCamera(page: Page, cameraId: string) {
  return page.locator('.event-item', { hasText: cameraId }).first();
}

test.describe('Events Dashboard', () => {
  test('renders results meta without latency when missing', async ({ page }) => {
    const eventsRoute = async (route: Route) => {
      if (route.request().method() !== 'GET') {
        await route.continue();
        return;
      }
      const url = new URL(route.request().url());
      if (url.pathname !== '/api/events') {
        await route.continue();
        return;
      }
      try {
        const response = await route.fetch();
        const body = await response.json();
        const { latency_ms, ...rest } = body as { latency_ms?: number };
        await route.fulfill({
          response,
          body: JSON.stringify(rest),
        });
      } catch (error) {
        if (
          String(error).includes(
            'Target page, context or browser has been closed',
          )
        ) {
          return;
        }
        throw error;
      }
    };

    await page.route('**/api/events?**', eventsRoute);

    try {
      await page.goto('/events/search');
      await page.waitForSelector('#camera-filter option[value="camera-1"]', {
        state: 'attached',
      });
      await page.locator('#date-start').fill(EVENT_DATE);
      await page.locator('#date-end').fill(EVENT_DATE);
      await page.selectOption('#camera-filter', CAMERA_ID);
      await page.click('#search-submit');

      const resultsMeta = page.locator('#results-meta');
      await expect(resultsMeta).toContainText('events shown');
      await expect(resultsMeta).not.toContainText('ms');
    } finally {
      await page.unroute('**/api/events?**', eventsRoute);
    }
  });

  test('latest page loads seeded events for a custom range', async ({ page }) => {
    const eventList = await loadLatestPage(page, {
      dateStart: EVENT_DATE,
      dateEnd: EVENT_DATE,
    });

    await expect
      .poll(async () => eventList.count())
      .toBeGreaterThan(1);
    await expect(page.locator('#event-list')).toContainText(CAMERA_ID);
    await expect(page.locator('#event-list')).toContainText(CAMERA_WITHOUT_TRANSCRIPT);
  });

  test('loads seeded event and displays transcript', async ({ page }) => {
    const { eventItem } = await loadSearchPage(page);

    await eventItem.click();

    const transcriptSegment = page.locator('.transcript-segment').first();
    await expect(transcriptSegment).toContainText(TRANSCRIPT_SNIPPET);

    await expect(page.locator('#video-title')).toContainText(CAMERA_ID);
    await expect(page.locator('body')).toContainText('Transcript');
  });

  test('shows transcript availability indicator on events', async ({ page }) => {
    const { eventItem } = await loadSearchPage(page);

    const transcriptIndicator = eventItem.locator('.transcript-indicator');
    await expect(transcriptIndicator).toBeVisible();
    await expect(transcriptIndicator).toHaveAttribute('title', 'Transcript available');
  });

  test('sets video and caption tracks when selecting an event', async ({ page }) => {
    const { eventItem } = await loadSearchPage(page);

    await eventItem.click();

    const videoPlayer = page.locator('#video-player');
    await expect(videoPlayer).toHaveAttribute(
      'src',
      new RegExp(`/video/${EVENT_ID}$`),
    );

    const captionTrack = videoPlayer.locator('track[kind="captions"]');
    await expect(captionTrack).toHaveAttribute('src', `/api/captions/${EVENT_ID}`);
    await expect(captionTrack).toHaveAttribute('default', '');

    const storyboardTrack = videoPlayer.locator('track[kind="metadata"][label="thumbnails"]');
    await expect(storyboardTrack).toHaveAttribute('src', `/api/storyboard/vtt/${EVENT_ID}`);
  });

  test('filters events by camera, time, and resets filters', async ({ page }) => {
    await loadSearchPage(page, { cameraId: null });

    const eventList = page.locator('.event-item');
    await expect(eventList).toHaveCount(2);

    await page.locator('#date-start').fill(EVENT_DATE);
    await page.locator('#date-end').fill(EVENT_DATE);
    await page.locator('#search-filter').fill('');
    await page.selectOption('#camera-filter', CAMERA_WITHOUT_TRANSCRIPT);
    await page.click('#search-submit');
    await expect(eventList).toHaveCount(1);
    await expect(eventList.first()).toContainText(CAMERA_WITHOUT_TRANSCRIPT);

    await page.selectOption('#camera-filter', '');
    await page.click('#search-submit');
    await expect(eventList).toHaveCount(2);

    const timeStart = page.locator('#time-start');
    const timeEnd = page.locator('#time-end');
    await timeStart.fill('12:00');
    await timeEnd.fill('23:59');
    await page.click('#search-submit');

    await expect(eventList).toHaveCount(1);
    await expect(eventList.first()).toContainText(CAMERA_WITHOUT_TRANSCRIPT);

    await timeStart.fill('');
    await timeEnd.fill('');
    await page.click('#search-submit');
    await expect(eventList).toHaveCount(2);

    await page.locator('#date-start').fill(ALT_EVENT_DATE);
    await page.locator('#date-end').fill(ALT_EVENT_DATE);
    await page.click('#search-submit');
    await expect(eventList).toHaveCount(1);
    await expect(eventList.first()).toContainText(ALT_CAMERA_ID);

    await page.click('#search-reset');
    const today = new Date();
    const endDate = today.toISOString().split('T')[0];
    const startDate = new Date(today);
    startDate.setDate(startDate.getDate() - 29);
    const startValue = startDate.toISOString().split('T')[0];

    await expect(page.locator('#date-start')).toHaveValue(startValue);
    await expect(page.locator('#date-end')).toHaveValue(endDate);
    await expect(page.locator('#camera-filter')).toHaveValue('');
    await expect(page.locator('#time-start')).toHaveValue('');
    await expect(page.locator('#time-end')).toHaveValue('');
    await expect(page.locator('#search-filter')).toHaveValue('');
    await expect(page.locator('#event-list')).toContainText(
      'Run a search to see results.',
    );
  });

  test('filters events by transcript search', async ({ page }) => {
    const { eventList } = await loadSearchPage(page, {
      cameraId: null,
      query: SEARCH_TERM,
    });

    await expect(eventList).toHaveCount(1);
    await expect(eventList.first()).toContainText(CAMERA_ID);
    await expect(eventList.first()).toContainText('Transcript available');
    await expect(eventList.first().locator('mark').first()).toContainText(
      SEARCH_TERM,
    );
    const resultsMeta = page.locator('#results-meta');
    await expect(resultsMeta).toContainText('ms');
  });

  test('handles events without transcripts gracefully', async ({ page }) => {
    const { eventItem } = await loadSearchPage(page, {
      cameraId: CAMERA_WITHOUT_TRANSCRIPT,
    });

    await expect(eventItem.locator('.transcript-indicator')).toHaveCount(0);

    await eventItem.click();

    await expect(page.locator('#video-title')).toContainText('Transcription pending');
    await expect(page.locator('.transcript-placeholder')).toHaveText(
      'No transcript available for this video',
    );
    await expect(
      page.locator('#video-player track[kind="captions"]'),
    ).toHaveCount(0);
  });

  test('shows and hides storyboard preview on hover', async ({ page }) => {
    const { eventItem } = await loadSearchPage(page);

    const preview = eventItem.locator('.event-preview');
    await expect(preview).toBeHidden();

    await eventItem.hover();
    await preview.waitFor({ state: 'visible' });
    await expect(preview.locator('img')).toHaveAttribute(
      'src',
      `/api/storyboard/image/${EVENT_ID}`,
    );

    await page.mouse.move(0, 0);
    await expect(preview).toBeHidden();
  });

  test('deep links directly to an event', async ({ page }) => {
    await page.goto(`/events/latest?video=${EVENT_ID}`);

    const videoPlayer = page.locator('#video-player');
    await expect(videoPlayer).toHaveAttribute(
      'src',
      new RegExp(`/video/${EVENT_ID}$`),
    );

    const transcriptSegment = page.locator('.transcript-segment').first();
    await expect(transcriptSegment).toContainText(TRANSCRIPT_SNIPPET);
    await expect(page.locator('#video-title')).toContainText(CAMERA_ID);
  });
});
