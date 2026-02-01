import { expect, test } from '@playwright/test';
import type { Page } from '@playwright/test';

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
  date?: string;
  cameraId?: string | null;
};

async function loadEventsPage(
  page: Page,
  { date = EVENT_DATE, cameraId = CAMERA_ID }: LoadOptions = {},
) {
  await page.goto('/events');

  await page.waitForSelector('#camera-filter option[value="camera-1"]', {
    state: 'attached',
  });

  const dateFilter = page.locator('#date-filter');
  await dateFilter.fill(date);
  await dateFilter.dispatchEvent('change');

  await expect(
    page.locator('#event-list'),
  ).not.toContainText('Loading events...');

  const eventList = page.locator('.event-item');

  if (cameraId) {
    const eventItem = page
      .locator('.event-item', { hasText: cameraId })
      .first();
    await expect(eventItem).toBeVisible();
    return { eventItem, eventList, dateFilter };
  }

  return { eventList, dateFilter };
}

function eventItemByCamera(page: Page, cameraId: string) {
  return page.locator('.event-item', { hasText: cameraId }).first();
}

test.describe('Events Dashboard', () => {
  test('loads seeded event and displays transcript', async ({ page }) => {
    const { eventItem } = await loadEventsPage(page);

    await eventItem.click();

    const transcriptSegment = page.locator('.transcript-segment').first();
    await expect(transcriptSegment).toContainText(TRANSCRIPT_SNIPPET);

    await expect(page.locator('#video-title')).toContainText(CAMERA_ID);
    await expect(page.locator('body')).toContainText('Transcript');
  });

  test('shows transcript availability indicator on recent events', async ({ page }) => {
    const { eventItem } = await loadEventsPage(page);

    const transcriptIndicator = eventItem.locator('.transcript-indicator');
    await expect(transcriptIndicator).toBeVisible();
    await expect(transcriptIndicator).toHaveAttribute('title', 'Transcript available');
  });

  test('sets video and caption tracks when selecting an event', async ({ page }) => {
    const { eventItem } = await loadEventsPage(page);

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
    const { eventList, dateFilter } = await loadEventsPage(page, {
      cameraId: null,
    });

    await expect(eventList).toHaveCount(2);

    await page.selectOption('#camera-filter', CAMERA_WITHOUT_TRANSCRIPT);
    await expect(eventList).toHaveCount(1);
    await expect(eventList.first()).toContainText(CAMERA_WITHOUT_TRANSCRIPT);

    await page.selectOption('#camera-filter', '');
    await expect(eventList).toHaveCount(2);

    const timeStart = page.locator('#time-start');
    const timeEnd = page.locator('#time-end');
    await timeStart.fill('12:00:00');
    await timeStart.dispatchEvent('change');
    await timeEnd.fill('23:59:59');
    await timeEnd.dispatchEvent('change');

    await expect(eventList).toHaveCount(1);
    await expect(eventList.first()).toContainText(CAMERA_WITHOUT_TRANSCRIPT);

    await timeStart.fill('');
    await timeStart.dispatchEvent('change');
    await timeEnd.fill('');
    await timeEnd.dispatchEvent('change');
    await expect(eventList).toHaveCount(2);

    await dateFilter.fill(ALT_EVENT_DATE);
    await dateFilter.dispatchEvent('change');
    await expect(eventList).toHaveCount(1);
    await expect(eventList.first()).toContainText(ALT_CAMERA_ID);

    await page.click('#reset-filters');
    const today = await page.evaluate(
      () => new Date().toISOString().split('T')[0],
    );
    await expect(dateFilter).toHaveValue(today);
    await expect(page.locator('#camera-filter')).toHaveValue('');
    await expect(page.locator('#time-start')).toHaveValue('');
    await expect(page.locator('#time-end')).toHaveValue('');
    await expect(page.locator('#search-filter')).toHaveValue('');
    await expect(eventList).toHaveCount(0);
    await expect(page.locator('#event-list')).toContainText('No events available');
  });

  test('filters events by transcript search', async ({ page }) => {
    const { eventList } = await loadEventsPage(page, { cameraId: null });

    await expect(eventList).toHaveCount(2);

    const searchInput = page.locator('#search-filter');
    await searchInput.fill(SEARCH_TERM);

    await expect(eventList).toHaveCount(1);
    await expect(eventList.first()).toContainText(CAMERA_ID);
    await expect(eventList.first()).toContainText('Transcript available');
  });

  test('handles events without transcripts gracefully', async ({ page }) => {
    const { eventItem } = await loadEventsPage(page, {
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
    const { eventItem } = await loadEventsPage(page);

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
    await page.goto(`/events?video=${EVENT_ID}`);

    const videoPlayer = page.locator('#video-player');
    await expect(videoPlayer).toHaveAttribute(
      'src',
      new RegExp(`/video/${EVENT_ID}$`),
    );

    const transcriptSegment = page.locator('.transcript-segment').first();
    await expect(transcriptSegment).toContainText(TRANSCRIPT_SNIPPET);
    await expect(page.locator('#video-title')).toContainText(CAMERA_ID);
  });

  test('transcript controls respond to user interaction', async ({ page }) => {
    const { eventItem } = await loadEventsPage(page);

    await eventItem.click();

    const autoScrollToggle = page.locator('#auto-scroll-toggle');
    await autoScrollToggle.uncheck();
    await expect(autoScrollToggle).not.toBeChecked();

    const thumbnailToggle = page.locator('#thumbnail-size-toggle');
    const transcriptContainer = page.locator('#transcript-container');
    await thumbnailToggle.click();
    await expect(transcriptContainer).toHaveClass(/large-thumbnails/);

    await page.evaluate(() => {
      const global = window as unknown as Record<string, unknown>;
      const original = global.seekToTime as ((time: number) => void) | undefined;
      const calls: number[] = [];
      global.__seekInvocations = calls;
      global.seekToTime = ((time: number) => {
        calls.push(time);
        if (typeof original === 'function') {
          original.call(global, time);
        }
      }) as unknown;
    });

    const timestamps = page.locator('.transcript-timestamp');
    await expect(timestamps).toHaveCount(2);
    const timestamp = timestamps.nth(1);
    const onclickAttr = (await timestamp.getAttribute('onclick')) ?? '';
    const match = onclickAttr.match(/seekToTime\(([\d.]+)\)/);
    expect(match).not.toBeNull();
    const targetTime = match ? parseFloat(match[1]) : 0;

    await timestamp.click();
    const seekInvocations = await page.evaluate(() => {
      const global = window as unknown as Record<string, unknown>;
      return (global.__seekInvocations as number[]) ?? [];
    });
    expect(seekInvocations).toContain(targetTime);

    await thumbnailToggle.click();
    await expect(transcriptContainer).not.toHaveClass(/large-thumbnails/);
    await autoScrollToggle.check();
    await expect(autoScrollToggle).toBeChecked();
  });

  test('renders transcript for alternate event when date changes', async ({ page }) => {
    const { dateFilter } = await loadEventsPage(page, {
      date: ALT_EVENT_DATE,
      cameraId: ALT_CAMERA_ID,
    });

    const eventItem = eventItemByCamera(page, ALT_CAMERA_ID);
    await eventItem.click();

    const transcriptSegment = page.locator('.transcript-segment').first();
    await expect(transcriptSegment).toContainText(ALT_TRANSCRIPT_SNIPPET);
    await expect(dateFilter).toHaveValue(ALT_EVENT_DATE);
  });
});
