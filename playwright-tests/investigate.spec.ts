import { expect, test } from '@playwright/test';
import fs from 'node:fs/promises';
import path from 'node:path';

async function waitForRunDone(request: Parameters<typeof test>[0]['request'], chatId: string, runId: string) {
  await expect
    .poll(
      async () => {
        const response = await request.get(`/api/chats/${chatId}/runs/${runId}`);
        if (!response.ok()) {
          return `http_${response.status()}`;
        }
        const payload = await response.json();
        return payload.run?.status ?? 'missing';
      },
      { timeout: 20_000, intervals: [150, 300, 500, 1000] },
    )
    .toBe('done');
}

test.describe('Investigate Chat', () => {
  test('creates bookmarkable chat URLs and restores persisted conversation', async ({ page }) => {
    await page.goto('/investigate');

    await expect(page).toHaveURL(/\/investigate\/chat_/);
    const currentUrl = new URL(page.url());
    const chatId = currentUrl.pathname.split('/').pop();
    expect(chatId).toBeTruthy();

    const input = page.locator('#chat-input');
    await input.fill('what happened with the package?');
    await page.locator('#send-button').click();

    await expect(page.locator('.chat-row.user .chat-bubble').last()).toContainText(
      'what happened with the package?',
    );

    const progress = page.locator('.tool-progress').last();
    await expect(progress).toBeVisible();
    await expect(progress.locator('summary')).toContainText(/\([1-9]\d*\)/);
    await expect(progress).toContainText(/\[(transcript_search|bedrock_synthesis)\]/);

    const assistant = page.locator('.chat-row.assistant .chat-bubble').last();
    await expect(assistant).toContainText('Findings');
    await expect(assistant).toContainText('package');
    await expect(assistant.locator('.evidence-card')).toHaveCount(1);
    await expect(assistant.locator('.evidence-card a')).toHaveAttribute(
      'href',
      /\/video\/event-alpha#t=\d+/,
    );

    await expect(page.locator('#chat-status')).toHaveText('Complete');
    await expect(progress.locator('.badge')).toHaveText('Complete');

    await page.reload();
    await expect(page).toHaveURL(new RegExp(`/investigate/${chatId}$`));
    await expect(page.locator('.chat-row.user .chat-bubble')).toContainText('what happened with the package?');
    await expect(page.locator('.chat-row.assistant .chat-bubble').last()).toContainText('package');
    await expect(page.locator('.chat-row.assistant .chat-bubble').last().locator('.evidence-card')).toHaveCount(1);

    const activeSidebarLink = page.locator('.chat-session-link.active');
    await expect(activeSidebarLink).toHaveAttribute('href', `/investigate/${chatId}`);
  });

  test('continues running in background after stream disconnect and can be resumed', async ({ page, request }) => {
    const createChatResponse = await request.post('/api/chats');
    expect(createChatResponse.ok()).toBeTruthy();
    const createChatPayload = await createChatResponse.json();
    const chatId = createChatPayload.chat_id as string;

    const createMessageResponse = await request.post(`/api/chats/${chatId}/messages`, {
      data: { question: 'summarize the package event' },
    });
    expect(createMessageResponse.ok()).toBeTruthy();
    const createMessagePayload = await createMessageResponse.json();
    const runId = createMessagePayload.run_id as string;

    await page.goto(`/investigate/${chatId}`);
    await page.evaluate(
      async ({ chatId: id, runId: rid }) => {
        const controller = new AbortController();
        const response = await fetch(`/api/chats/${id}/runs/${rid}/stream`, {
          signal: controller.signal,
        });
        setTimeout(() => controller.abort(), 60);
        if (!response.body) return;
        const reader = response.body.getReader();
        try {
          while (true) {
            const { done } = await reader.read();
            if (done) break;
          }
        } catch (_error) {
          // Expected because we abort quickly.
        }
      },
      { chatId, runId },
    );

    await waitForRunDone(request, chatId, runId);

    await page.reload();
    await expect(page.locator('.chat-row.user .chat-bubble')).toContainText('summarize the package event');
    await expect(page.locator('.chat-row.assistant .chat-bubble').last()).toContainText('package');

    const sidebarLink = page.locator(`.chat-session-link[href="/investigate/${chatId}"]`).first();
    await expect(sidebarLink).toBeVisible();
  });

  test('captures mobile iPhone-style screenshot after search completes', async ({ browser }, testInfo) => {
    const context = await browser.newContext({
      viewport: { width: 390, height: 844 },
      isMobile: true,
      hasTouch: true,
      userAgent:
        'Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1',
      deviceScaleFactor: 3,
    });
    const page = await context.newPage();

    await page.goto('/investigate');
    await expect(page).toHaveURL(/\/investigate\/chat_/);

    const input = page.locator('#chat-input');
    await input.fill('what happened with the package? include the detailed evidence and timeline');
    await page.locator('#send-button').click();

    await expect(page.locator('#chat-status')).toHaveText('Complete', { timeout: 30_000 });

    const outputPath = testInfo.outputPath('investigate-mobile-after-search.png');
    await page.screenshot({ path: outputPath, fullPage: true });

    const copyTarget = path.resolve('test-results/investigate-mobile-after-search.png');
    await testInfo.attach('investigate-mobile-after-search', {
      path: outputPath,
      contentType: 'image/png',
    });
    await context.close();

    await fs.mkdir(path.dirname(copyTarget), { recursive: true });
    await fs.copyFile(outputPath, copyTarget);
  });
});
