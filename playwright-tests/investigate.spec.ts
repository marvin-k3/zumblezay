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
  test('supports multiline input with Shift+Enter and submits on Enter', async ({ page }) => {
    await page.goto('/investigate');
    await expect(page).toHaveURL(/\/investigate$/);

    const input = page.locator('#chat-input');
    await input.fill('first line');
    await input.press('Shift+Enter');
    await input.type('second line');
    await expect(input).toHaveValue('first line\nsecond line');

    await input.press('Enter');
    await expect(page).toHaveURL(/\/investigate\/chat_/);
    const lastUserBubble = page.locator('.chat-row.user .chat-bubble').last();
    await expect(lastUserBubble).toContainText('first line');
    await expect(lastUserBubble).toContainText('second line');
  });

  test('creates bookmarkable chat URLs and restores persisted conversation', async ({ page }) => {
    await page.goto('/investigate');

    await expect(page).toHaveURL(/\/investigate$/);

    const input = page.locator('#chat-input');
    await input.fill('what happened with the package?');
    await page.locator('#send-button').click();
    await expect(page).toHaveURL(/\/investigate\/chat_/);
    const currentUrl = new URL(page.url());
    const chatId = currentUrl.pathname.split('/').pop();
    expect(chatId).toBeTruthy();

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
    await expect(assistant.locator('.evidence-card .evidence-link')).toHaveAttribute(
      'href',
      /\/events\/latest\?video=event-alpha/,
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

  test('keeps scroll position when scrolled up and allows stopping a streaming run', async ({
    page,
    request,
  }) => {
    const createChatResponse = await request.post('/api/chats');
    expect(createChatResponse.ok()).toBeTruthy();
    const { chat_id: chatId } = (await createChatResponse.json()) as { chat_id: string };

    await page.addInitScript(({ activeChatId }) => {
      const originalFetch = window.fetch.bind(window);

      function jsonResponse(payload: unknown) {
        return new Response(JSON.stringify(payload), {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        });
      }

      (window as unknown as { __mockAborted?: boolean }).__mockAborted = false;

      window.fetch = async (input: RequestInfo | URL, init?: RequestInit) => {
        const url = typeof input === 'string' ? input : input instanceof URL ? input.toString() : input.url;
        const parsed = new URL(url, window.location.origin);
        const path = parsed.pathname;

        if (path === '/api/chats') {
          return jsonResponse([]);
        }

        if (path === `/api/chats/${activeChatId}`) {
          return jsonResponse({
            session: { id: activeChatId, title: 'Mock chat', created_at: 0, updated_at: 0, last_message_at: 0 },
            messages: [
              {
                id: 1,
                role: 'assistant',
                content: Array.from({ length: 120 }, (_, i) => `line ${i + 1}`).join('\n'),
                evidence: [],
                run_id: null,
                created_at: 0,
              },
            ],
            active_run: null,
          });
        }

        if (path === `/api/chats/${activeChatId}/messages` && (init?.method ?? 'GET') === 'POST') {
          return jsonResponse({ run_id: 'run-mock', status: 'queued' });
        }

        if (path === `/api/chats/${activeChatId}/runs/run-mock/stream`) {
          const stream = new ReadableStream({
            start(controller) {
              let counter = 0;
              const timer = window.setInterval(() => {
                const payload = `event: answer_delta\ndata: ${JSON.stringify({ delta: `chunk_${counter} ` })}\n\n`;
                controller.enqueue(new TextEncoder().encode(payload));
                counter += 1;
              }, 70);

              const abortSignal = init?.signal;
              if (abortSignal) {
                abortSignal.addEventListener(
                  'abort',
                  () => {
                    (window as unknown as { __mockAborted?: boolean }).__mockAborted = true;
                    window.clearInterval(timer);
                    controller.close();
                  },
                  { once: true },
                );
              }
            },
          });

          return new Response(stream, {
            status: 200,
            headers: { 'Content-Type': 'text/event-stream' },
          });
        }

        return originalFetch(input, init);
      };
    }, { activeChatId: chatId });

    await page.goto(`/investigate/${chatId}`);
    const messages = page.locator('#chat-messages');
    const input = page.locator('#chat-input');

    await input.fill('stream this');
    await page.locator('#send-button').click();
    await expect(page.locator('#stop-button')).toBeVisible();

    await expect.poll(async () => {
      return page.evaluate(() => {
        const el = document.getElementById('chat-messages');
        return el ? el.scrollHeight : 0;
      });
    }).toBeGreaterThan(0);

    await page.evaluate(() => {
      const el = document.getElementById('chat-messages');
      if (!el) return;
      el.scrollTop = 0;
    });
    const scrollBefore = await page.evaluate(() => {
      const el = document.getElementById('chat-messages');
      return el ? el.scrollTop : -1;
    });

    await page.waitForTimeout(450);
    const scrollAfter = await page.evaluate(() => {
      const el = document.getElementById('chat-messages');
      return el ? el.scrollTop : -1;
    });
    expect(Math.abs(scrollAfter - scrollBefore)).toBeLessThanOrEqual(5);

    await page.locator('#stop-button').click();
    await expect(page.locator('#chat-status')).toContainText('Stopped');
    await expect(page.locator('#send-button')).toBeEnabled();
    await expect
      .poll(async () => page.evaluate(() => (window as unknown as { __mockAborted?: boolean }).__mockAborted))
      .toBeTruthy();
    await expect(messages).toBeVisible();
  });

  test('shows assistant action bar and evidence reference affordances', async ({ page }) => {
    await page.goto('/investigate');
    await expect(page).toHaveURL(/\/investigate$/);

    const question = 'what happened with the package?';
    await page.locator('#chat-input').fill(question);
    await page.locator('#send-button').click();
    await expect(page).toHaveURL(/\/investigate\/chat_/);
    await expect(page.locator('#chat-status')).toHaveText('Complete');

    const assistantRow = page.locator('.chat-row.assistant').last();
    const assistantBubble = assistantRow.locator('.chat-bubble');
    const actionBar = assistantRow.locator('.chat-message-actions');
    await assistantRow.hover();
    await expect(actionBar).toBeVisible();
    await expect(actionBar.getByRole('button', { name: 'Copy' })).toBeVisible();
    await expect(actionBar.getByRole('button', { name: 'Edit prompt' })).toBeVisible();
    await expect(actionBar.getByRole('button', { name: 'Regenerate' })).toBeVisible();

    await actionBar.getByRole('button', { name: 'Edit prompt' }).click();
    await expect(page.locator('#chat-input')).toHaveValue(question);

    const refButtons = assistantBubble.locator('.evidence-reference-links button');
    await expect(refButtons).toHaveCount(1);
    await refButtons.first().click();
    await expect(assistantBubble.locator('.evidence-card.highlight')).toHaveCount(1);

    const userBefore = await page.locator('.chat-row.user .chat-bubble').count();
    await actionBar.getByRole('button', { name: 'Regenerate' }).click();
    await expect.poll(async () => page.locator('.chat-row.user .chat-bubble').count()).toBe(userBefore + 1);
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
    await expect(page).toHaveURL(/\/investigate$/);

    const input = page.locator('#chat-input');
    await input.fill('what happened with the package? include the detailed evidence and timeline');
    await page.locator('#send-button').click();
    await expect(page).toHaveURL(/\/investigate\/chat_/);

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
