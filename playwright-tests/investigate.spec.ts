import { expect, test } from '@playwright/test';

test.describe('Investigate Chat', () => {
  test('streams progress and renders markdown + evidence without real bedrock', async ({
    page,
  }) => {
    await page.goto('/investigate');

    const input = page.locator('#chat-input');
    await input.fill('what happened with the package?');
    await page.locator('#send-button').click();

    await expect(page.locator('.chat-row.user .chat-bubble').last()).toContainText(
      'what happened with the package?',
    );

    const progress = page.locator('.tool-progress');
    await expect(progress).toBeVisible();
    await expect(progress.locator('.tool-current')).not.toHaveText('');
    await expect(progress.locator('summary')).toContainText('Show full tool history');

    await expect(progress.locator('.badge')).toHaveText('Complete');
    await expect(progress.locator('summary')).toContainText(/\([1-9]\d*\)/);

    const assistant = page.locator('.chat-row.assistant .chat-bubble').last();
    await expect(assistant.locator('h3')).toContainText('Findings');
    await expect(assistant).toContainText('package');
    await expect(assistant.locator('.evidence-card')).toHaveCount(1);
    await expect(assistant.locator('.evidence-card a')).toHaveAttribute(
      'href',
      /\/video\/event-alpha#t=\d+/,
    );
  });
});
