import asyncio
import time

from app.core.config import settings

MAKEYOURSTATS_LEAGUES_URL = "https://makeyourstats.com/es/leagues"


def _open_makeyourstats_leagues_sync() -> dict[str, str | None]:
    from playwright.sync_api import sync_playwright

    headless = settings.playwright_headless
    wait_s = settings.playwright_after_load_wait_seconds
    timeout_ms = settings.playwright_page_ready_timeout_ms

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        try:
            page = browser.new_page()
            page.set_default_timeout(timeout_ms)
            page.goto(MAKEYOURSTATS_LEAGUES_URL, wait_until="domcontentloaded")
            page.locator("text=MakeYourStats").first.wait_for(
                state="visible",
                timeout=timeout_ms,
            )
            time.sleep(wait_s)
            return {
                "url": page.url,
                "document_title": page.title(),
            }
        finally:
            browser.close()


async def open_makeyourstats_leagues() -> dict[str, str | None]:
    return await asyncio.to_thread(_open_makeyourstats_leagues_sync)
