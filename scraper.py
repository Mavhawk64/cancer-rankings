import asyncio
import json
import os
import re
import time
from collections import deque
from dataclasses import dataclass
from typing import Dict, Optional

from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from playwright.async_api import async_playwright


@dataclass(frozen=True)
class PullSpec:
    key: str
    url_template: str


SPECS = [
    PullSpec(
        key="incidence",
        url_template="https://seer.cancer.gov/statistics-network/explorer/application.html?site={SITE_ID}&data_type=1&graph_type=2&compareBy=sex&chk_sex_3=3&chk_sex_2=2&rate_type=2&race=1&age_range=1&hdn_stage=101&advopt_precision=1&advopt_show_ci=on#resultsRegion0",
    ),
    PullSpec(
        key="mortality",
        url_template="https://seer.cancer.gov/statistics-network/explorer/application.html?site={SITE_ID}&data_type=2&graph_type=2&compareBy=sex&chk_sex_3=3&chk_sex_2=2&race=1&age_range=1&advopt_precision=1&advopt_show_ci=on",
    ),
    PullSpec(
        key="survival",
        url_template="https://seer.cancer.gov/statistics-network/explorer/application.html?site={SITE_ID}&data_type=4&graph_type=2&compareBy=sex&chk_sex_3=3&chk_sex_2=2&relative_survival_interval=5&race=1&age_range=1&hdn_stage=101&advopt_precision=1&advopt_show_ci=on",
    ),
]


def slugify_site_name(name: str) -> str:
    s = name.strip().lower()
    s = s.replace("&", " and ")
    s = re.sub(r"[’'`]", "", s)
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s)
    return s.strip("_")


async def get_site_name(page) -> Optional[str]:
    sel = "#select2-Select_site-container"
    try:
        el = await page.wait_for_selector(sel, timeout=20_000)
    except PlaywrightTimeoutError:
        return None
    title = await el.get_attribute("title")
    if title and title.strip():
        return title.strip()
    txt = (await el.inner_text()).strip()
    return txt or None


async def download_csv(page, out_path: str) -> bool:
    button_sel = "#dload-data"
    try:
        await page.wait_for_selector(button_sel, timeout=30_000)
    except PlaywrightTimeoutError:
        return False

    try:
        async with page.expect_download(timeout=45_000) as dl_info:
            await page.click(button_sel)
        dl = await dl_info.value
        os.makedirs(os.path.dirname(out_path), exist_ok=True)
        await dl.save_as(out_path)
        return True
    except PlaywrightTimeoutError:
        return False


async def get_all_site_ids(page) -> list[int]:
    bootstrap_url = SPECS[0].url_template.format(SITE_ID=0)
    await page.goto(bootstrap_url, wait_until="domcontentloaded", timeout=45_000)

    await page.click("#select2-Select_site-container")
    await page.wait_for_selector("#select2-Select_site-results", timeout=20_000)

    def _extract_script() -> str:
        return """
        () => {
            const els = Array.from(document.querySelectorAll('#select2-Select_site-results li.select2-results__option[id]'));
            const ids = [];
            for (const el of els) {
                const m = el.id.match(/-(\\d+)$/);
                if (m) ids.push(parseInt(m[1], 10));
            }
            return ids;
        }
        """

    seen: set[int] = set()
    stable_rounds = 0

    while stable_rounds < 6:
        ids = await page.evaluate(_extract_script())
        before = len(seen)
        for x in ids:
            seen.add(x)
        after = len(seen)

        stable_rounds = stable_rounds + 1 if after == before else 0

        await page.evaluate(
            """
            () => {
                const ul = document.querySelector('#select2-Select_site-results');
                if (!ul) return;
                const parent = ul.parentElement;
                const scroller = parent && parent.classList.contains('select2-results')
                    ? parent
                    : ul.closest('.select2-results') || ul;
                scroller.scrollTop = scroller.scrollHeight;
            }
            """
        )
        await page.wait_for_timeout(250)

    await page.keyboard.press("Escape")
    return sorted(seen)


def file_ok(path: str) -> bool:
    return os.path.exists(path) and os.path.getsize(path) > 0


def is_site_complete(out_dir: str, slug: str) -> bool:
    for spec in SPECS:
        p = os.path.join(out_dir, spec.key, f"{slug}.csv")
        if not file_ok(p):
            return False
    return True


async def pull_one_site(
    page, site_id: int, out_dir: str, name_map: Dict[str, str]
) -> bool:
    first_url = SPECS[0].url_template.format(SITE_ID=site_id)
    try:
        await page.goto(first_url, wait_until="domcontentloaded", timeout=45_000)
    except PlaywrightTimeoutError:
        return False

    site_name = await get_site_name(page)
    if not site_name:
        return False

    slug = slugify_site_name(site_name)
    name_map.setdefault(slug, site_name)

    if is_site_complete(out_dir, slug):
        print(f"   SKIP (complete): {slug}", flush=True)
        return True

    ok_any = False

    for spec in SPECS:
        out_path = os.path.join(out_dir, spec.key, f"{slug}.csv")

        if file_ok(out_path):
            ok_any = True
            continue

        url = spec.url_template.format(SITE_ID=site_id)
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=45_000)
        except PlaywrightTimeoutError:
            continue

        ok = await download_csv(page, out_path)
        ok_any = ok_any or ok

    return ok_any


def fmt_seconds(seconds: float) -> str:
    if seconds <= 0:
        return "0s"
    s = int(seconds)
    h, rem = divmod(s, 3600)
    m, s = divmod(rem, 60)
    if h > 0:
        return f"{h}h{m:02d}m{s:02d}s"
    if m > 0:
        return f"{m}m{s:02d}s"
    return f"{s}s"


async def main(
    out_dir: str = "out",
    stop_after_consecutive_misses: int = 30,
) -> None:
    os.makedirs(out_dir, exist_ok=True)
    for spec in SPECS:
        os.makedirs(os.path.join(out_dir, spec.key), exist_ok=True)

    name_map_path = os.path.join(out_dir, "site_name_map.json")
    progress_path = os.path.join(out_dir, "progress.json")

    name_map: Dict[str, str] = {}
    if os.path.exists(name_map_path):
        with open(name_map_path, "r", encoding="utf-8") as f:
            name_map = json.load(f)

    progress: Dict[str, Optional[int]] = {"last_site_id": None}
    if os.path.exists(progress_path):
        with open(progress_path, "r", encoding="utf-8") as f:
            progress = json.load(f)

    misses = 0

    # last 3 successful site durations (seconds)
    last3 = deque(maxlen=3)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(accept_downloads=True)
        page = await context.new_page()

        site_ids = await get_all_site_ids(page)
        total = len(site_ids)

        start_idx = 0
        last_site = progress.get("last_site_id")
        if last_site is not None and last_site in site_ids:
            start_idx = site_ids.index(last_site) + 1

        t0 = time.time()

        for idx, site_id in enumerate(site_ids[start_idx:], start=start_idx):
            completed = idx
            remaining = total - completed
            elapsed = time.time() - t0
            percent = (completed / total) * 100 if total > 0 else 0.0

            # ETA based on last 3 completed sites
            if len(last3) > 0:
                avg = sum(last3) / len(last3)
                eta = avg * remaining
                eta_str = fmt_seconds(eta)
                avg_str = f"{avg:.2f}s/site(3)"
            else:
                eta_str = "n/a"
                avg_str = "n/a"

            print(
                f"[{completed}/{total}] {percent:6.2f}% | "
                f"site={site_id} | remaining={remaining} | "
                f"elapsed={fmt_seconds(elapsed)} | ETA={eta_str} | avg={avg_str}",
                flush=True,
            )

            site_start = time.time()
            ok = await pull_one_site(page, site_id, out_dir, name_map)
            site_dur = time.time() - site_start

            if ok:
                misses = 0
                last3.append(site_dur)
            else:
                misses += 1
                if misses >= stop_after_consecutive_misses:
                    print(f"Stopping after {misses} consecutive misses.", flush=True)
                    break

            progress["last_site_id"] = site_id
            with open(progress_path, "w", encoding="utf-8") as f:
                json.dump(progress, f, indent=2)

        await context.close()
        await browser.close()

    with open(name_map_path, "w", encoding="utf-8") as f:
        json.dump(name_map, f, ensure_ascii=False, indent=2, sort_keys=True)


if __name__ == "__main__":
    asyncio.run(main())
