import asyncio
import gzip
import os
import urllib.request
from concurrent.futures import ProcessPoolExecutor
from urllib.parse import urlparse, parse_qs

import aiofiles
import aiohttp
from lxml import etree
# noinspection PyProtectedMember
from lxml.etree import _Element
from tqdm import tqdm

MAX_WORKERS = 10
ROBOTS_TXT_URL = "https://play.google.com/robots.txt"
# noinspection HttpUrlsUsage
SITEMAP_NS = {"sitemap": "http://www.sitemaps.org/schemas/sitemap/0.9"}


async def read_sitemap_loc_urls(session: aiohttp.ClientSession, url: str) -> list[str]:
    async with session.get(url) as response:
        tree: _Element = etree.fromstring(await response.read())
        elements = tree.xpath(".//sitemap:loc", namespaces=SITEMAP_NS)
        return [e.text for e in elements]


def get_url_file_name(url: str) -> str:
    return os.path.basename(urlparse(url).path)


# noinspection PyBroadException
def fetch_app_ids_task(url: str, app_ids_dir: str) -> bool:
    output_path = os.path.join(app_ids_dir, get_url_file_name(url) + ".txt")
    try:
        with urllib.request.urlopen(url) as response:
            content = gzip.decompress(response.read())
        tree: _Element = etree.fromstring(content)
        gp_urls: set[str] = set([element.attrib["href"] for element in tree.iter() if "href" in element.attrib])
        app_ids = [parse_qs(urlparse(i).query)["id"][0] for i in gp_urls if i.startswith("https://play.google.com/store/apps")]
        with open(output_path, "w", encoding="utf-8") as f:
            f.writelines([i + "\n" for i in app_ids])
        return True
    except:
        if os.path.isfile(output_path):
            os.remove(output_path)
        return False


async def run_fetch_app_ids_tasks(urls: list[str], app_ids_dir: str):
    current_looper = asyncio.get_event_loop()
    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        with tqdm(total=len(urls)) as pbar:
            tasks = []
            for url in urls:
                task = current_looper.run_in_executor(executor, fetch_app_ids_task, url, app_ids_dir)
                task.add_done_callback(lambda _: pbar.update(1))
                tasks.append(task)
            task_results = await asyncio.gather(*tasks)
            failed_task_size = task_results.count(False)
            print()
            if failed_task_size > 0:
                print(f"{failed_task_size} failed tasks")
            else:
                print(f"All {len(tasks)} tasks done")


async def concat_all_app_ids(app_ids_dir: str, app_ids_path: str):
    all_app_ids_files = [os.path.join(app_ids_dir, i) for i in os.listdir(app_ids_dir) if not i.startswith(".") and i.endswith(".txt")]
    all_app_ids_files = sorted(all_app_ids_files)
    async with aiofiles.open(app_ids_path, "w") as f_write:
        for file_path in tqdm(all_app_ids_files):
            async with aiofiles.open(file_path, "r") as f_read:
                await f_write.write(await f_read.read())


async def fetch_app_ids(sitemaps_url: list[str], sitemaps_path: str, app_ids_dir: str, app_ids_path: str):
    async with aiohttp.ClientSession() as client:
        print("Loading sitemap urls")
        sitemap_parts_url = set()
        if os.path.exists(sitemaps_path):
            async with aiofiles.open(sitemaps_path, "r", encoding="utf-8") as f:
                sitemap_parts_url.update([line.strip() for line in await f.readlines()])
        else:
            for sitemap_url in tqdm(sitemaps_url):
                sitemap_parts_url.update(await read_sitemap_loc_urls(client, sitemap_url))
            async with aiofiles.open(sitemaps_path, "w", encoding="utf-8") as f:
                await f.writelines([line + "\n" for line in sorted(sitemap_parts_url)])
            print()
    if os.path.isdir(app_ids_dir):
        print("Checking records")
        record_names = set([i[:-len(".txt")] for i in os.listdir(app_ids_dir) if not i.startswith(".") and i.endswith(".txt")])
        sitemap_parts_url = [i for i in sitemap_parts_url if get_url_file_name(i) not in record_names]
    print("Fetching app ids")
    if len(sitemap_parts_url) > 0:
        await run_fetch_app_ids_tasks(sorted(sitemap_parts_url), app_ids_dir)
        print()
    print("Combining app ids")
    if not os.path.isfile(app_ids_path):
        await concat_all_app_ids(app_ids_dir, app_ids_path)
        print()
    print("Done")


async def fetch_sitemaps_url() -> list[str]:
    result = []
    async with aiohttp.ClientSession() as client:
        async with client.get(ROBOTS_TXT_URL) as response:
            async for raw_line in response.content:
                line = raw_line.decode().strip()
                if line.startswith("Sitemap: "):
                    result.append(line[len("Sitemap: "):].strip())
    return result


async def main():
    output_dir = "GPAppId"
    if not os.path.isdir(output_dir):
        os.makedirs(output_dir)
    sitemaps_path = os.path.join(output_dir, "sitemaps.txt")

    # Dynamic task name
    # task_name = str(int(time.time()))
    task_name = "main"

    app_ids_dir = os.path.join(output_dir, f"app_ids_{task_name}")
    app_ids_path = os.path.join(output_dir, f"app_ids_{task_name}.txt")

    if not os.path.isdir(app_ids_dir):
        os.makedirs(app_ids_dir)

    print(f"Task: {task_name}")
    print(f"Cache dir: {app_ids_dir}")
    print(f"Output file: {app_ids_path}")

    print("Fetching sitemaps url")
    sitemaps_url = await fetch_sitemaps_url()

    print("Fetching app ids")
    await fetch_app_ids(sitemaps_url, sitemaps_path, app_ids_dir, app_ids_path)


if __name__ == '__main__':
    looper = asyncio.get_event_loop()
    try:
        looper.run_until_complete(main())
    except KeyboardInterrupt:
        pass
    finally:
        if not looper.is_closed:
            looper.close()
