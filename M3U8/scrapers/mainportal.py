import asyncio
from functools import partial
from urllib.parse import urljoin

from .utils import Cache, Time, get_logger, leagues, network

log = get_logger(__name__)

urls: dict[str, dict[str, str | float]] = {}

TAG = "MP66"

CACHE_FILE = Cache(TAG, exp=10_800)

API_URLS = {
    sport: f"https://api.{sport.lower()}24all.ir"
    for sport in [
        "MLB",
        # "NBA",
        # "NFL",
        "NHL",
    ]
}

BASE_URLS = {sport: url.replace("api.", "") for sport, url in API_URLS.items()}


async def process_event(
    sport: str,
    flavor_id: str,
    media_id: int,
    url_num: int,
) -> str | None:

    r = await network.client.post(
        urljoin(API_URLS[sport], "api/v2/generate_stream_info"),
        headers={"Referer": BASE_URLS[sport]},
        json={"flavor_id": flavor_id, "media_event_id": media_id},
    )

    if r.status_code != 200:
        log.warning(f"URL {url_num}) Failed to create post request.")
        return

    data: dict[str, str] = r.json()

    if not (m3u8_url := data.get("url")):
        log.warning(f"URL {url_num}) No M3U8 found")
        return

    log.info(f"URL {url_num}) Captured M3U8")

    return m3u8_url


async def get_api_data() -> dict[str, dict[str, list[dict]]]:
    tasks = [
        (
            sport,
            network.request(
                urljoin(url, "api/v2/stateshot"),
                log=log,
            ),
        )
        for sport, url in API_URLS.items()
    ]

    results = await asyncio.gather(*(task for _, task in tasks))

    return {sport: r.json() for (sport, _), r in zip(tasks, results) if r}


async def get_events(cached_keys: list[str]) -> list[dict[str, str]]:
    now = Time.clean(Time.now())

    api_data = await get_api_data()

    events = []

    start_dt = now.delta(hours=-1)
    end_dt = now.delta(minutes=1)

    for sport in api_data:
        data = api_data[sport]

        teams = data.get("teams", {})

        flavors = data.get("flavors", {})

        media_events = data.get("media_events", {})

        team_identifier: dict[int, str] = {t.get("id"): t.get("name") for t in teams}

        event_to_flavor_id: dict[int, str] = {
            event_id: flavor["id"]
            for flavor in flavors
            for event_id in flavor.get("media_event_ids", [])
        }

        parsed_media_events: dict[int, int] = {
            x.get("game_id"): x.get("id") for x in media_events
        }

        for game in data.get("games", {}):
            game_id = game["id"]

            event_dt = Time.fromisoformat(game["datetime"]).to_tz("EST")

            if not start_dt <= event_dt <= end_dt:
                continue

            away = team_identifier.get(game["away_team_id"])
            home = team_identifier.get(game["home_team_id"])

            if f"[{sport}] {(event_name:=f"{away} vs {home}")} ({TAG})" in cached_keys:
                continue

            media_id = parsed_media_events.get(game_id, 0)

            if (flavor_id := event_to_flavor_id.get(media_id)) and (
                flavor_id.lower().startswith("free.live")
            ):
                events.append(
                    {
                        "sport": sport,
                        "event": event_name,
                        "timestamp": event_dt.timestamp(),
                        "flavor_id": flavor_id,
                        "media_id": media_id,
                    }
                )

    return events


async def scrape() -> None:
    cached_urls = CACHE_FILE.load()

    valid_urls = {k: v for k, v in cached_urls.items() if v["url"]}

    valid_count = cached_count = len(valid_urls)

    urls.update(valid_urls)

    log.info(f"Loaded {cached_count} event(s) from cache")

    log.info('Scraping from "https://mainportal66.com"')

    if events := await get_events(cached_urls.keys()):
        log.info(f"Processing {len(events)} new URL(s)")

        for i, ev in enumerate(events, start=1):
            handler = partial(
                process_event,
                sport=(sport := ev["sport"]),
                flavor_id=ev["flavor_id"],
                media_id=ev["media_id"],
                url_num=i,
            )

            url = await network.safe_process(
                handler,
                url_num=i,
                semaphore=network.HTTP_S,
                log=log,
            )

            event, ts = ev["event"], ev["timestamp"]

            key = f"[{sport}] {event} ({TAG})"

            tvg_id, logo = leagues.get_tvg_info(sport, event)

            entry = {
                "url": url,
                "logo": logo,
                "base": BASE_URLS[sport],
                "timestamp": ts,
                "id": tvg_id or "Live.Event.us",
            }

            cached_urls[key] = entry

            if url:
                valid_count += 1

                urls[key] = entry

        log.info(f"Collected and cached {valid_count - cached_count} new event(s)")

    else:
        log.info("No new events found")

    CACHE_FILE.write(cached_urls)
