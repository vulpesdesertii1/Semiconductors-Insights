#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Scraper genérico con BeautifulSoup.
- Lee sitios desde sites.yaml (selectores CSS).
- Respeta robots.txt de forma básica.
- Reintentos con backoff + pausas aleatorias.
- Salida CSV: data/scraped.csv (sin duplicar URLs).

Uso:
  python scraper.py --config sites.yaml --out data/scraped.csv --max-per-site 20 --with-text
"""
import os
import argparse, time, random
from datetime import datetime, timezone
from urllib.parse import urljoin, urlparse
from urllib import robotparser

import requests, yaml, pandas as pd
from bs4 import BeautifulSoup

UA = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:120.0) Gecko/20100101 Firefox/120.0",
]
HDRS = {"Accept": "text/html", "Accept-Language": "es-ES,es;q=0.9,en;q=0.8"}

def polite_get(s, url, rp=None, tries=4, tmin=1.0, tmax=2.5, timeout=15):
    if rp and not rp.can_fetch("*", url):
        raise PermissionError(f"Bloqueado por robots.txt: {url}")
    last_err = None
    for i in range(tries):
        try:
            h = dict(HDRS); h["User-Agent"] = random.choice(UA)
            r = s.get(url, headers=h, timeout=timeout)
            if r.status_code in (429,) or 500 <= r.status_code < 600:
                raise requests.HTTPError(r.status_code)
            r.raise_for_status()
            time.sleep(random.uniform(tmin, tmax))
            return r
        except Exception as e:
            last_err = e
            time.sleep((2**i) + random.random())
    raise last_err

def robots_for(base):
    try:
        p = urlparse(base); robots = f"{p.scheme}://{p.netloc}/robots.txt"
        rp = robotparser.RobotFileParser(); rp.set_url(robots); rp.read(); return rp
    except:
        return None

def extract_first(soup, selector):
    if not selector: return ""
    # Soporta "meta[name='description']::attr(content)"
    if "::attr(" in selector:
        css, rest = selector.split("::attr("); attr = rest.rstrip(")")
        el = soup.select_one(css)
        return (el.get(attr) or "").strip() if el else ""
    el = soup.select_one(selector)
    return el.get_text(" ", strip=True) if el else ""

def parse_list(html, link_sel, base):
    soup = BeautifulSoup(html, "lxml")
    links = set()
    if link_sel:
        for a in soup.select(link_sel.split("::")[0]):
            href = a.get("href") or a.get("data-href") or ""
            if href: links.add(urljoin(base, href))
    else:
        # Fallback: cualquier <a> del mismo dominio
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if href.startswith("/") or urlparse(href).netloc == urlparse(base).netloc:
                links.add(urljoin(base, href))
    return list(links)

def normalize_urls(urls):
    seen = set(); out = []
    for u in urls:
        if not u: continue
        normalized = urlparse(u)._replace(fragment="").geturl()
        if normalized not in seen:
            seen.add(normalized); out.append(normalized)
    return out

def extract_article(s, url, cfg, rp=None, with_text=False):
    try:
        r = polite_get(s, url, rp=rp)
    except Exception as e:
        print(f"[WARN] Artículo KO {url}: {e}")
        return None
    soup = BeautifulSoup(r.text, "lxml")
    title = extract_first(soup, cfg.get("title_selector","h1"))
    date = extract_first(soup, cfg.get("date_selector"))
    author = extract_first(soup, cfg.get("author_selector"))
    summary = extract_first(soup, cfg.get("summary_selector"))
    text = ""
    if with_text and cfg.get("article_text_selector"):
        node = soup.select_one(cfg["article_text_selector"])
        if node:
            for tag in node.find_all(["script","style","noscript","aside","footer","figure"]):
                tag.decompose()
            text = node.get_text(" ", strip=True)
    return {
        "source": cfg.get("name") or urlparse(url).netloc,
        "title": title, "url": url, "date": date, "author": author,
        "summary": summary, "text": text,
        "fetched_at": datetime.now(timezone.utc).isoformat()
    }

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="sites.yaml")
    ap.add_argument("--out", default="data/scraped.csv")
    ap.add_argument("--max-per-site", type=int, default=20)
    ap.add_argument("--with-text", action="store_true")
    args = ap.parse_args()

    # Carga config y canta lo que hay
    with open(args.config, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}
    sites = cfg.get("sites", [])
    print(f"[INFO] Cargados {len(sites)} sitios desde {args.config}")
    for s_ in sites:
        print(f"   - {s_.get('name')} ({s_.get('base_url')})")

    # Carga CSV previo (para evitar duplicados por URL)
    existing = set(); rows = []
    try:
        df_old = pd.read_csv(args.out)
        existing = set(df_old["url"].dropna().tolist())
        rows.extend(df_old.to_dict("records"))
    except Exception:
        pass

    s = requests.Session()

    for site in sites:
        base = site.get("base_url") or (site.get("start_urls") or [""])[0]
        rp = robots_for(base) if base else None

        all_links = []
        for su in site.get("start_urls", []):
            try:
                r = polite_get(s, su, rp=rp)
                links_sel = parse_list(r.text, site.get("article_link_selector"), base or su)
                print(f"[INFO] Listado {su} → {len(links_sel)} links por selector")
                if not links_sel:
                    # Fallback si selector no devuelve nada
                    links_fb = parse_list(r.text, "", base or su)
                    print(f"[INFO] Fallback {su} → {len(links_fb)} links")
                    links_sel = links_fb
                all_links += links_sel
            except Exception as e:
                print(f"[WARN] listado {su}: {e}")

        article_urls = normalize_urls(all_links)
        random.shuffle(article_urls)  # evita siempre los mismos primeros
        article_urls = article_urls[: args.max_per_site]

        print(f"[INFO] {site.get('name', base)} → {len(article_urls)} candidatos (muestra)")
        for u in article_urls[:5]:
            print("   ", u)

        new_count = 0
        for url in article_urls:
            if url in existing:
                continue
            art = extract_article(s, url, site, rp=rp, with_text=args.with_text)
            if not art:
                continue
            rows.append(art); existing.add(url); new_count += 1
            print(f"[OK] {art['title'][:80]}")

        print(f"[INFO] Nuevos añadidos de {site.get('name')}: {new_count}")

    if rows:
        df = pd.DataFrame(rows).sort_values("fetched_at", ascending=False)
        os.makedirs("data", exist_ok=True)
        df.to_csv(args.out, index=False)
        print(f"[DONE] Guardado {len(df)} filas en {args.out}")
    else:
        print("[DONE] No hay nuevas filas.")

if __name__ == "__main__":
    os.makedirs("data", exist_ok=True)
    main()
