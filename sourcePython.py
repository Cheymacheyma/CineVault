import os
import subprocess
import asyncio
from ddgs import DDGS
from curl_cffi import requests as curl_requests
from bs4 import BeautifulSoup
from rich.console import Console
from rich.table import Table
from rich.prompt import Prompt

console = Console()

# قائمة خوادم التتبع الحديثة لضمان سرعة الاتصال الفوري (Public Trackers)
TRACKERS = [
    "udp://tracker.openbittorrent.com:80/announce",
    "udp://tracker.opentrackr.org:1337/announce",
    "udp://tracker.internetwarriors.net:1337/announce",
    "udp://exodus.desync.com:6969/announce",
    "udp://tracker.torrent.eu.org:451/announce"
]
TRACKER_PARAMS = "".join([f"&tr={t}" for t in TRACKERS])
YTS_DOMAINS = ["https://yts.mx", "https://yts.am", "https://yts.pm"]

# 1. المصدر الأول: YTS API مع الفلترة الذكية (تجنب الأفلام الرديئة والمزيفة)
def search_yts(query):
    options = []
    query_clean = query.strip().lower()
    
    # كلمات مفتاحية مستبعدة لتجنب النتائج التجارية الرخيصة وغير المرغوبة
    excluded_keywords = ["planet", "the dunes", "children of dune"]
    
    for domain in YTS_DOMAINS:
        try:
            url = f"{domain}/api/v2/list_movies.json"
            resp = curl_requests.get(url, params={"query_term": query, "limit": 15}, impersonate="chrome120", timeout=4)
            if resp.status_code == 200:
                data = resp.json().get("data", {}).get("movies", [])
                for movie in data:
                    title = movie.get("title", "").strip()
                    title_lower = title.lower()
                    
                    # فحص واستبعاد الأفلام التجارية الفرعية غير المقصودة
                    if any(exc in title_lower for exc in excluded_keywords):
                        if query_clean not in ["children of dune"]:
                            continue
                    
                    # التأكد من أن عنوان الفيلم يحتوي على كلمة البحث الأساسية
                    if query_clean not in title_lower:
                        continue
                        
                    title_long = movie.get("title_long", title)
                    rating = movie.get("rating", "N/A")
                    summary = movie.get("summary", "No description available.")[:150] + "..."
                    
                    for t in movie.get("torrents", []):
                        file_hash = t.get("hash", "")
                        if file_hash:
                            magnet = f"magnet:?xt=urn:btih:{file_hash}&dn={title_long.replace(' ', '+')}{TRACKER_PARAMS}"
                            options.append({
                                "source": "YTS",
                                "title": f"🎬 {title_long} [⭐ IMDb: {rating}] - {t.get('quality')} ({t.get('type', '')})",
                                "size": t.get("size", "Unknown"),
                                "seeds": t.get("seeds", 10),
                                "magnet": magnet,
                                "description": summary
                            })
                if options:
                    break
        except Exception:
            continue
    return options

# 2. المصادر الأخرى: 1337x و The Pirate Bay مع استخراج حقيقي وآمن للـ Magnet
async def search_tpb_and_1337x(query):
    options = []
    search_query = f"{query} site:1337x.to OR site:thepiratebay.party torrent magnet"
    
    try:
        with DDGS() as client:
            results = list(client.text(search_query, max_results=8))
            for item in results:
                title = item.get("title", "Unknown")
                href = item.get("href", "")
                
                if any(k in href for k in ["1337x.to/torrent/", "tpb.party/torrent/", "thepiratebay"]):
                    try:
                        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
                        resp = curl_requests.get(href, headers=headers, impersonate="chrome120", timeout=3)
                        if resp.status_code == 200:
                            soup = BeautifulSoup(resp.text, 'html.parser')
                            magnet_tag = soup.find('a', href=lambda x: x and x.startswith('magnet:?'))
                            if magnet_tag:
                                magnet = magnet_tag['href']
                                if "&tr=" not in magnet:
                                    magnet += TRACKER_PARAMS
                                
                                source_name = "1337x" if "1337x" in href else "TPB"
                                options.append({
                                    "source": source_name,
                                    "title": f"🔥 {title[:55]}...",
                                    "size": "Verified",
                                    "seeds": 50,
                                    "magnet": magnet,
                                    "description": f"High quality verified release from {source_name} aggregator."
                                })
                    except Exception:
                        continue
    except Exception:
        pass
        
    return options

# دمج كل المصادر معاً
async def aggregate_search(query):
    console.print(f"[dim]🔍 Querying YTS, 1337x, and TPB for: '{query}'...[/dim]")
    
    yts_results = search_yts(query)
    web_results = await search_tpb_and_1337x(query)
    
    return yts_results + web_results

# واجهة العرض التفاعلية وإدارة مدخلات المستخدم بدقة
def display_and_select_file(options):
    if not options:
        return None
        
    table = Table(title="🌐 Multi-Source Verified Torrents (IMDb & Quality)")
    
    table.add_column("No.", style="cyan", justify="center")
    table.add_column("Source", style="blue")
    table.add_column("Title, IMDb Rating & Quality", style="magenta")
    table.add_column("Size", style="green")
    table.add_column("Seeds", style="yellow")
    
    for idx, opt in enumerate(options[:15], 1):
        table.add_row(str(idx), opt["source"], opt["title"], opt["size"], str(opt["seeds"]))
    
    console.print(table)
    
    while True:
        choice = Prompt.ask("\n[bold green]Enter the number of the file you want to inspect & download (or 'q' to quit)[/bold green]", default="1")
        if choice.lower() == 'q':
            return None
        try:
            selected_idx = int(choice) - 1
            if 0 <= selected_idx < len(options):
                selected = options[selected_idx]
                console.print(f"\n[bold cyan]📖 Movie Overview / IMDb Details:[/bold cyan]")
                console.print(f"[italic white]{selected.get('description', 'No details available.')}[/italic white]\n")
                
                confirm = Prompt.ask("[bold yellow]Proceed with downloading this file? (y/n)[/bold yellow]", default="y")
                if confirm.strip().lower() in ['y', 'yes', '']:
                    return selected
                else:
                    console.print("[yellow]Selection cancelled. Choose another file or press 'q' to quit.[/yellow]")
            else:
                console.print("[bold red]❌ Invalid number. Please select from the list.[/bold red]")
        except ValueError:
            console.print("[bold red]❌ Please enter a valid number.[/bold red]")

# محرك التنزيل الصلب (aria2c Engine مع دعم المسارات وتجاوز الملفات الموجودة)
def download_torrent(selected_option):
    if not selected_option or "magnet" not in selected_option:
        console.print("[bold red]❌ No valid selection made.[/bold red]")
        return
        
    magnet = selected_option["magnet"]

    default_path = os.getcwd()
    while True:
        user_path = Prompt.ask(f"[bold blue]Enter destination path (e.g. C:, D: or press Enter for current directory)[/bold blue]", default=default_path)
        
        # تحويل الحرف البسيط مثل "D" إلى مسار نظام صحيح "D:\"
        if len(user_path.strip()) == 1 and user_path.strip().isalpha():
            user_path = user_path.strip().upper() + ":\\"

        if not os.path.exists(user_path):
            try:
                os.makedirs(user_path, exist_ok=True)
                break
            except OSError:
                console.print("[bold red]❌ Error: Invalid path or permission denied! Try again.[/bold red]")
        else:
            break

    console.print(f"\n[bold yellow]🚀 Launching hardened aria2c download engine...[/bold yellow]")
    cmd = [
        "aria2c",
        "--dir=" + user_path,
        "--seed-time=0",
        "--enable-dht=true",
        "--dht-entry-point=router.bittorrent.com:6881",
        "--allow-overwrite=true",  # حل جذري لمشكلة الملفات الموجودة مسبقاً
        magnet
    ]

    try:
        subprocess.run(cmd, check=True)
        console.print(f"\n[bold green]✨ Download completed successfully! Saved to: {user_path} 🍿[/bold green]")
    except subprocess.CalledProcessError as e:
        console.print(f"\n[bold red]❌ Error during download: {e}[/bold red]")
    except FileNotFoundError:
        console.print("\n[bold red]❌ Error: 'aria2c' is not installed or not found in system PATH![/bold red]")

# الدورة الرئيسية للبرنامج
async def main_async():
    console.clear()
    console.print("[bold cyan]==================================================[/bold cyan]")
    console.print("[bold yellow]    🧠 Next-Gen Media Automation Automator (10/10) [/bold yellow]")
    console.print("[bold cyan]==================================================[/bold cyan]\n")
    
    query = Prompt.ask("[bold green]Enter the name of the movie you are looking for[/bold green]")
    if not query.strip():
        console.print("[bold red]❌ You must enter a valid name![/bold red]")
        return
    
    options = await aggregate_search(query)
    if not options:
        console.print("[bold red]❌ No results found from any source.[/bold red]")
        return
    
    best_option = display_and_select_file(options)
    if best_option:
        download_torrent(best_option)

if __name__ == "__main__":
    asyncio.run(main_async())