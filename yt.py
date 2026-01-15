import os
import sys
import uuid
import threading
import subprocess
import zipfile
import concurrent.futures
import webbrowser
import logging
import time
import socket
from datetime import datetime, timedelta
from flask import Flask, request, render_template, send_file, redirect, url_for, flash, jsonify
import yt_dlp

# --- 配置區 ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- 資源路徑處理 ---
if getattr(sys, 'frozen', False):
    resource_path = sys._MEIPASS
    exec_dir = os.path.dirname(sys.executable)
else:
    resource_path = os.getcwd()
    exec_dir = os.getcwd()

# 明確指定模板路徑
template_dir = os.path.join(resource_path, 'templates')
app = Flask(__name__, template_folder=template_dir)
app.secret_key = os.urandom(24)

# ffmpeg 設定
ffmpeg_path = os.path.join(resource_path, "ffmpeg", "bin", "ffmpeg.exe")
if not os.path.exists(ffmpeg_path):
    logger.warning(f"指定路徑未找到 ffmpeg: {ffmpeg_path}，將嘗試使用系統路徑")
    ffmpeg_path = "ffmpeg" 
else:
    os.environ["PATH"] += os.pathsep + os.path.join(resource_path, "ffmpeg", "bin")

DOWNLOAD_FOLDER = os.path.join(exec_dir, 'downloads')
if not os.path.exists(DOWNLOAD_FOLDER):
    os.makedirs(DOWNLOAD_FOLDER)

# --- 核心邏輯 (JobManager) ---
class JobManager:
    def __init__(self):
        self._jobs = {}
        self._lock = threading.Lock()
        self._cleaner_thread = threading.Thread(target=self._auto_cleanup, daemon=True)
        self._cleaner_thread.start()

    def create_job(self, job_id, context=None, status='INITIALIZING'):
        with self._lock:
            self._jobs[job_id] = {
                'status': status,
                'progress_percent': 0,
                'message': '',
                'speed': 'N/A',
                'eta': 'N/A',
                'file': None,
                'created_at': datetime.now(),
                'context': context or {}
            }

    def update_job(self, job_id, **kwargs):
        with self._lock:
            if job_id in self._jobs:
                for key, value in kwargs.items():
                    self._jobs[job_id][key] = value

    def get_job(self, job_id):
        with self._lock:
            return self._jobs.get(job_id)

    def reset_job(self, job_id):
        with self._lock:
            if job_id in self._jobs:
                self._jobs[job_id].update({
                    'status': 'RETRYING',
                    'progress_percent': 0,
                    'message': '',
                    'speed': 'N/A',
                    'eta': 'N/A',
                    'created_at': datetime.now()
                })
                return self._jobs[job_id]['context']
        return None

    def _auto_cleanup(self):
        while True:
            time.sleep(600)
            with self._lock:
                now = datetime.now()
                keys_to_remove = [k for k, v in self._jobs.items() if now - v['created_at'] > timedelta(hours=1)]
                for k in keys_to_remove:
                    del self._jobs[k]

manager = JobManager()

# --- 輔助函數 ---
def validate_youtube_url(url):
    import re
    pattern = re.compile(r'(https?://)?(www\.)?(youtube\.com|youtu\.?be)/.+')
    return bool(pattern.match(url))

def get_available_qualities(video_url):
    try:
        ydl_opts = {'quiet': True, 'skip_download': True}
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(video_url, download=False)
        formats = info.get('formats', [])
        available = []
        seen = set()
        for f in sorted(formats, key=lambda x: (x.get('height') or 0), reverse=True):
            height = f.get('height')
            if height and f.get('vcodec') != 'none' and height not in seen:
                note = f.get('format_note', '')
                label = f"{height}p" + (f" ({note})" if note else "")
                available.append({'id': f['format_id'], 'label': label})
                seen.add(height)
        return available
    except Exception as e:
        logger.error(f"提取信息失敗: {e}")
        return []

# --- 下載邏輯 ---
def run_download_pipeline(video_url, quality, job_id):
    try:
        def progress_hook(d):
            if d['status'] == 'downloading':
                total = d.get('total_bytes') or d.get('total_bytes_estimate')
                if total:
                    pct = int(d['downloaded_bytes'] / total * 100)
                    manager.update_job(job_id, progress_percent=pct)
                speed = d.get('speed')
                if isinstance(speed, (int, float)):
                    manager.update_job(job_id, speed=f"{speed/1024/1024:.2f} MB/s", eta=d.get('eta'))
            elif d['status'] == 'finished':
                manager.update_job(job_id, progress_percent=100)

        manager.update_job(job_id, status='DOWNLOADING AUDIO', progress_percent=0)
        audio_opts = {'format': 'bestaudio/best', 'outtmpl': os.path.join(DOWNLOAD_FOLDER, f'{job_id}_audio.%(ext)s'), 'quiet': True}
        with yt_dlp.YoutubeDL(audio_opts) as ydl:
            info = ydl.extract_info(video_url, download=True)
            audio_file = ydl.prepare_filename(info)

        manager.update_job(job_id, status='DOWNLOADING VIDEO', progress_percent=0)
        video_opts = {'format': quality, 'outtmpl': os.path.join(DOWNLOAD_FOLDER, f'{job_id}_video.%(ext)s'), 'quiet': True, 'progress_hooks': [progress_hook]}
        with yt_dlp.YoutubeDL(video_opts) as ydl:
            info = ydl.extract_info(video_url, download=True)
            video_file = ydl.prepare_filename(info)

        manager.update_job(job_id, status='PROCESSING MERGE', progress_percent=99)
        final_filename = os.path.join(DOWNLOAD_FOLDER, f"{job_id}_merged.mp4")
        try:
            safe_title = "".join([c for c in info.get('title', 'video') if c.isalnum() or c in (' ', '-', '_')]).strip()
            if safe_title:
                 final_filename = os.path.join(DOWNLOAD_FOLDER, f"{safe_title}_{job_id[:8]}.mp4")
        except: pass

        subprocess.run([ffmpeg_path, '-y', '-i', video_file, '-i', audio_file, '-c', 'copy', final_filename], check=True, capture_output=True)
        
        if os.path.exists(audio_file): os.remove(audio_file)
        if os.path.exists(video_file): os.remove(video_file)

        manager.update_job(job_id, file=final_filename, status='done', progress_percent=100)
    except Exception as e:
        logger.error(f"Job {job_id} failed: {e}")
        manager.update_job(job_id, status='error', message=str(e))

def run_multi_download(job_id, urls):
    manager.create_job(job_id, status='BATCH PROCESSING')
    downloaded_files = []
    total = len(urls)

    def single_worker(url):
        temp_id = str(uuid.uuid4())
        try:
            quals = get_available_qualities(url)
            if not quals: raise Exception("No quality available")
            manager.create_job(temp_id, status='TEMP_WORKER')
            run_download_pipeline(url, quals[0]['id'], temp_id)
            res = manager.get_job(temp_id)
            return res['file'] if res['status'] == 'done' else None
        except: return None

    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            futures = {executor.submit(single_worker, url): url for url in urls}
            for i, future in enumerate(concurrent.futures.as_completed(futures)):
                if res := future.result(): downloaded_files.append(res)
                manager.update_job(job_id, progress_percent=int((i + 1) / total * 100), status=f'BATCH: {i+1}/{total}')

        if not downloaded_files: raise Exception("所有影片下載失敗")

        zip_name = os.path.join(DOWNLOAD_FOLDER, f"Batch_{job_id[:8]}.zip")
        with zipfile.ZipFile(zip_name, 'w') as zf:
            for f in downloaded_files:
                zf.write(f, os.path.basename(f))
                try: os.remove(f)
                except: pass
        manager.update_job(job_id, file=zip_name, status='done', progress_percent=100)
    except Exception as e:
        manager.update_job(job_id, status='error', message=str(e))

# --- Flask Routes ---
@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        url = request.form.get('video_url')
        if not url or not validate_youtube_url(url):
            flash("無效的 YouTube 鏈接")
            return redirect(url_for('index'))
        if quals := get_available_qualities(url):
            return render_template('select.html', video_url=url, qualities=quals)
        flash("無法解析影片數據")
        return redirect(url_for('index'))
    return render_template('index.html')

@app.route('/multi')
def multi(): return render_template('multi.html')

@app.route('/download', methods=['POST'])
def download():
    url = request.form.get('video_url')
    qual = request.form.get('quality')
    job_id = str(uuid.uuid4())
    manager.create_job(job_id, context={'type': 'single', 'url': url, 'quality': qual})
    threading.Thread(target=run_download_pipeline, args=(url, qual, job_id)).start()
    return redirect(url_for('progress', job_id=job_id))

@app.route('/download_multi', methods=['POST'])
def download_multi():
    urls = [l.strip() for l in request.form.get('video_links', '').splitlines() if l.strip()]
    if not urls or len(urls) > 25:
        flash("無效的鏈接數量 (1-25)")
        return redirect(url_for('multi'))
    job_id = str(uuid.uuid4())
    manager.create_job(job_id, context={'type': 'multi', 'urls': urls})
    threading.Thread(target=run_multi_download, args=(job_id, urls)).start()
    return redirect(url_for('progress', job_id=job_id))

@app.route('/retry', methods=['POST'])
def retry_job():
    job_id = request.json.get('job_id')
    if context := manager.reset_job(job_id):
        target = run_download_pipeline if context['type'] == 'single' else run_multi_download
        args = (context['url'], context['quality'], job_id) if context['type'] == 'single' else (job_id, context['urls'])
        threading.Thread(target=target, args=args).start()
        return jsonify({'status': 'ok'})
    return jsonify({'status': 'failed', 'message': 'Job not found'})

@app.route('/progress')
def progress(): return render_template('progress.html')

@app.route('/job_status')
def job_status():
    job = manager.get_job(request.args.get('job_id'))
    return jsonify(job if job else {'status': 'unknown'})

# [修正：移除了 check=True 以防止 Explorer 回傳 1 時被判定為錯誤]
@app.route('/open_location', methods=['POST'])
def open_location():
    job = manager.get_job(request.json.get('job_id'))
    if job and job.get('status') == 'done' and job.get('file'):
        file_path = os.path.abspath(job['file'])
        if os.path.exists(file_path):
            try:
                # 這裡刪除了 check=True
                subprocess.run(['explorer', '/select,', file_path])
                return jsonify({'status': 'ok'})
            except Exception as e:
                return jsonify({'status': 'error', 'message': str(e)})
    return jsonify({'status': 'error', 'message': 'File not found'})

# --- 啟動入口 ---
if __name__ == '__main__':
    import webview
    
    def get_free_port():
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind(('localhost', 0))
        port = s.getsockname()[1]
        s.close()
        return port
    
    def wait_for_server(port):
        retries = 30
        while retries > 0:
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.settimeout(1)
                result = s.connect_ex(('127.0.0.1', port))
                s.close()
                if result == 0:
                    return True
            except:
                pass
            time.sleep(0.1)
            retries -= 1
        return False

    port = get_free_port()
    
    t = threading.Thread(target=lambda: app.run(host='127.0.0.1', port=port, threaded=True, use_reloader=False))
    t.daemon = True
    t.start()

    if wait_for_server(port):
        webview.create_window(
            title="V-Downloader",
            url=f"http://127.0.0.1:{port}",
            width=750, 
            height=850, 
            resizable=True,
            background_color='#050505'
        )
        webview.start()
    else:
        print("Error: Server failed to start within 3 seconds.")