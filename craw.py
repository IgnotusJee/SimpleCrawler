#!/usr/bin/env python3
"""
Enhanced Batch Web Scraper with Anti-Anti-Scraping Features
"""
import os
import re
import sys
import aiohttp
import asyncio
import hashlib
import sqlite3
import random
from datetime import datetime, timedelta
from urllib.parse import urlparse
from typing import Iterator, Dict, Any, Optional
from itertools import product
import logging
from logging.handlers import RotatingFileHandler
import argparse
import json
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from playwright.async_api import async_playwright

DEFAULT_CONFIG = {
    'concurrency': 5,
    'retry_policy': {
        'max_attempts': 3,
        'backoff_base': 2
    },
    'storage': {
        'base_dir': './downloads',
        'db_file': 'scraped_data.db',
        'max_file_size': 1024 * 1024 * 100
    },
    'logging': {
        'log_filename': 'scraper.log',
        'backup_count': 5,
        'max_bytes': 1024 * 1024 * 10
    },
    'file_types': {
        'images': ['jpg', 'jpeg', 'png', 'gif', 'webp'],
        'documents': ['pdf', 'doc', 'docx', 'xls', 'xlsx', 'ppt', 'pptx'],
        'archives': ['zip', 'rar', 'gz'],
        'webviews': ['html', 'php', 'css'],
        'texts': ['json', 'txt'],
    },
    'resource_patterns': [
        r'.*\.(jpg|jpeg|png|gif|webp)(\?.*)?$',
        r'.*\.(pdf|doc|docx|xls|xlsx)(\?.*)?$'
    ],
    'browser': {
        'timeout': 30000,  # 30秒超时
        'max_pages': 3  # 最大并发页面数
    },
    'request_delay': [0.5, 3.0],  # 随机延迟范围
    'proxies': [],  # 代理服务器列表
    'headers': {  # 基础请求头
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Referer': 'https://www.google.com/',
        'DNT': '1',
        'Connection': 'keep-alive',
    }
}


class URLGenerator:
    """动态URL生成器"""

    def __init__(self, config: Dict[str, Any]):
        """config: 包含模板和生成规则的字典"""
        self.template = config['url_template']
        self.rules = config['variables']
        self.fixed = False
        self._validate_config()

    def _validate_config(self):
        """验证配置完整性"""
        # 提取模板变量
        variables = set(re.findall(r'\{(.*?)}', self.template))

        if len(variables) == 0:
            self.fixed = True
            return

        # 验证规则匹配
        missing = variables - set(self.rules.keys())
        if missing:
            raise ValueError(f"缺少变量规则: {missing}")

        # 验证规则参数
        required_params = {'type', 'start', 'end', 'step'}
        for var, rule in self.rules.items():
            if missing_params := required_params - set(rule.keys()):
                raise ValueError(f"变量 {var} 缺少参数: {missing_params}")

    def _gen_numbers(self, rule: Dict) -> Iterator[int]:
        """生成数值序列"""
        current = rule['start']
        end = rule['end']
        step = rule['step']

        while current <= end:
            yield current
            current += step

    def _gen_dates(self, rule: Dict) -> Iterator[str]:
        """生成日期序列"""
        format = str(rule['format'])
        start = datetime.strptime(str(rule['start']), format)
        end = datetime.strptime(str(rule['end']), format)
        step = timedelta(days=rule['step'])

        current = start
        while current <= end:
            yield current.strftime(format)
            current += step

    def _generate_values(self) -> Dict[str, Any]:
        """生成所有变量的值序列"""
        value_generators = {}

        for var, rule in self.rules.items():
            if rule['type'] == 'number':
                value_generators[var] = self._gen_numbers(rule)
            elif rule['type'] == 'date':
                value_generators[var] = self._gen_dates(rule)
            else:
                raise ValueError(f"不支持的变量类型: {rule['type']}")

        return value_generators

    def __iter__(self) -> Iterator[str]:
        """生成完整的URL序列"""
        if self.fixed:
            yield self.template
            return

        values = self._generate_values()

        # 生成笛卡尔积
        all_combinations = product(*[
            list(gen) for gen in values.values()
        ])

        for combination in all_combinations:
            params = dict(zip(values.keys(), combination))
            yield self.template.format(**params)


class AsyncScraper:
    """异步抓取引擎（增强反爬能力版）"""

    def __init__(self, config: Dict[str, Any], timestamp, force_render):
        self.playwright = None
        self.config = config
        self.timestamp = timestamp
        # 是否强制网页渲染
        self.force_render = force_render

        # 初始化浏览器指纹
        self.ua = UserAgent()
        self.browser = None
        self.browser_sem = None

        # 初始化异步组件
        self.session = None
        self.semaphore = asyncio.Semaphore(config['concurrency'])
        self.logger = self.setup_logger()
        self.db_conn = sqlite3.connect(config['storage']['db_file'])
        self.init_db()
        self.queue = asyncio.Queue(maxsize=1000)
        # 用户代理轮换缓存
        self._current_headers = None

    def init_db(self):
        """初始化数据库"""
        with self.db_conn:
            self.db_conn.execute('''
				CREATE TABLE IF NOT EXISTS pages (
					id INTEGER PRIMARY KEY,
					url TEXT,
					content_hash TEXT,
					file_path TEXT,
					timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
					status_code INTEGER,
					attempts INTEGER,
					content_type TEXT
				)
			''')
            self.db_conn.execute('''
				CREATE INDEX IF NOT EXISTS idx_content_type ON pages (content_type)
			''')

    def setup_logger(self):
        """配置日志系统"""
        logger = logging.getLogger('BatchScraper')
        logger.setLevel(logging.INFO)

        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )

        file_handler = RotatingFileHandler(
            self.config['logging']['log_filename'],
            maxBytes=self.config['logging']['max_bytes'],
            backupCount=self.config['logging']['backup_count']
        )
        file_handler.setFormatter(formatter)

        logger.addHandler(file_handler)
        return logger

    async def __aenter__(self):
        """异步上下文管理"""
        await self.init_browser()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        """清理资源"""
        await self.close()

    async def init_browser(self):
        """初始化无头浏览器"""
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.chromium.launch(
            headless=True,
            proxy={
                'server': self._random_proxy()
            } if self.config['proxies'] else None
        )
        self.browser_sem = asyncio.Semaphore(
            self.config['browser']['max_pages']
        )

    async def close(self):
        """关闭所有资源"""
        if self.session:
            await self.session.close()
        if self.browser:
            await self.browser.close()
        if hasattr(self, 'playwright'):
            await self.playwright.stop()
        self.db_conn.close()

    def _random_proxy(self) -> Optional[str]:
        """随机获取代理服务器"""
        return random.choice(self.config['proxies']) if self.config['proxies'] else None

    def _gen_headers(self) -> Dict[str, str]:
        """生成动态请求头"""
        base = {
            'User-Agent': self.ua.random,
            **self.config['headers']
        }
        # 自动添加Referer链
        if self._current_headers and 'Referer' in self._current_headers:
            base['Referer'] = self._current_headers['Referer']
        return base

    async def fetch(self, url: str, is_resource: bool) -> Dict[str, Any]:
        """智能请求方法"""
        # 随机延迟
        delay = random.uniform(*self.config['request_delay'])
        await asyncio.sleep(delay)

        if not is_resource and self.force_render:
            result = await self._browser_fetch(url)
        else:
            # 优先尝试API请求
            result = await self._http_fetch(url)
            # 需要浏览器渲染的情况
            if not is_resource and self._needs_browser_rendering(result):
                result = await self._browser_fetch(url)

        return result

    async def _http_fetch(self, url: str) -> Dict[str, Any]:
        """使用aiohttp进行请求"""
        headers = self._gen_headers()
        self._current_headers = headers  # 保存当前headers
        async with self.semaphore:
            for attempt in range(self.config['retry_policy']['max_attempts']):
                try:
                    async with self.session.get(
                            url,
                            headers=headers,
                            proxy=self._random_proxy(),
                            timeout=aiohttp.ClientTimeout(total=30),
                            ssl=False
                    ) as response:
                        content = await self._process_response(response)
                        return self._success_result(url, content, response)

                except Exception as e:
                    await self._handle_error(url, e, attempt)
            return self._failure_result(url)

    async def _browser_fetch(self, url: str) -> Dict[str, Any]:
        """使用无头浏览器获取页面"""
        async with self.browser_sem:
            page = None
            try:
                context = await self.browser.new_context(
                    user_agent=self.ua.random,
                    locale='en-US',
                    timezone_id='America/New_York',
                )
                page = await context.new_page()

                # 设置浏览器指纹
                await self._set_browser_fingerprint(page)
                await page.goto(url, timeout=self.config['browser']['timeout'])

                # 等待页面加载完成
                await page.wait_for_load_state('networkidle')

                # 获取最终内容
                content = await page.content()
                return self._success_result(url, content)

            except Exception as e:
                self.logger.error(f"Browser fetch failed: {str(e)}")
                return self._failure_result(url)
            finally:
                if page:
                    await page.close()
                if context:
                    await context.close()

    async def _set_browser_fingerprint(self, page):
        """设置浏览器指纹"""
        # 覆盖webdriver属性
        await page.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            })
        """)
        # 设置屏幕分辨率
        await page.set_viewport_size({"width": 1920, "height": 1080})

    async def _process_response(self, response) -> Any:
        """处理响应内容"""
        content_type = response.headers.get('Content-Type', '').split(';')[0]
        if 'text' in content_type:
            return await response.text()
        return await response.read()

    def _needs_browser_rendering(self, result: Dict) -> bool:
        """判断是否需要浏览器渲染"""
        if result.get('status') != 200 or result.get('content_type', '') != 'text/html':
            return False
        # 检测JS重定向
        if '<meta http-equiv="refresh"' in result.get('content', ''):
            return True
        # 检测常见反爬技术
        anti_selectors = [
            '#challenge-form',  # Cloudflare
            '.geetest_captcha',  # Geetest
            '#px-captcha'  # PerimeterX
        ]
        soup = BeautifulSoup(result.get('content', ''), 'html.parser')
        return any(soup.select(selector) for selector in anti_selectors)

    def _success_result(self, url: str, content: Any, response=None) -> Dict:
        """构造成功响应"""
        is_text = isinstance(content, str)
        hash_str = hashlib.md5(content.encode() if is_text else content).hexdigest()

        return {
            'url': url,
            'content': content,
            'content_type': response.headers.get('Content-Type', '') if response else 'text/html',
            'status': response.status if response else 200,
            'hash': hash_str,
            'attempts': 1
        }

    async def _handle_error(self, url: str, error: Exception, attempt: int):
        """统一错误处理"""
        self.logger.warning(f"Attempt {attempt + 1} failed for {url}: {str(error)}")
        backoff = self.config['retry_policy']['backoff_base'] ** attempt
        await asyncio.sleep(backoff + random.uniform(0, 1))

    def _failure_result(self, url: str) -> Dict:
        """构造失败响应"""
        return {
            'url': url,
            'error': 'Max retries exceeded',
            'attempts': self.config['retry_policy']['max_attempts']
        }

    def extract_links(self, html: str, base_url: str) -> set:
        """从HTML内容中提取资源链接"""
        soup = BeautifulSoup(html, 'html.parser')
        links = set()

        # 提取图片链接
        for img in soup.find_all('img'):
            src = img.get('src')
            if src:
                links.add(self.normalize_url(base_url, src))

        # 提取文件链接
        for a in soup.find_all('a'):
            href = a.get('href')
            if href and self.is_resource_link(href):
                links.add(self.normalize_url(base_url, href))

        return links

    def normalize_url(self, base_url: str, path: str) -> str:
        """规范化相对路径为绝对URL"""
        parsed_base = urlparse(base_url)
        if path.startswith('//'):
            return f"{parsed_base.scheme}:{path}"
        elif path.startswith('/'):
            return f"{parsed_base.scheme}://{parsed_base.netloc}{path}"
        elif path.startswith(('http://', 'https://')):
            return path
        else:
            return f"{parsed_base.scheme}://{parsed_base.netloc}/{path}"

    def is_resource_link(self, url: str) -> bool:
        """判断是否是资源链接"""
        for pattern in self.config['resource_patterns']:
            if re.match(pattern, url, re.IGNORECASE):
                return True
        return False

    async def process_result(self, result: Dict[str, Any]):
        """处理抓取结果"""
        if 'error' in result:
            self.logger.error(f"Failed to fetch {result['url']}: {result['error']}")
            return
        # 内容去重检查
        cur = self.db_conn.execute(
            'SELECT url FROM pages WHERE content_hash = ?',
            (result['hash'],)
        )
        if cur.fetchone():
            self.logger.info(f"Skipping duplicate content: {result['url']}")
            return
        # 保存文件
        try:
            file_path = self.save_to_file(
                result['url'],
                result['content'],
                result.get('content_type', 'text/html')
            )
        except Exception as e:
            self.logger.error(f"Failed to save {result['url']}: {str(e)}")
            return
        # 存储到数据库
        self.db_conn.execute('''
			INSERT INTO pages (url, content_hash, file_path, status_code, attempts, content_type)
			VALUES (?, ?, ?, ?, ?, ?)
		''', (
            result['url'],
            result['hash'],
            file_path,
            result['status'],
            result['attempts'],
            result.get('content_type', '')
        ))
        self.db_conn.commit()
        # 如果是HTML内容，提取资源链接
        if file_path.endswith('html'):
            try:
                html_content = result['content'].decode('utf-8') if isinstance(result['content'], bytes) else result[
                    'content']
                resource_links = self.extract_links(html_content, result['url'])
                await self.enqueue_resources(resource_links)
            except Exception as e:
                self.logger.error(f"Failed to extract links from {result['url']}: {str(e)}")

    async def enqueue_resources(self, links: set):
        """将资源链接加入队列"""
        for link in links:
            if not self.is_duplicate_url(link):
                await self.queue.put((link, True))

    def is_duplicate_url(self, url: str) -> bool:
        """检查URL是否已经处理过"""
        cur = self.db_conn.execute(
            'SELECT url FROM pages WHERE url = ?',
            (url,)
        )
        return cur.fetchone() is not None

    def save_to_file(self, url: str, content: Any, content_type: str) -> str:
        """保存到文件系统（支持二进制）"""
        parsed_url = urlparse(url)
        path = parsed_url.path.strip('/') or 'index'

        # 生成安全文件名
        safe_filename = re.sub(r'[^\w\-_.]', '_', path.split('/')[-1][:128])
        safe_filename = os.path.splitext(safe_filename)[0]

        file_ext = content_type.split('/')[-1].split(';')[0].split('+')[0]

        safe_filename += f'.{file_ext}'

        # 构建存储路径
        category = self.get_file_category(file_ext)
        full_path = os.path.join(
            self.config['storage']['base_dir'],
            self.timestamp,
            category,
            parsed_url.netloc,
            safe_filename
        )
        os.makedirs(os.path.dirname(full_path), exist_ok=True)

        # 写入文件
        mode = 'wb' if isinstance(content, bytes) else 'w'
        encoding = None if isinstance(content, bytes) else 'utf-8'

        with open(full_path, mode, encoding=encoding) as f:
            f.write(content)

        return full_path

    def get_file_category(self, ext: str) -> str:
        """获取文件分类目录"""
        ext = ext.lower()
        for category, exts in self.config['file_types'].items():
            if ext in exts:
                return category
        return 'others'

    async def run(self, urls: URLGenerator):
        """运行增强版抓取任务"""
        async with aiohttp.ClientSession() as self.session:
            # 创建混合worker
            workers = [
                asyncio.create_task(self._mixed_worker())
                for _ in range(self.config['concurrency'] * 2)
            ]

            # 填充队列
            async def populate_queue():
                for url in urls:
                    await self.queue.put((url, False))
                await self.queue.join()

            await populate_queue()

            # 清理worker
            for w in workers:
                w.cancel()
            await asyncio.gather(*workers, return_exceptions=True)

    async def _mixed_worker(self):
        """混合模式worker"""
        while True:
            url, isr = await self.queue.get()
            try:
                result = await self.fetch(url, isr)
                await self.process_result(result)
            finally:
                self.queue.task_done()


def cleanup_resources(args, config):
    """清理所有持久化资源"""
    import shutil

    if args.all or args.database:
        """清理数据库文件"""
        db_path = config['storage']['db_file']
        if os.path.exists(db_path):
            try:
                os.remove(db_path)
                print(f"🗑️ 数据库文件已删除: {db_path}")
            except PermissionError as e:
                print(f"❌ 无法删除数据库文件: {str(e)}")

    if args.all or args.log:
        """清理日志文件"""
        log_file = config['logging']['log_filename']
        for single_file in [log_file] + [log_file + f'.{i}' for i in range(1, config['logging']['backup_count'] + 1)]:
            if os.path.exists(single_file):
                try:
                    os.remove(single_file)
                    print(f"🗑️ 日志文件已删除: {single_file}")
                except Exception as e:
                    print(f"❌ 清理日志失败: {str(e)}")

    if args.all or args.download:
        """清理下载目录"""
        download_dir = config['storage']['base_dir']
        if os.path.exists(download_dir):
            try:
                shutil.rmtree(download_dir)
                print(f"🗑️ 下载目录已清理: {download_dir}")
            except Exception as e:
                print(f"❌ 清理下载目录失败: {str(e)}")
        else:
            print(f"ℹ️ 下载目录不存在: {download_dir}")


async def run_crawler(config, url_input, time, force_render=False):
    """爬取主程序"""
    os.makedirs(config['storage']['base_dir'], exist_ok=True)

    if url_input.get('force_render'):
        force_render = True

    # 生成URL列表
    try:
        urls = URLGenerator(url_input)
    except ValueError as e:
        print(f"Invalid parameters: {str(e)}")
        sys.exit(1)

    # 运行抓取任务
    async with AsyncScraper(config, time, force_render) as scraper:
        await scraper.run(urls)


class VarAction(argparse.Action):
    """自定义参数处理器，解析形如 varname type=type,start=x,end=y,step=z 的字符串"""

    def __call__(self, parser, namespace, values, option_string=None):
        # 初始化存储结构
        if not hasattr(namespace, 'var_rules'):
            namespace.var_rules = {}

        try:
            # 分割变量名和参数部分
            var_name, params_str = values.split(',', 1)

            # 解析参数键值对
            params = {}
            for pair in params_str.split(','):
                key, value = pair.split('=', 1)
                params[key.strip()] = value.strip()

            # 参数完整性校验
            required_keys = {'type', 'start', 'end', 'step'}
            if missing := required_keys - params.keys():
                raise ValueError(f"缺少必要参数: {missing}")

            # 类型转换逻辑
            if params['type'] == 'date':
                params = self._process_date_params(params)
            elif params['type'] == 'number':
                params = self._process_number_params(params)
            else:
                raise ValueError(f"不支持的类型: {params['type']}")

            # 存储到命名空间
            namespace.var_rules[var_name] = params

        except Exception as e:
            parser.error(f"参数解析错误: {str(e)}")

    def _process_date_params(self, params):
        """处理日期类型参数"""
        if params['format']:
            format = params['format']
        else:
            format = "%Y-%m-%d"
        try:
            datetime.strptime(params['start'], format)
            datetime.strptime(params['end'], format)
            params['step'] = int(params['step'])
            params['format'] = format
        except ValueError:
            raise ValueError(f"日期格式应为 {format}，步长应为整数")
        return params

    def _process_number_params(self, params):
        """处理数字类型参数"""
        try:
            params['start'] = int(params['start'])
            params['end'] = int(params['end'])
            params['step'] = int(params['step'])
        except ValueError:
            raise ValueError("数值参数应为数字")
        return params


def main():
    """命令行入口"""
    parser = argparse.ArgumentParser(description='Batch Web Scraper')
    parser.add_argument('-c', '--config', help='Configuration file path')
    subparsers = parser.add_subparsers(dest='command')

    run_parser = subparsers.add_parser('run', description='Run crawl process')
    run_parser.add_argument_group()
    run_parser.add_argument('-i', '--input', help='Url pattern and vars input file path')
    run_parser.add_argument('-p', '--pattern', help='URL pattern with placeholders {var}')
    run_parser.add_argument('-a', '--var-rule', action=VarAction, metavar="VAR_RULE",
                            help='Var rule, e.g. date,type=date,start=20240101,end=20240105,step=1,format=%%Y-%%m-%%d')
    run_parser.add_argument('-f', '--force-render', action='store_true', default=False,
                            help='Enable forced using browser render engine')

    clean_parser = subparsers.add_parser('clean', description='Clean generated and downloaded files')
    clean_group = clean_parser.add_mutually_exclusive_group(required=True)
    clean_group.add_argument('-a', '--all',
                             action='store_true',
                             help='Clean all log, downloaded and db file')
    clean_group.add_argument('-o', '--log',
                             action='store_true',
                             help='Clean log files')
    clean_group.add_argument('-d', '--download',
                             action='store_true',
                             help='Clean download folder')
    clean_group.add_argument('-D', '--database',
                             action='store_true',
                             help='Clean database file')

    args = parser.parse_args()

    # 初始化配置
    if args.config and os.path.exists(args.config):
        with open(args.config, 'r', encoding='utf-8') as f:
            try:
                config = json.load(f)
            except json.JSONDecodeError as e:
                print(f"配置文件解析错误: {str(e)}")
                sys.exit(1)
    else:
        config = DEFAULT_CONFIG

    if args.command == 'clean':
        print("🚀 开始清理系统资源...")
        cleanup_resources(args, config)
        print("✅ 资源清理完成\n")
    elif args.command == 'run':
        if args.pattern and args.input:
            print("Command input and file input must be set up to one")
            return
        if args.pattern is None and args.input is None:
            print("Command input and file input must be set at least one")
            return

        nowtime = datetime.now().strftime("%Y%m%d%H%M%S")
        print("Start doing crawling job...")
        # 创建新事件循环（显式指定）
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)  # 绑定到当前上下文

        try:
            if args.pattern:
                _input = {
                    "url_template": args.pattern,
                    "variables": args.var_rules if hasattr(args, 'var_rules') else None
                }
                loop.run_until_complete(run_crawler(config, _input, nowtime, args.force_render))
            else:
                with open(args.input, 'r', encoding='utf-8') as f:
                    inputs = json.load(f)
                for _input in inputs:
                    if 'url_template' not in _input or 'variables' not in _input:
                        print("Invalid input file format")
                        return
                    loop.run_until_complete(run_crawler(config, _input, nowtime))
                    print(f"Done crawling job for {_input['url_template']}")
        finally:
            loop.close()  # 明确清理资源
        print("Done crawling job")


if __name__ == '__main__':
    main()
