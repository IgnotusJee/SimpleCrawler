#!/usr/bin/env python3
"""
Batch Web Scraper with Auto-Retry and Smart Storage
"""

import os
import re
import sys
import aiohttp
import asyncio
import hashlib
import sqlite3
from datetime import datetime, timedelta
from urllib.parse import urlparse
from typing import Iterator, Dict, Any
from itertools import product
import logging
from logging.handlers import RotatingFileHandler
import argparse
import json
import magic

# Configuration
DEFAULT_CONFIG = {
    'concurrency': 5,
    'retry_policy': {
        'max_attempts': 3,
        'backoff_base': 2
    },
    'storage': {
        'base_dir': './downloads',
        'db_file': 'scraped_data.db',
        'max_file_size': 1024 * 1024 * 100  # 100MB
    },
    'logging': {
        'log_filename': 'scraper.log',
        'backup_count': 5,
        'max_bytes': 1024 * 5
    },
    'user_agent': 'Mozilla/5.0 (compatible; BatchScraper/1.0; +https://example.com/bot)'
}


class URLGenerator:
    """动态URL生成器"""

    def __init__(self, config: Dict[str, Any]):
        """
        :param config: 包含模板和生成规则的字典
        """
        self.template = config['url_template']
        self.rules = config['variables']
        self._validate_config()

    def _validate_config(self):
        """验证配置完整性"""
        # 提取模板变量
        variables = set(re.findall(r'\{(.*?)}', self.template))

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
        values = self._generate_values()

        # 生成笛卡尔积
        all_combinations = product(*[
            list(gen) for gen in values.values()
        ])

        for combination in all_combinations:
            params = dict(zip(values.keys(), combination))
            yield self.template.format(**params)


class AsyncScraper:
    """异步抓取引擎"""

    def __init__(self, config: Dict[str, Any], timestamp):
        self.config = config
        self.session = None
        self.semaphore = asyncio.Semaphore(config['concurrency'])
        self.logger = self.setup_logger()
        self.db_conn = sqlite3.connect(config['storage']['db_file'])
        self.timestamp = timestamp
        self.init_db()

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
                    attempts INTEGER
                )
            ''')
            self.db_conn.execute('''
                CREATE INDEX IF NOT EXISTS idx_url ON pages (url)
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

    async def fetch(self, url: str) -> Dict[str, Any]:
        """执行单个请求"""
        async with self.semaphore:
            for attempt in range(self.config['retry_policy']['max_attempts']):
                try:
                    async with self.session.get(
                            url,
                            headers={'User-Agent': self.config['user_agent']},
                            timeout=aiohttp.ClientTimeout(total=30)
                    ) as response:
                        content = await response.text()
                        content_hash = hashlib.md5(content.encode()).hexdigest()

                        return {
                            'url': url,
                            'content': content,
                            'status': response.status,
                            'hash': content_hash,
                            'attempts': attempt + 1
                        }
                except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                    self.logger.warning(f"Attempt {attempt + 1} failed for {url}: {str(e)}")
                    await asyncio.sleep(self.config['retry_policy']['backoff_base'] ** attempt)
            return {'url': url, 'error': str(e), 'attempts': attempt + 1}

    async def worker(self, queue: asyncio.Queue):
        """工作协程"""
        while True:
            url = await queue.get()
            try:
                result = await self.fetch(url)
                await self.process_result(result)
            finally:
                queue.task_done()

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

        # 存储到文件系统
        file_path = self.save_to_file(result['url'], str(result['content']))

        # 存储到数据库
        self.db_conn.execute('''
            INSERT INTO pages (url, content_hash, file_path, status_code, attempts)
            VALUES (?, ?, ?, ?, ?)
        ''', (
            result['url'],
            result['hash'],
            file_path,
            result['status'],
            result['attempts']
        ))
        self.db_conn.commit()

    def save_to_file(self, url: str, content: str) -> str:
        """保存到文件系统"""
        parsed_url = urlparse(url)
        path = parsed_url.path.strip('/') or 'index'
        safe_filename = re.sub(r'[^\w\-_.]', '_', path)

        filetype = magic.from_buffer(content, mime=True).split('/')[-1]
        if not safe_filename.endswith(f'.{filetype}'):
            safe_filename += f'.{filetype}'
        full_path = os.path.join(
            self.config['storage']['base_dir'],
            self.timestamp,
            parsed_url.netloc,
            safe_filename
        )

        os.makedirs(os.path.dirname(full_path), exist_ok=True)
        with open(full_path, 'w', encoding='utf-8') as f:
            f.write(content)
        return full_path

    async def run(self, urls: URLGenerator):
        """运行抓取任务"""
        queue = asyncio.Queue(maxsize=1000)

        # 创建worker任务
        workers = [
            asyncio.create_task(self.worker(queue))
            for _ in range(self.config['concurrency'])
        ]

        # 填充任务队列
        async def populate_queue():
            for url in urls:
                await queue.put(url)
            await queue.join()

        async with aiohttp.ClientSession() as self.session:
            await populate_queue()

        # 清理worker
        for worker in workers:
            worker.cancel()
        await asyncio.gather(*workers, return_exceptions=True)


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


def run_crawler(config, url_input, time):
    """爬取主程序"""
    os.makedirs(config['storage']['base_dir'], exist_ok=True)

    # 生成URL列表
    try:
        urls = URLGenerator(url_input)
    except ValueError as e:
        print(f"Invalid parameters: {str(e)}")
        sys.exit(1)

    # 运行抓取任务
    scraper = AsyncScraper(config, time)

    # 创建新事件循环（显式指定）
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)  # 绑定到当前上下文

    try:
        loop.run_until_complete(scraper.run(urls))
    finally:
        loop.close()  # 明确清理资源


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
    if(args.config and os.path.exists(args.config)):
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
        if args.pattern:
            input = {
                "url_template": args.pattern,
                "variables": args.var_rules
            }
            run_crawler(config, input, nowtime)
        else:
            with open(args.input, 'r', encoding='utf-8') as f:
                inputs = json.load(f)
            for input in inputs:
                if 'url_template' not in input or 'variables' not in input:
                    print("Invalid input file format")
                    return
                run_crawler(config, input, nowtime)
                print(f"Done crawling job for {input['url_template']}")
        print("Done crawling job")


if __name__ == '__main__':
    main()

